"""Morning scan worker - runs at 10:00 AM ET.

Scans options chains for credit spread opportunities and sends
recommendations to Discord for approval.

Timing: Moved from 9:35 AM to 10:00 AM to avoid the first 30 minutes
when spreads are wide and quotes are stale.
"""

from datetime import datetime, timedelta

from core import http
from core.ai.claude import ClaudeClient
from core.analysis.iv_rank import calculate_iv_metrics
from core.analysis.screener import OptionsScreener, ScreenerConfig
from core.broker.alpaca import AlpacaClient
from core.db.d1 import D1Client
from core.db.kv import KVClient
from core.notifications.discord import DiscordClient
from core.risk.circuit_breaker import CircuitBreaker, RiskLevel
from core.risk.position_sizer import PositionSizer
from core.types import Confidence, RecommendationStatus, SpreadType, TradeStatus

# Underlyings to scan
# SPY/QQQ/IWM are equity ETFs (86-92% correlated)
# TLT is treasury ETF (negatively correlated with equities)
# GLD is gold ETF (low/variable correlation)
UNDERLYINGS = ["SPY", "QQQ", "IWM", "TLT", "GLD"]

# Maximum recommendations per scan
MAX_RECOMMENDATIONS = 3


async def handle_morning_scan(env):
    """Run the morning options scan."""
    print("Starting morning scan...")

    # Signal start to heartbeat monitor
    heartbeat_url = getattr(env, "HEARTBEAT_URL", None)
    await http.ping_heartbeat_start(heartbeat_url, "morning_scan")

    job_success = False
    try:
        await _run_morning_scan(env)
        job_success = True
    finally:
        # Ping heartbeat with success/failure
        await http.ping_heartbeat(heartbeat_url, "morning_scan", success=job_success)


async def _run_morning_scan(env):
    """Internal morning scan logic."""

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)
    circuit_breaker = CircuitBreaker(kv)

    # Quick check if manually halted (full evaluation happens after account info loaded)
    status = await circuit_breaker.get_status()
    if status.halted:
        print(f"Trading manually halted: {status.reason}")
        return

    # Initialize external clients
    alpaca = AlpacaClient(
        api_key=env.ALPACA_API_KEY,
        secret_key=env.ALPACA_SECRET_KEY,
        paper=(env.ENVIRONMENT == "paper"),
    )

    discord = DiscordClient(
        bot_token=env.DISCORD_BOT_TOKEN,
        public_key=env.DISCORD_PUBLIC_KEY,
        channel_id=env.DISCORD_CHANNEL_ID,
    )

    claude = ClaudeClient(api_key=env.ANTHROPIC_API_KEY)

    # Check if market is open
    market_open = await alpaca.is_market_open()
    print(f"Market open: {market_open}")
    if not market_open:
        print("Market is closed, skipping scan")
        return

    # Get account info for position sizing
    account = await alpaca.get_account()
    positions = await db.get_all_positions()
    open_trades = await db.get_open_trades()

    # Initialize weekly stats (will only set if not already initialized this week)
    await kv.initialize_weekly_stats(starting_equity=account.equity)
    weekly_stats = await kv.get_weekly_stats()
    print(f"Weekly starting equity: ${weekly_stats['starting_equity']:,.2f}")

    # Get current VIX for position sizing and circuit breaker
    current_vix = None
    try:
        vix_data = await alpaca.get_vix_snapshot()
        if vix_data:
            current_vix = vix_data.get("vix")
            vix3m = vix_data.get("vix3m")
            if current_vix:
                print(f"VIX: {current_vix:.2f}")
                # Check for backwardation (VIX > VIX3M indicates near-term fear)
                if vix3m and current_vix / vix3m > 1.0:
                    print(f"VIX in backwardation ({current_vix/vix3m:.2f}x), elevated caution")
    except Exception as e:
        print(f"Could not fetch VIX: {e}")

    # Full graduated risk evaluation
    daily_stats = await kv.get_daily_stats()
    daily_starting_equity = daily_stats.get("starting_equity", account.equity)

    risk_state = await circuit_breaker.evaluate_all(
        starting_daily_equity=daily_starting_equity,
        starting_weekly_equity=weekly_stats["starting_equity"] or account.equity,
        peak_equity=max(daily_starting_equity, weekly_stats["starting_equity"] or account.equity),
        current_equity=account.equity,
        current_vix=current_vix,
    )

    # Log and handle risk state
    if risk_state.level != RiskLevel.NORMAL:
        print(f"Risk level: {risk_state.level.value}, size multiplier: {risk_state.size_multiplier}")
        if risk_state.reason:
            print(f"Reason: {risk_state.reason}")

    if risk_state.level == RiskLevel.HALTED:
        print(f"Trading halted: {risk_state.reason}")
        return

    # Get playbook rules for AI context
    playbook_rules = await db.get_playbook_rules()

    # Initialize screener
    screener = OptionsScreener(ScreenerConfig())
    sizer = PositionSizer()

    # Risk-adjusted size multiplier (from graduated circuit breaker)
    risk_size_multiplier = risk_state.size_multiplier

    all_opportunities = []

    # Scan each underlying
    for symbol in UNDERLYINGS:
        try:
            print(f"Scanning {symbol}...")

            # Get options chain
            chain = await alpaca.get_options_chain(symbol)

            if not chain.contracts:
                print(f"No options data for {symbol}")
                continue

            print(f"{symbol}: Got {len(chain.contracts)} contracts, price=${chain.underlying_price:.2f}")

            # Calculate IV metrics (using ATM options as proxy)
            atm_contracts = [
                c
                for c in chain.contracts
                if abs(c.strike - chain.underlying_price) < chain.underlying_price * 0.02
            ]

            if atm_contracts and atm_contracts[0].implied_volatility:
                current_iv = atm_contracts[0].implied_volatility
            else:
                current_iv = 0.20  # Default

            # Load real IV history from database
            historical_ivs = await db.get_iv_history(symbol, lookback_days=252)
            iv_history_count = len(historical_ivs)

            # Use historical data if available, otherwise use fallback for testing
            if iv_history_count >= 30:
                iv_metrics = calculate_iv_metrics(current_iv, historical_ivs)
            else:
                # Fallback: estimate IV rank from VIX level (temporary for testing)
                # VIX 20-30 suggests elevated IV, use 70% rank as estimate
                from core.analysis.iv_rank import IVMetrics
                estimated_rank = min(90.0, max(50.0, current_vix * 2.5)) if current_vix else 70.0
                iv_metrics = IVMetrics(
                    current_iv=current_iv,
                    iv_rank=estimated_rank,
                    iv_percentile=estimated_rank,
                    iv_high=current_iv * 1.2,
                    iv_low=current_iv * 0.7,
                )
                print(f"{symbol}: Using estimated IV rank {estimated_rank:.0f}% (only {iv_history_count} days history)")
            print(f"{symbol}: IV={current_iv:.2%}, Rank={iv_metrics.iv_rank:.1f}%, Percentile={iv_metrics.iv_percentile:.1f}%")

            # Screen for opportunities
            opportunities = screener.screen_chain(chain, iv_metrics)

            if opportunities:
                print(f"Found {len(opportunities)} opportunities for {symbol}")
                for opp in opportunities[:2]:  # Top 2 per symbol
                    all_opportunities.append((opp, chain.underlying_price, iv_metrics))

        except Exception as e:
            print(f"Error scanning {symbol}: {e}")
            await circuit_breaker.check_api_errors()

    # Sort all opportunities by score
    all_opportunities.sort(key=lambda x: x[0].score, reverse=True)

    # Process top opportunities
    recommendations_sent = 0

    for opp, underlying_price, iv_metrics in all_opportunities[:MAX_RECOMMENDATIONS]:
        try:
            spread = opp.spread

            # Calculate position size
            size_result = sizer.calculate_size(
                spread=spread,
                account_equity=account.equity,
                current_positions=positions,
                current_vix=current_vix,
            )

            if size_result.contracts == 0:
                print(f"Position size is 0 for {spread.underlying}: {size_result.reason}")
                continue

            # Apply graduated risk multiplier
            adjusted_contracts = max(1, int(size_result.contracts * risk_size_multiplier))
            if adjusted_contracts < size_result.contracts:
                print(f"Risk-adjusted contracts: {size_result.contracts} -> {adjusted_contracts}")

            # Get AI analysis
            analysis = await claude.analyze_trade(
                spread=spread,
                underlying_price=underlying_price,
                iv_rank=iv_metrics.iv_rank,
                current_iv=iv_metrics.current_iv,
                playbook_rules=playbook_rules,
            )

            # Skip low confidence trades
            if analysis.confidence == Confidence.LOW:
                print(f"Skipping low confidence trade: {spread.underlying}")
                continue

            # Get delta/theta from the scored spread or Greeks if available
            short_delta = None
            short_theta = None
            if spread.short_contract.greeks:
                short_delta = spread.short_contract.greeks.delta
                short_theta = spread.short_contract.greeks.theta

            # Create recommendation
            rec_id = await db.create_recommendation(
                underlying=spread.underlying,
                spread_type=spread.spread_type,
                short_strike=spread.short_strike,
                long_strike=spread.long_strike,
                expiration=spread.expiration,
                credit=spread.credit,
                max_loss=spread.max_loss,
                expires_at=datetime.now() + timedelta(minutes=15),
                iv_rank=iv_metrics.iv_rank,
                delta=short_delta,
                theta=short_theta,
                thesis=analysis.thesis,
                confidence=analysis.confidence,
                suggested_contracts=adjusted_contracts,
                analysis_price=spread.credit,
            )

            # Get the full recommendation
            rec = await db.get_recommendation(rec_id)

            # Send to Discord
            message_id = await discord.send_recommendation(rec)
            await db.set_recommendation_discord_message_id(rec_id, message_id)

            recommendations_sent += 1
            print(f"Sent recommendation for {spread.underlying}: {rec_id}")

            # Auto-approve if enabled
            auto_approve = getattr(env, "AUTO_APPROVE_TRADES", "false").lower() == "true"
            if auto_approve:
                try:
                    # Build OCC symbols
                    exp_parts = spread.expiration.split("-")
                    exp_str = exp_parts[0][2:] + exp_parts[1] + exp_parts[2]
                    option_type = "P" if spread.spread_type == SpreadType.BULL_PUT else "C"
                    short_symbol = f"{spread.underlying}{exp_str}{option_type}{int(spread.short_strike * 1000):08d}"
                    long_symbol = f"{spread.underlying}{exp_str}{option_type}{int(spread.long_strike * 1000):08d}"

                    # Place order
                    from core.broker.types import SpreadOrder
                    spread_order = SpreadOrder(
                        underlying=spread.underlying,
                        short_symbol=short_symbol,
                        long_symbol=long_symbol,
                        contracts=adjusted_contracts,
                        limit_price=spread.credit,
                    )
                    order = await alpaca.place_spread_order(spread_order)

                    # Update recommendation status
                    await db.update_recommendation_status(rec_id, RecommendationStatus.APPROVED)

                    # Create trade record with pending_fill status
                    # The position monitor will verify the order filled and update to 'open'
                    trade_id = await db.create_trade(
                        recommendation_id=rec_id,
                        underlying=spread.underlying,
                        spread_type=spread.spread_type,
                        short_strike=spread.short_strike,
                        long_strike=spread.long_strike,
                        expiration=spread.expiration,
                        entry_credit=spread.credit,
                        contracts=adjusted_contracts,
                        broker_order_id=order.id,
                        status=TradeStatus.PENDING_FILL,
                    )

                    # Update Discord message to show order placed (pending fill)
                    await discord.update_message(
                        message_id=message_id,
                        content=f"**Order Placed: {spread.underlying}** (awaiting fill)",
                        embeds=[{
                            "title": f"Trade Order Placed: {spread.underlying}",
                            "description": "Order submitted - awaiting fill confirmation",
                            "color": 0xFEE75C,  # Yellow for pending
                            "fields": [
                                {"name": "Strategy", "value": spread.spread_type.value.replace("_", " ").title(), "inline": True},
                                {"name": "Expiration", "value": spread.expiration, "inline": True},
                                {"name": "Strikes", "value": f"${spread.short_strike:.2f}/${spread.long_strike:.2f}", "inline": True},
                                {"name": "Credit", "value": f"${spread.credit:.2f}", "inline": True},
                                {"name": "Contracts", "value": str(adjusted_contracts), "inline": True},
                                {"name": "Order ID", "value": order.id, "inline": True},
                            ],
                        }],
                        components=[],  # Remove buttons
                    )

                    # Don't update daily stats yet - wait for fill confirmation
                    print(f"Order placed (pending fill): {trade_id}, Order: {order.id}")

                except Exception as e:
                    print(f"Error auto-approving trade: {e}")

        except Exception as e:
            import traceback
            print(f"Error processing opportunity: {e}")
            print(traceback.format_exc())

    print(f"Morning scan complete. Sent {recommendations_sent} recommendations.")
