"""Midday check worker - runs at 12:00 PM ET.

Lighter version of morning scan - checks positions and
looks for new setups if capacity available.
"""

from datetime import datetime, timedelta

from core.ai.claude import ClaudeClient
from core.analysis.iv_rank import calculate_iv_metrics
from core.analysis.screener import OptionsScreener, ScreenerConfig
from core.broker.alpaca import AlpacaClient
from core.db.d1 import D1Client
from core.db.kv import KVClient
from core.notifications.discord import DiscordClient
from core.risk.circuit_breaker import CircuitBreaker
from core.risk.position_sizer import PositionSizer
from core.types import Confidence

UNDERLYINGS = ["SPY", "QQQ", "IWM"]
MAX_RECOMMENDATIONS = 2  # Fewer than morning scan


async def handle_midday_check(env):
    """Run midday position check and opportunity scan."""
    print("Starting midday check...")

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)
    circuit_breaker = CircuitBreaker(kv)

    # Check circuit breaker
    if not await circuit_breaker.is_trading_allowed():
        status = await circuit_breaker.check_status()
        print(f"Trading halted: {status.reason}")
        return

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
    if not await alpaca.is_market_open():
        print("Market is closed, skipping midday check")
        return

    # Get account and position info
    account = await alpaca.get_account()
    positions = await db.get_all_positions()
    open_trades = await db.get_open_trades()

    sizer = PositionSizer()
    heat = sizer.calculate_portfolio_heat(positions, account.equity)

    # If at heat limit, skip scanning for new opportunities
    if heat["at_limit"]:
        print(f"Portfolio heat at limit ({heat['heat_percent']:.1%}), skipping scan")
        return

    # Check for pending recommendations that haven't been acted on
    pending = await db.get_pending_recommendations()
    if pending:
        print(f"Found {len(pending)} pending recommendations, skipping new scan")
        return

    # Run lighter scan (only if capacity available)
    playbook_rules = await db.get_playbook_rules()
    screener = OptionsScreener(ScreenerConfig())

    all_opportunities = []

    for symbol in UNDERLYINGS:
        try:
            chain = await alpaca.get_options_chain(symbol)
            if not chain.contracts:
                continue

            # Quick IV estimate
            atm = [
                c
                for c in chain.contracts
                if abs(c.strike - chain.underlying_price) < chain.underlying_price * 0.02
            ]
            current_iv = atm[0].implied_volatility if atm and atm[0].implied_volatility else 0.20
            iv_metrics = calculate_iv_metrics(current_iv, [current_iv * 0.8, current_iv * 1.2])

            # Only proceed if IV is elevated
            if iv_metrics.iv_rank < 50:
                continue

            opportunities = screener.screen_chain(chain, iv_metrics)
            for opp in opportunities[:1]:  # Just top 1 per symbol at midday
                all_opportunities.append((opp, chain.underlying_price, iv_metrics))

        except Exception as e:
            print(f"Error scanning {symbol}: {e}")

    # Process best opportunity only
    if all_opportunities:
        all_opportunities.sort(key=lambda x: x[0].score, reverse=True)
        opp, underlying_price, iv_metrics = all_opportunities[0]
        spread = opp.spread

        size_result = sizer.calculate_size(
            spread=spread,
            account_equity=account.equity,
            current_positions=positions,
        )

        if size_result.contracts > 0:
            try:
                analysis = await claude.analyze_trade(
                    spread=spread,
                    underlying_price=underlying_price,
                    iv_rank=iv_metrics.iv_rank,
                    current_iv=iv_metrics.current_iv,
                    playbook_rules=playbook_rules,
                )

                if analysis.confidence != Confidence.LOW:
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
                        delta=spread.short_contract.greeks.delta
                        if spread.short_contract.greeks
                        else None,
                        theta=spread.short_contract.greeks.theta
                        if spread.short_contract.greeks
                        else None,
                        thesis=analysis.thesis,
                        confidence=analysis.confidence,
                        suggested_contracts=size_result.contracts,
                        analysis_price=spread.credit,
                    )

                    rec = await db.get_recommendation(rec_id)
                    message_id = await discord.send_recommendation(rec)
                    await db.set_recommendation_discord_message_id(rec_id, message_id)

                    print(f"Sent midday recommendation: {rec_id}")

            except Exception as e:
                print(f"Error processing opportunity: {e}")

    print("Midday check complete.")
