"""Morning scan worker - runs at 9:35 AM ET.

Scans options chains for credit spread opportunities and sends
recommendations to Discord for approval.
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


# Underlyings to scan
UNDERLYINGS = ["SPY", "QQQ", "IWM"]

# Maximum recommendations per scan
MAX_RECOMMENDATIONS = 3


async def handle_morning_scan(env):
    """Run the morning options scan."""
    print("Starting morning scan...")

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)
    circuit_breaker = CircuitBreaker(kv)

    # Check circuit breaker first
    if not await circuit_breaker.is_trading_allowed():
        status = await circuit_breaker.check_status()
        print(f"Trading halted: {status.reason}")
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
    if not await alpaca.is_market_open():
        print("Market is closed, skipping scan")
        return

    # Get account info for position sizing
    account = await alpaca.get_account()
    positions = await db.get_all_positions()
    open_trades = await db.get_open_trades()

    # Get playbook rules for AI context
    playbook_rules = await db.get_playbook_rules()

    # Initialize screener
    screener = OptionsScreener(ScreenerConfig())
    sizer = PositionSizer()

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

            # Calculate IV metrics (using ATM options as proxy)
            atm_contracts = [
                c for c in chain.contracts
                if abs(c.strike - chain.underlying_price) < chain.underlying_price * 0.02
            ]

            if atm_contracts and atm_contracts[0].implied_volatility:
                current_iv = atm_contracts[0].implied_volatility
            else:
                current_iv = 0.20  # Default

            # For a production system, you'd load historical IV from R2/D1
            # For now, use a synthetic IV rank based on current IV
            iv_metrics = calculate_iv_metrics(current_iv, [current_iv * 0.8, current_iv * 1.2])

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
            )

            if size_result.contracts == 0:
                print(f"Position size is 0 for {spread.underlying}: {size_result.reason}")
                continue

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
                delta=spread.short_contract.greeks.delta if spread.short_contract.greeks else None,
                theta=spread.short_contract.greeks.theta if spread.short_contract.greeks else None,
                thesis=analysis.thesis,
                confidence=analysis.confidence,
                suggested_contracts=size_result.contracts,
                analysis_price=spread.credit,
            )

            # Get the full recommendation
            rec = await db.get_recommendation(rec_id)

            # Send to Discord
            message_id = await discord.send_recommendation(rec)
            await db.set_recommendation_discord_message_id(rec_id, message_id)

            recommendations_sent += 1
            print(f"Sent recommendation for {spread.underlying}: {rec_id}")

        except Exception as e:
            print(f"Error processing opportunity: {e}")

    print(f"Morning scan complete. Sent {recommendations_sent} recommendations.")
