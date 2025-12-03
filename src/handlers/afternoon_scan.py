"""Afternoon scan worker - runs at 3:30 PM ET.

Final scan before market close. More conservative - only very high
probability setups with excellent IV conditions.
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


async def handle_afternoon_scan(env):
    """Run afternoon scan with stricter criteria."""
    print("Starting afternoon scan...")

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)
    circuit_breaker = CircuitBreaker(kv)

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

    if not await alpaca.is_market_open():
        print("Market is closed, skipping afternoon scan")
        return

    account = await alpaca.get_account()
    positions = await db.get_all_positions()

    sizer = PositionSizer()
    heat = sizer.calculate_portfolio_heat(positions, account.equity)

    if heat["at_limit"]:
        print("Portfolio heat at limit, skipping scan")
        return

    # More conservative config for afternoon
    config = ScreenerConfig(
        min_dte=35,  # Slightly longer DTE
        max_dte=45,
        min_delta=0.15,  # More OTM
        max_delta=0.25,
        min_iv_rank=60.0,  # Higher IV requirement
        min_credit_pct=0.30,  # Better premium requirement
    )

    screener = OptionsScreener(config)
    playbook_rules = await db.get_playbook_rules()

    best_opportunity = None
    best_score = 0

    for symbol in UNDERLYINGS:
        try:
            chain = await alpaca.get_options_chain(symbol)
            if not chain.contracts:
                continue

            atm = [c for c in chain.contracts
                   if abs(c.strike - chain.underlying_price) < chain.underlying_price * 0.02]
            current_iv = atm[0].implied_volatility if atm and atm[0].implied_volatility else 0.20
            iv_metrics = calculate_iv_metrics(current_iv, [current_iv * 0.8, current_iv * 1.2])

            # Stricter IV requirement for afternoon
            if iv_metrics.iv_rank < 60:
                continue

            opportunities = screener.screen_chain(chain, iv_metrics)
            if opportunities and opportunities[0].score > best_score:
                best_score = opportunities[0].score
                best_opportunity = (opportunities[0], chain.underlying_price, iv_metrics)

        except Exception as e:
            print(f"Error scanning {symbol}: {e}")

    # Only send if we found a high-quality setup
    if best_opportunity and best_score > 0.6:  # Higher score threshold
        opp, underlying_price, iv_metrics = best_opportunity
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

                # Only high confidence for afternoon
                if analysis.confidence == Confidence.HIGH:
                    rec_id = await db.create_recommendation(
                        underlying=spread.underlying,
                        spread_type=spread.spread_type,
                        short_strike=spread.short_strike,
                        long_strike=spread.long_strike,
                        expiration=spread.expiration,
                        credit=spread.credit,
                        max_loss=spread.max_loss,
                        expires_at=datetime.now() + timedelta(minutes=10),  # Shorter expiry
                        iv_rank=iv_metrics.iv_rank,
                        delta=spread.short_contract.greeks.delta if spread.short_contract.greeks else None,
                        theta=spread.short_contract.greeks.theta if spread.short_contract.greeks else None,
                        thesis=analysis.thesis,
                        confidence=analysis.confidence,
                        suggested_contracts=size_result.contracts,
                        analysis_price=spread.credit,
                    )

                    rec = await db.get_recommendation(rec_id)
                    message_id = await discord.send_recommendation(rec)
                    await db.set_recommendation_discord_message_id(rec_id, message_id)

                    print(f"Sent afternoon recommendation: {rec_id}")

            except Exception as e:
                print(f"Error processing opportunity: {e}")

    print("Afternoon scan complete.")
