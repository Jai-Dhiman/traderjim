"""Position monitor worker - runs every 5 minutes during market hours.

Monitors open positions for exit conditions:
- 50% profit target
- 200% stop loss
- 21 DTE time exit
"""

from datetime import datetime

from core.broker.alpaca import AlpacaClient
from core.db.d1 import D1Client
from core.db.kv import KVClient
from core.notifications.discord import DiscordClient
from core.risk.circuit_breaker import CircuitBreaker
from core.risk.validators import ExitValidator


async def handle_position_monitor(env):
    """Monitor positions for exit conditions."""
    print("Starting position monitor...")

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)
    circuit_breaker = CircuitBreaker(kv)

    # Check circuit breaker
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

    # Check if market is open
    if not await alpaca.is_market_open():
        print("Market is closed, skipping monitor")
        return

    # Get open trades from database
    open_trades = await db.get_open_trades()

    if not open_trades:
        print("No open trades to monitor")
        return

    print(f"Monitoring {len(open_trades)} open trades...")

    exit_validator = ExitValidator()

    # Get account info for circuit breaker checks
    account = await alpaca.get_account()

    # Run circuit breaker checks
    daily_stats = await kv.get_daily_stats()
    starting_equity = daily_stats.get("starting_equity", account.equity)

    ok, reason = await circuit_breaker.run_all_checks(
        starting_daily_equity=starting_equity,
        starting_weekly_equity=starting_equity,  # Would need weekly tracking
        peak_equity=starting_equity,  # Would need peak tracking
        current_equity=account.equity,
    )

    if not ok:
        await discord.send_circuit_breaker_alert(reason)
        return

    # Process each open trade
    for trade in open_trades:
        try:
            # Get current prices
            chain = await alpaca.get_options_chain(trade.underlying)

            # Find our contracts
            exp_parts = trade.expiration.split("-")
            exp_str = exp_parts[0][2:] + exp_parts[1] + exp_parts[2]
            option_type = "P" if trade.spread_type.value == "bull_put" else "C"

            short_symbol = (
                f"{trade.underlying}{exp_str}{option_type}{int(trade.short_strike * 1000):08d}"
            )
            long_symbol = (
                f"{trade.underlying}{exp_str}{option_type}{int(trade.long_strike * 1000):08d}"
            )

            short_contract = next((c for c in chain.contracts if c.symbol == short_symbol), None)
            long_contract = next((c for c in chain.contracts if c.symbol == long_symbol), None)

            if not short_contract or not long_contract:
                print(f"Could not find contracts for trade {trade.id}")
                continue

            # Calculate current value (cost to close)
            # To close: buy back short, sell long
            current_value = short_contract.mid - long_contract.mid
            unrealized_pnl = (trade.entry_credit - current_value) * trade.contracts * 100

            # Update position in database
            await db.upsert_position(
                trade_id=trade.id,
                underlying=trade.underlying,
                short_strike=trade.short_strike,
                long_strike=trade.long_strike,
                expiration=trade.expiration,
                contracts=trade.contracts,
                current_value=current_value,
                unrealized_pnl=unrealized_pnl,
            )

            # Check exit conditions
            should_exit, exit_reason = exit_validator.check_all_exit_conditions(
                entry_credit=trade.entry_credit,
                current_value=current_value,
                expiration=trade.expiration,
            )

            if should_exit:
                print(f"Exit triggered for {trade.underlying}: {exit_reason}")

                # Send exit alert to Discord
                await discord.send_exit_alert(
                    trade=trade,
                    reason=exit_reason,
                    current_value=current_value,
                    unrealized_pnl=unrealized_pnl,
                )

        except Exception as e:
            print(f"Error monitoring trade {trade.id}: {e}")
            await circuit_breaker.check_api_errors()

    print("Position monitor complete.")
