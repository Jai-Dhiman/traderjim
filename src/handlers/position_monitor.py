"""Position monitor worker - runs every 5 minutes during market hours.

Monitors open positions for exit conditions:
- 50% profit target
- 200% stop loss (configurable based on win rate)
- 21 DTE time exit
"""

from datetime import datetime

from core import http
from core.broker.alpaca import AlpacaClient
from core.broker.types import OrderStatus
from core.db.d1 import D1Client
from core.db.kv import KVClient
from core.notifications.discord import DiscordClient
from core.risk.circuit_breaker import CircuitBreaker, RiskLevel
from core.risk.validators import ExitConfig, ExitValidator
from core.types import TradeStatus


async def handle_position_monitor(env):
    """Monitor positions for exit conditions."""
    print("Starting position monitor...")

    # Signal start to heartbeat monitor
    heartbeat_url = getattr(env, "HEARTBEAT_URL", None)
    await http.ping_heartbeat_start(heartbeat_url, "position_monitor")

    job_success = False
    try:
        await _run_position_monitor(env)
        job_success = True
    finally:
        await http.ping_heartbeat(heartbeat_url, "position_monitor", success=job_success)


async def _run_position_monitor(env):
    """Internal position monitor logic."""

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

    # Step 1: Reconcile pending_fill trades (check if orders filled or expired)
    await _reconcile_pending_orders(db, alpaca, discord, kv)

    # Check if market is open
    if not await alpaca.is_market_open():
        print("Market is closed, skipping monitor")
        return

    # Get open trades from database (only trades with confirmed fills)
    open_trades = await db.get_open_trades()

    if not open_trades:
        print("No open trades to monitor")
        return

    print(f"Monitoring {len(open_trades)} open trades...")

    exit_validator = ExitValidator()

    # Get account info for circuit breaker checks
    account = await alpaca.get_account()

    # Run graduated circuit breaker checks
    daily_stats = await kv.get_daily_stats()
    daily_starting_equity = daily_stats.get("starting_equity", account.equity)

    weekly_starting_equity = await kv.get_weekly_starting_equity()
    if weekly_starting_equity == 0:
        weekly_starting_equity = account.equity  # Fallback if not initialized

    risk_state = await circuit_breaker.evaluate_all(
        starting_daily_equity=daily_starting_equity,
        starting_weekly_equity=weekly_starting_equity,
        peak_equity=max(daily_starting_equity, weekly_starting_equity),
        current_equity=account.equity,
    )

    # Log risk state
    if risk_state.level != RiskLevel.NORMAL:
        print(f"Risk level: {risk_state.level.value}, size multiplier: {risk_state.size_multiplier}")
        if risk_state.reason:
            print(f"Reason: {risk_state.reason}")

    # Send alert if needed
    if risk_state.should_alert and risk_state.reason:
        await discord.send_circuit_breaker_alert(risk_state.reason)

    # If halted, stop processing
    if risk_state.level == RiskLevel.HALTED:
        print(f"Trading halted: {risk_state.reason}")
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

                # Check if auto-execute is enabled
                auto_execute = getattr(env, "AUTO_APPROVE_TRADES", "false").lower() == "true"

                if auto_execute:
                    # Auto-execute the exit
                    await _auto_execute_exit(
                        trade=trade,
                        short_symbol=short_symbol,
                        long_symbol=long_symbol,
                        current_value=current_value,
                        unrealized_pnl=unrealized_pnl,
                        exit_reason=exit_reason,
                        alpaca=alpaca,
                        db=db,
                        discord=discord,
                        kv=kv,
                    )
                else:
                    # Send exit alert with buttons for manual approval
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


async def _reconcile_pending_orders(db, alpaca, discord, kv):
    """Reconcile pending_fill trades by checking their broker order status.

    This ensures we only track trades that actually filled, and properly
    handle orders that expired or were cancelled.
    """
    pending_trades = await db.get_pending_fill_trades()

    if not pending_trades:
        return

    print(f"Reconciling {len(pending_trades)} pending orders...")

    for trade in pending_trades:
        if not trade.broker_order_id:
            print(f"Trade {trade.id} has no broker_order_id, marking as expired")
            await db.update_trade_status(trade.id, TradeStatus.EXPIRED)
            continue

        try:
            order = await alpaca.get_order(trade.broker_order_id)

            if order.status == OrderStatus.FILLED:
                # Order filled - mark trade as open
                print(f"Order {order.id} FILLED - activating trade {trade.id}")
                await db.mark_trade_filled(trade.id)

                # Update daily stats now that we have a confirmed fill
                await kv.update_daily_stats(trades_delta=1)

                # Send Discord notification
                await discord.send_message(
                    content=f"**Trade Filled: {trade.underlying}**",
                    embeds=[{
                        "title": f"Order Filled: {trade.underlying}",
                        "description": "Your order has been filled and position is now active.",
                        "color": 0x57F287,  # Green
                        "fields": [
                            {"name": "Strategy", "value": trade.spread_type.value.replace("_", " ").title(), "inline": True},
                            {"name": "Strikes", "value": f"${trade.short_strike:.2f}/${trade.long_strike:.2f}", "inline": True},
                            {"name": "Credit", "value": f"${trade.entry_credit:.2f}", "inline": True},
                            {"name": "Contracts", "value": str(trade.contracts), "inline": True},
                        ],
                    }],
                )

            elif order.status in [OrderStatus.EXPIRED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                # Order did not fill - mark trade as expired
                print(f"Order {order.id} {order.status.value} - expiring trade {trade.id}")
                await db.update_trade_status(trade.id, TradeStatus.EXPIRED)

                # Delete any position snapshot for this trade
                await db.delete_position(trade.id)

                # Send Discord notification
                await discord.send_message(
                    content=f"**Order Expired: {trade.underlying}**",
                    embeds=[{
                        "title": f"Order {order.status.value.title()}: {trade.underlying}",
                        "description": "The limit order did not fill before expiration.",
                        "color": 0xED4245,  # Red
                        "fields": [
                            {"name": "Strategy", "value": trade.spread_type.value.replace("_", " ").title(), "inline": True},
                            {"name": "Strikes", "value": f"${trade.short_strike:.2f}/${trade.long_strike:.2f}", "inline": True},
                            {"name": "Limit Price", "value": f"${trade.entry_credit:.2f}", "inline": True},
                        ],
                    }],
                )

            else:
                # Order still pending (new, accepted, partially_filled)
                print(f"Order {order.id} still pending ({order.status.value})")

        except Exception as e:
            print(f"Error reconciling order for trade {trade.id}: {e}")


async def _auto_execute_exit(
    trade,
    short_symbol,
    long_symbol,
    current_value,
    unrealized_pnl,
    exit_reason,
    alpaca,
    db,
    discord,
    kv,
):
    """Auto-execute an exit when conditions are met.

    Places a closing order and updates the database.
    """
    try:
        print(f"Auto-executing exit for {trade.underlying}: {exit_reason}")

        # Place closing order (buy back short, sell long)
        order = await alpaca.place_close_spread_order(
            short_symbol=short_symbol,
            long_symbol=long_symbol,
            contracts=trade.contracts,
            limit_price=current_value,  # Close at current mid price
        )

        print(f"Exit order placed: {order.id}")

        # Close the trade in database
        await db.close_trade(
            trade_id=trade.id,
            exit_debit=current_value,
        )

        # Delete position snapshot
        await db.delete_position(trade.id)

        # Calculate realized P/L
        realized_pnl = (trade.entry_credit - current_value) * trade.contracts * 100

        # Update daily stats
        await kv.update_daily_stats(pnl_delta=realized_pnl)

        # Send Discord notification (no buttons)
        pnl_color = 0x57F287 if realized_pnl > 0 else 0xED4245  # Green or Red
        pnl_emoji = "+" if realized_pnl > 0 else ""

        await discord.send_message(
            content=f"**Position Closed: {trade.underlying}** - {exit_reason}",
            embeds=[{
                "title": f"Position Closed: {trade.underlying}",
                "color": pnl_color,
                "fields": [
                    {"name": "Reason", "value": exit_reason, "inline": False},
                    {"name": "Strategy", "value": trade.spread_type.value.replace("_", " ").title(), "inline": True},
                    {"name": "Strikes", "value": f"${trade.short_strike:.2f}/${trade.long_strike:.2f}", "inline": True},
                    {"name": "Contracts", "value": str(trade.contracts), "inline": True},
                    {"name": "Entry Credit", "value": f"${trade.entry_credit:.2f}", "inline": True},
                    {"name": "Exit Debit", "value": f"${current_value:.2f}", "inline": True},
                    {"name": "Realized P/L", "value": f"{pnl_emoji}${realized_pnl:.2f}", "inline": True},
                    {"name": "Order ID", "value": order.id, "inline": False},
                ],
            }],
        )

        print(f"Exit complete for {trade.underlying}: P/L ${realized_pnl:.2f}")

    except Exception as e:
        print(f"Error auto-executing exit for {trade.id}: {e}")
        # Send error notification
        await discord.send_message(
            content=f"**Exit Error: {trade.underlying}**",
            embeds=[{
                "title": f"Exit Failed: {trade.underlying}",
                "color": 0xED4245,
                "description": f"Auto-exit failed: {str(e)}",
                "fields": [
                    {"name": "Reason", "value": exit_reason, "inline": False},
                    {"name": "Trade ID", "value": trade.id, "inline": True},
                ],
            }],
        )
