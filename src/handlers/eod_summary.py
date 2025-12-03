"""EOD summary worker - runs at 4:15 PM ET.

Generates daily summary with:
- P/L for the day
- Open positions
- AI reflection on closed trades
- Archives data to R2
"""

from datetime import datetime

from core.ai.claude import ClaudeClient
from core.broker.alpaca import AlpacaClient
from core.db.d1 import D1Client
from core.db.kv import KVClient
from core.db.r2 import R2Client
from core.notifications.discord import DiscordClient
from core.types import TradeStatus


async def handle_eod_summary(env):
    """Generate end-of-day summary."""
    print("Starting EOD summary...")

    today = datetime.now().strftime("%Y-%m-%d")

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)
    r2 = R2Client(env.ARCHIVE)

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

    # Get account info
    account = await alpaca.get_account()

    # Get or create daily performance record
    daily_stats = await kv.get_daily_stats(today)
    starting_balance = daily_stats.get("starting_equity", account.equity)

    performance = await db.get_or_create_daily_performance(
        date=today,
        starting_balance=starting_balance,
    )

    # Update ending balance
    await db.update_daily_performance(
        date=today,
        ending_balance=account.equity,
    )

    # Refresh performance
    performance = await db.get_or_create_daily_performance(
        date=today,
        starting_balance=starting_balance,
    )

    # Get positions
    positions = await db.get_all_positions()
    open_trades = await db.get_open_trades()

    # Get trade stats
    trade_stats = await db.get_trade_stats()

    # Generate reflections for trades closed today
    closed_today = []
    all_trades_result = await db.execute(
        "SELECT * FROM trades WHERE status = 'closed' AND closed_at LIKE ?",
        [f"{today}%"],
    )

    for row in all_trades_result.results:
        trade = db._row_to_trade(row)
        closed_today.append(trade)

    # Generate AI reflections for closed trades
    for trade in closed_today:
        if trade.reflection:
            continue  # Already has reflection

        try:
            # Get original thesis from recommendation
            rec = (
                await db.get_recommendation(trade.recommendation_id)
                if trade.recommendation_id
                else None
            )
            original_thesis = rec.thesis if rec else None

            reflection = await claude.generate_reflection(trade, original_thesis)

            # Update trade with reflection
            await db.run(
                "UPDATE trades SET reflection = ?, lesson = ? WHERE id = ?",
                [reflection.reflection, reflection.lesson, trade.id],
            )

            print(f"Generated reflection for trade {trade.id}")

        except Exception as e:
            print(f"Error generating reflection: {e}")

    # Check for playbook updates if we have enough closed trades
    if len(closed_today) >= 2:
        try:
            # Get recent trades with reflections
            recent_with_reflections = [t for t in closed_today if t.reflection]

            if recent_with_reflections:
                playbook_rules = await db.get_playbook_rules()
                updates = await claude.suggest_playbook_updates(
                    recent_trades=recent_with_reflections,
                    current_rules=playbook_rules,
                )

                for new_rule in updates.new_rules:
                    await db.add_playbook_rule(
                        rule=new_rule["rule"],
                        source="learned",
                        supporting_trade_ids=new_rule.get("supporting_trades", []),
                    )
                    print(f"Added playbook rule: {new_rule['rule']}")

        except Exception as e:
            print(f"Error updating playbook: {e}")

    # Archive daily snapshot to R2
    try:
        positions_data = [
            {
                "trade_id": p.trade_id,
                "underlying": p.underlying,
                "short_strike": p.short_strike,
                "long_strike": p.long_strike,
                "expiration": p.expiration,
                "contracts": p.contracts,
                "current_value": p.current_value,
                "unrealized_pnl": p.unrealized_pnl,
            }
            for p in positions
        ]

        await r2.archive_daily_snapshot(
            date=today,
            positions=positions_data,
            performance={
                "starting_balance": performance.starting_balance,
                "ending_balance": performance.ending_balance,
                "realized_pnl": performance.realized_pnl,
                "trades_opened": performance.trades_opened,
                "trades_closed": performance.trades_closed,
                "win_count": performance.win_count,
                "loss_count": performance.loss_count,
            },
            account={
                "equity": account.equity,
                "cash": account.cash,
                "buying_power": account.buying_power,
            },
        )
        print(f"Archived daily snapshot for {today}")

    except Exception as e:
        print(f"Error archiving snapshot: {e}")

    # Send Discord summary
    await discord.send_daily_summary(
        performance=performance,
        open_positions=len(open_trades),
        trade_stats=trade_stats,
    )

    # Reset daily KV stats for next day
    tomorrow = datetime.now().replace(hour=0, minute=0, second=0) + __import__(
        "datetime"
    ).timedelta(days=1)
    # Store starting equity for tomorrow
    await kv.put_json(
        f"daily:{tomorrow.strftime('%Y-%m-%d')}",
        {"starting_equity": account.equity, "trades_count": 0, "realized_pnl": 0},
        expiration_ttl=7 * 24 * 3600,
    )

    print("EOD summary complete.")
