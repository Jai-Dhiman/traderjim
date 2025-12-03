"""Main entry point for Cloudflare Workers.

Routes incoming requests (HTTP and cron) to appropriate handlers.
"""

from datetime import datetime

from workers import Response

from handlers.afternoon_scan import handle_afternoon_scan
from handlers.discord_webhook import handle_discord_webhook
from handlers.eod_summary import handle_eod_summary
from handlers.health import handle_health
from handlers.midday_check import handle_midday_check
from handlers.morning_scan import handle_morning_scan
from handlers.position_monitor import handle_position_monitor


async def on_fetch(request, env):
    """Handle HTTP requests."""
    import json

    url = request.url
    method = request.method

    try:
        # Health check
        if "/health" in url:
            return await handle_health(request, env)

        # Discord webhook - handle both /discord and root POST (Discord sometimes ignores path)
        if method == "POST" and ("/discord" in url or url.rstrip("/").endswith(".workers.dev")):
            return await handle_discord_webhook(request, env)

        # Test endpoints (for development only)
        if "/test/alpaca" in url:
            from core.broker.alpaca import AlpacaClient

            alpaca = AlpacaClient(
                api_key=env.ALPACA_API_KEY,
                secret_key=env.ALPACA_SECRET_KEY,
                paper=(env.ENVIRONMENT == "paper"),
            )
            account = await alpaca.get_account()
            market_open = await alpaca.is_market_open()
            return Response(
                json.dumps(
                    {
                        "status": "ok",
                        "account": {
                            "equity": account.equity,
                            "cash": account.cash,
                            "buying_power": account.buying_power,
                        },
                        "market_open": market_open,
                    }
                ),
                headers={"Content-Type": "application/json"},
            )

        if "/test/scan" in url:
            await handle_morning_scan(env)
            return Response(
                '{"status": "ok", "message": "Morning scan completed"}',
                headers={"Content-Type": "application/json"},
            )

        if "/test/db" in url:
            from core.db.d1 import D1Client

            db = D1Client(env.MAHLER_DB)
            rules = await db.get_playbook_rules()
            return Response(
                json.dumps(
                    {
                        "status": "ok",
                        "playbook_rules_count": len(rules),
                        "sample_rules": [r.rule for r in rules[:3]],
                    }
                ),
                headers={"Content-Type": "application/json"},
            )

        if "/test/discord" in url:
            from core.notifications.discord import DiscordClient

            discord = DiscordClient(
                bot_token=env.DISCORD_BOT_TOKEN,
                public_key=env.DISCORD_PUBLIC_KEY,
                channel_id=env.DISCORD_CHANNEL_ID,
            )
            message_id = await discord.send_message(
                content="Mahler test message - if you see this, Discord integration is working!",
                embeds=[
                    {
                        "title": "System Test",
                        "description": "All systems operational",
                        "color": 0x00FF00,
                        "fields": [
                            {"name": "Environment", "value": env.ENVIRONMENT, "inline": True},
                            {
                                "name": "Timestamp",
                                "value": datetime.now().isoformat(),
                                "inline": True,
                            },
                        ],
                    }
                ],
            )
            return Response(
                json.dumps(
                    {
                        "status": "ok",
                        "message_id": message_id,
                        "channel_id": env.DISCORD_CHANNEL_ID,
                    }
                ),
                headers={"Content-Type": "application/json"},
            )

        # Default response
        return Response(
            '{"status": "ok", "service": "mahler"}',
            headers={"Content-Type": "application/json"},
        )

    except Exception as e:
        import traceback

        print(f"Error handling request: {e}")
        print(traceback.format_exc())
        return Response(
            json.dumps({"error": str(e)}),
            status=500,
            headers={"Content-Type": "application/json"},
        )


async def on_scheduled(event, env, ctx):
    """Handle cron triggers."""
    cron = event.cron

    try:
        print(f"Cron triggered: {cron} at {datetime.now().isoformat()}")

        # Route based on cron pattern
        if cron == "35 14 * * MON-FRI":
            await handle_morning_scan(env)
        elif cron == "0 17 * * MON-FRI":
            await handle_midday_check(env)
        elif cron == "30 20 * * MON-FRI":
            await handle_afternoon_scan(env)
        elif cron == "15 21 * * MON-FRI":
            await handle_eod_summary(env)
        elif "*/5" in cron:
            await handle_position_monitor(env)
        else:
            print(f"Unknown cron pattern: {cron}")

    except Exception as e:
        print(f"Error in scheduled handler: {e}")
        raise


# Cloudflare Workers entry points
def fetch(request, env):
    """Fetch handler for HTTP requests."""
    return on_fetch(request, env)


def scheduled(event, env, ctx):
    """Scheduled handler for cron triggers."""
    ctx.wait_until(on_scheduled(event, env, ctx))
