"""Discord webhook handler for interactive buttons."""

import json
from workers import Response

from core.broker.alpaca import AlpacaClient
from core.broker.types import SpreadOrder
from core.db.d1 import D1Client
from core.db.kv import KVClient
from core.notifications.discord import DiscordClient
from core.risk.circuit_breaker import CircuitBreaker
from core.risk.validators import TradeValidator
from core.types import RecommendationStatus, SpreadType


async def handle_discord_webhook(request, env):
    """Handle Discord interaction webhooks (button clicks)."""
    print("Discord webhook received!")

    # Parse request
    body = await request.text()
    timestamp = request.headers.get("X-Signature-Timestamp", "")
    signature = request.headers.get("X-Signature-Ed25519", "")

    print(f"Body length: {len(body)}, timestamp: {timestamp}, signature length: {len(signature) if signature else 0}")

    # Initialize Discord client for signature verification
    discord = DiscordClient(
        bot_token=env.DISCORD_BOT_TOKEN,
        public_key=env.DISCORD_PUBLIC_KEY,
        channel_id=env.DISCORD_CHANNEL_ID,
    )

    # Verify signature (async)
    if not await discord.verify_signature(body, timestamp, signature):
        return Response('{"error": "Invalid signature"}', status=401)

    # Parse payload
    try:
        payload = json.loads(body)
    except Exception as e:
        return Response(f'{{"error": "Invalid payload: {e}"}}', status=400)

    # Handle Discord ping (required for interaction URL verification)
    if payload.get("type") == 1:  # PING
        return Response(
            '{"type": 1}',  # PONG
            headers={"Content-Type": "application/json"},
        )

    # Handle component interactions (button clicks)
    if payload.get("type") != 3:  # MESSAGE_COMPONENT
        return Response('{"error": "Unsupported interaction type"}', status=400)

    # Get interaction details
    interaction_id = payload.get("id")
    interaction_token = payload.get("token")
    custom_id = payload.get("data", {}).get("custom_id", "")
    message_id = payload.get("message", {}).get("id")

    print(f"Received interaction: {custom_id}")

    # Parse action and ID from custom_id (format: "action:id")
    if ":" not in custom_id:
        return Response('{"error": "Invalid custom_id format"}', status=400)

    action, entity_id = custom_id.split(":", 1)

    # Initialize clients
    db = D1Client(env.MAHLER_DB)
    kv = KVClient(env.MAHLER_KV)

    # Handle different actions
    if action == "approve_trade":
        return await handle_approve(
            env, db, kv, discord, entity_id,
            interaction_id, interaction_token, message_id
        )
    elif action == "reject_trade":
        return await handle_reject(
            env, db, discord, entity_id,
            interaction_id, interaction_token, message_id
        )
    elif action == "close_position":
        return await handle_close_position(
            env, db, kv, discord, entity_id,
            interaction_id, interaction_token
        )
    elif action == "hold_position":
        return await handle_hold_position(
            discord, interaction_id, interaction_token
        )
    else:
        return Response(f'{{"error": "Unknown action: {action}"}}', status=400)


async def handle_approve(
    env, db, kv, discord, rec_id,
    interaction_id, interaction_token, message_id
):
    """Handle trade approval."""
    try:
        # Get recommendation
        rec = await db.get_recommendation(rec_id)
        if not rec:
            await send_error_response(discord, interaction_id, interaction_token, "Recommendation not found")
            return Response('{"error": "Recommendation not found"}', status=404)

        # Validate recommendation
        validator = TradeValidator()
        validation = validator.validate_recommendation(rec)
        if not validation.valid:
            await send_error_response(discord, interaction_id, interaction_token, validation.reason)
            return Response(json.dumps({"error": validation.reason}), status=400)

        # Check circuit breaker
        circuit_breaker = CircuitBreaker(kv)
        if not await circuit_breaker.is_trading_allowed():
            status = await circuit_breaker.check_status()
            await send_error_response(discord, interaction_id, interaction_token, f"Trading halted: {status.reason}")
            return Response(json.dumps({"error": "Trading halted"}), status=400)

        # Initialize Alpaca
        alpaca = AlpacaClient(
            api_key=env.ALPACA_API_KEY,
            secret_key=env.ALPACA_SECRET_KEY,
            paper=(env.ENVIRONMENT == "paper"),
        )

        # Build OCC symbols for the spread
        exp_parts = rec.expiration.split("-")
        exp_str = exp_parts[0][2:] + exp_parts[1] + exp_parts[2]  # YYMMDD

        option_type = "P" if rec.spread_type == SpreadType.BULL_PUT else "C"
        short_symbol = f"{rec.underlying}{exp_str}{option_type}{int(rec.short_strike * 1000):08d}"
        long_symbol = f"{rec.underlying}{exp_str}{option_type}{int(rec.long_strike * 1000):08d}"

        # Place order
        spread_order = SpreadOrder(
            underlying=rec.underlying,
            short_symbol=short_symbol,
            long_symbol=long_symbol,
            contracts=rec.suggested_contracts or 1,
            limit_price=rec.credit,
        )

        order = await alpaca.place_spread_order(spread_order)

        # Update recommendation status
        await db.update_recommendation_status(rec_id, RecommendationStatus.APPROVED)

        # Create trade record
        trade_id = await db.create_trade(
            recommendation_id=rec_id,
            underlying=rec.underlying,
            spread_type=rec.spread_type,
            short_strike=rec.short_strike,
            long_strike=rec.long_strike,
            expiration=rec.expiration,
            entry_credit=rec.credit,
            contracts=rec.suggested_contracts or 1,
            broker_order_id=order.id,
        )

        # Respond to interaction with updated message
        spread_name = rec.spread_type.value.replace("_", " ").title()
        embed = {
            "title": f"Trade Approved: {rec.underlying}",
            "color": 0x57F287,
            "fields": [
                {"name": "Strategy", "value": spread_name, "inline": True},
                {"name": "Expiration", "value": rec.expiration, "inline": True},
                {"name": "Strikes", "value": f"${rec.short_strike:.2f}/${rec.long_strike:.2f}", "inline": True},
                {"name": "Credit", "value": f"${rec.credit:.2f}", "inline": True},
                {"name": "Contracts", "value": str(rec.suggested_contracts or 1), "inline": True},
                {"name": "Order ID", "value": order.id, "inline": True},
            ],
        }

        await discord.respond_to_interaction(
            interaction_id,
            interaction_token,
            content=f"**Trade Approved: {rec.underlying}**",
            embeds=[embed],
            components=[],  # Remove buttons
        )

        # Update daily stats
        await kv.update_daily_stats(trades_delta=1)

        print(f"Trade approved: {trade_id}, Order: {order.id}")

        return Response('{"type": 1}', headers={"Content-Type": "application/json"})

    except Exception as e:
        print(f"Error approving trade: {e}")
        await send_error_response(discord, interaction_id, interaction_token, str(e))
        return Response(json.dumps({"error": str(e)}), status=500)


async def handle_reject(
    env, db, discord, rec_id,
    interaction_id, interaction_token, message_id
):
    """Handle trade rejection."""
    try:
        # Get recommendation
        rec = await db.get_recommendation(rec_id)
        if not rec:
            await send_error_response(discord, interaction_id, interaction_token, "Recommendation not found")
            return Response('{"error": "Recommendation not found"}', status=404)

        # Update status
        await db.update_recommendation_status(rec_id, RecommendationStatus.REJECTED)

        # Respond to interaction with updated message
        embed = {
            "title": f"Trade Rejected: {rec.underlying}",
            "color": 0xED4245,
            "description": f"{rec.spread_type.value.replace('_', ' ').title()} | ${rec.short_strike:.2f}/${rec.long_strike:.2f} | {rec.expiration}",
        }

        await discord.respond_to_interaction(
            interaction_id,
            interaction_token,
            content=f"**Trade Rejected: {rec.underlying}**",
            embeds=[embed],
            components=[],
        )

        print(f"Trade rejected: {rec_id}")

        return Response('{"type": 1}', headers={"Content-Type": "application/json"})

    except Exception as e:
        print(f"Error rejecting trade: {e}")
        return Response(json.dumps({"error": str(e)}), status=500)


async def handle_close_position(
    env, db, kv, discord, trade_id,
    interaction_id, interaction_token
):
    """Handle position close request."""
    try:
        # Get trade
        trade = await db.get_trade(trade_id)
        if not trade:
            await send_error_response(discord, interaction_id, interaction_token, "Trade not found")
            return Response('{"error": "Trade not found"}', status=404)

        # Initialize Alpaca
        alpaca = AlpacaClient(
            api_key=env.ALPACA_API_KEY,
            secret_key=env.ALPACA_SECRET_KEY,
            paper=(env.ENVIRONMENT == "paper"),
        )

        # Build close order symbols
        exp_parts = trade.expiration.split("-")
        exp_str = exp_parts[0][2:] + exp_parts[1] + exp_parts[2]
        option_type = "P" if trade.spread_type == SpreadType.BULL_PUT else "C"

        short_symbol = f"{trade.underlying}{exp_str}{option_type}{int(trade.short_strike * 1000):08d}"
        long_symbol = f"{trade.underlying}{exp_str}{option_type}{int(trade.long_strike * 1000):08d}"

        # Get current prices
        chain = await alpaca.get_options_chain(trade.underlying)
        short_contract = next((c for c in chain.contracts if c.symbol == short_symbol), None)
        long_contract = next((c for c in chain.contracts if c.symbol == long_symbol), None)

        if short_contract and long_contract:
            close_cost = short_contract.ask - long_contract.bid
        else:
            close_cost = trade.entry_credit * 0.5

        # Place close order
        order = await alpaca.place_close_spread_order(
            short_symbol=short_symbol,
            long_symbol=long_symbol,
            contracts=trade.contracts,
            limit_price=close_cost,
        )

        print(f"Close order placed: {order.id}")

        # Respond to interaction
        await discord.respond_to_interaction(
            interaction_id,
            interaction_token,
            content=f"Close order placed for {trade.underlying}. Order ID: {order.id}",
            update_message=False,
        )

        return Response('{"type": 1}', headers={"Content-Type": "application/json"})

    except Exception as e:
        print(f"Error closing position: {e}")
        await send_error_response(discord, interaction_id, interaction_token, str(e))
        return Response(json.dumps({"error": str(e)}), status=500)


async def handle_hold_position(discord, interaction_id, interaction_token):
    """Handle hold decision (dismiss the alert)."""
    await discord.respond_to_interaction(
        interaction_id,
        interaction_token,
        content="Position held. Will continue monitoring.",
        update_message=False,
    )
    return Response('{"type": 1}', headers={"Content-Type": "application/json"})


async def send_error_response(discord, interaction_id, interaction_token, error_msg):
    """Send error response to interaction."""
    embed = {
        "title": "Error",
        "color": 0xED4245,
        "description": error_msg,
    }
    await discord.respond_to_interaction(
        interaction_id,
        interaction_token,
        content=f"**Error:** {error_msg}",
        embeds=[embed],
        update_message=False,
    )
