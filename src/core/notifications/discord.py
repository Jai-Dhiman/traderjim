from __future__ import annotations

"""Discord client for notifications with interactive buttons."""

from typing import Any

from core import http
from core.types import DailyPerformance, Recommendation, Trade


class DiscordError(Exception):
    """Discord API error."""

    pass


async def verify_ed25519_signature(public_key_hex: str, message: bytes, signature_hex: str) -> bool:
    """Verify Ed25519 signature using JavaScript SubtleCrypto."""
    try:
        from js import Object, Uint8Array, crypto
        from pyodide.ffi import to_js

        print(
            f"Verifying signature: pk_len={len(public_key_hex)}, sig_len={len(signature_hex)}, msg_len={len(message)}"
        )

        # Convert hex to bytes
        public_key_bytes = bytes.fromhex(public_key_hex)
        signature_bytes = bytes.fromhex(signature_hex)

        print(f"Converted: pk_bytes={len(public_key_bytes)}, sig_bytes={len(signature_bytes)}")

        # Create Uint8Arrays from the bytes
        pk_array = Uint8Array.new(to_js(list(public_key_bytes)))
        sig_array = Uint8Array.new(to_js(list(signature_bytes)))
        msg_array = Uint8Array.new(to_js(list(message)))

        # Import the Ed25519 public key - convert dict to JS object via Object.fromEntries
        algorithm = Object.fromEntries(to_js([["name", "Ed25519"]]))
        print(f"Algorithm object: {algorithm}")

        key = await crypto.subtle.importKey("raw", pk_array, algorithm, False, to_js(["verify"]))
        print(f"Key imported successfully")

        # Verify the signature
        result = await crypto.subtle.verify(algorithm, key, sig_array, msg_array)
        print(f"Verification result: {result}")
        return bool(result)
    except Exception as e:
        import traceback

        print(f"Ed25519 verification error: {e}")
        print(traceback.format_exc())
        return False


class DiscordClient:
    """Client for Discord notifications with interactive components."""

    BASE_URL = "https://discord.com/api/v10"

    def __init__(self, bot_token: str, public_key: str, channel_id: str):
        self.bot_token = bot_token
        self.public_key = public_key
        self.channel_id = channel_id

        self._headers = {
            "Authorization": f"Bot {bot_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, endpoint: str, data: dict | None = None) -> dict:
        """Make a request to Discord API."""
        try:
            url = f"{self.BASE_URL}{endpoint}"
            return await http.request(method, url, headers=self._headers, json_data=data)
        except Exception as e:
            raise DiscordError(f"Discord API error: {str(e)}")

    async def verify_signature(self, body: str, timestamp: str, signature: str) -> bool:
        """Verify Discord interaction signature using Ed25519."""
        message = f"{timestamp}{body}".encode()
        return await verify_ed25519_signature(self.public_key, message, signature)

    # Message sending

    async def send_message(
        self,
        content: str,
        embeds: list[dict] | None = None,
        components: list[dict] | None = None,
    ) -> str:
        """Send a message to the channel. Returns message ID."""
        data = {"content": content}
        if embeds:
            data["embeds"] = embeds
        if components:
            data["components"] = components

        result = await self._request(
            "POST",
            f"/channels/{self.channel_id}/messages",
            data,
        )
        return result["id"]

    async def update_message(
        self,
        message_id: str,
        content: str,
        embeds: list[dict] | None = None,
        components: list[dict] | None = None,
    ) -> None:
        """Update an existing message."""
        data = {"content": content}
        if embeds:
            data["embeds"] = embeds
        # Always include components if provided (even empty list to remove buttons)
        if components is not None:
            data["components"] = components

        await self._request(
            "PATCH",
            f"/channels/{self.channel_id}/messages/{message_id}",
            data,
        )

    async def respond_to_interaction(
        self,
        interaction_id: str,
        interaction_token: str,
        content: str,
        embeds: list[dict] | None = None,
        components: list[dict] | None = None,
        update_message: bool = True,
    ) -> None:
        """Respond to a Discord interaction (button click)."""
        data = {
            "type": 7
            if update_message
            else 4,  # 7 = UPDATE_MESSAGE, 4 = CHANNEL_MESSAGE_WITH_SOURCE
            "data": {"content": content},
        }
        if embeds:
            data["data"]["embeds"] = embeds
        # Always include components if provided (even empty list to remove buttons)
        if components is not None:
            data["data"]["components"] = components

        # Interaction responses use a different endpoint (no auth needed)
        url = f"{self.BASE_URL}/interactions/{interaction_id}/{interaction_token}/callback"
        try:
            await http.request(
                "POST", url, headers={"Content-Type": "application/json"}, json_data=data
            )
        except Exception as e:
            raise DiscordError(f"Discord interaction error: {str(e)}")

    # Trade recommendation

    async def send_recommendation(self, rec: Recommendation) -> str:
        """Send a trade recommendation with approve/reject buttons."""
        spread_name = (
            "Bull Put Spread" if rec.spread_type.value == "bull_put" else "Bear Call Spread"
        )
        direction = "Bullish" if rec.spread_type.value == "bull_put" else "Bearish"

        confidence_color = {
            "low": 0xFEE75C,  # Yellow
            "medium": 0xF97316,  # Orange
            "high": 0x57F287,  # Green
        }
        color = confidence_color.get(rec.confidence.value if rec.confidence else "low", 0x5865F2)

        embed = {
            "title": f"Trade Recommendation: {rec.underlying}",
            "description": rec.thesis if rec.thesis else "No analysis provided",
            "color": color,
            "fields": [
                {"name": "Strategy", "value": spread_name, "inline": True},
                {"name": "Direction", "value": direction, "inline": True},
                {"name": "Expiration", "value": rec.expiration, "inline": True},
                {"name": "Short Strike", "value": f"${rec.short_strike:.2f}", "inline": True},
                {"name": "Long Strike", "value": f"${rec.long_strike:.2f}", "inline": True},
                {"name": "Credit", "value": f"${rec.credit:.2f}", "inline": True},
                {"name": "Max Loss", "value": f"${rec.max_loss:.2f}", "inline": True},
                {"name": "Contracts", "value": str(rec.suggested_contracts or 1), "inline": True},
                {
                    "name": "Confidence",
                    "value": (rec.confidence.value.upper() if rec.confidence else "N/A"),
                    "inline": True,
                },
            ],
            "footer": {
                "text": f"Expires: {rec.expires_at.strftime('%H:%M:%S')} | ID: {rec.id[:8]}",
            },
        }

        if rec.iv_rank:
            embed["fields"].insert(
                6, {"name": "IV Rank", "value": f"{rec.iv_rank:.1f}%", "inline": True}
            )
        if rec.delta:
            embed["fields"].insert(
                7, {"name": "Delta", "value": f"{rec.delta:.3f}", "inline": True}
            )

        components = [
            {
                "type": 1,  # Action Row
                "components": [
                    {
                        "type": 2,  # Button
                        "style": 3,  # Success (green)
                        "label": "Approve",
                        "custom_id": f"approve_trade:{rec.id}",
                    },
                    {
                        "type": 2,  # Button
                        "style": 4,  # Danger (red)
                        "label": "Reject",
                        "custom_id": f"reject_trade:{rec.id}",
                    },
                ],
            }
        ]

        return await self.send_message(
            content=f"**New Trade Recommendation: {rec.underlying}**",
            embeds=[embed],
            components=components,
        )

    async def update_recommendation_approved(
        self,
        message_id: str,
        rec: Recommendation,
        order_id: str,
    ) -> None:
        """Update recommendation message to show approved status."""
        spread_name = rec.spread_type.value.replace("_", " ").title()

        embed = {
            "title": f"Trade Approved: {rec.underlying}",
            "color": 0x57F287,  # Green
            "fields": [
                {"name": "Strategy", "value": spread_name, "inline": True},
                {"name": "Expiration", "value": rec.expiration, "inline": True},
                {
                    "name": "Strikes",
                    "value": f"${rec.short_strike:.2f}/${rec.long_strike:.2f}",
                    "inline": True,
                },
                {"name": "Credit", "value": f"${rec.credit:.2f}", "inline": True},
                {"name": "Contracts", "value": str(rec.suggested_contracts or 1), "inline": True},
                {"name": "Order ID", "value": order_id, "inline": True},
            ],
        }

        await self.update_message(
            message_id,
            content=f"**Trade Approved: {rec.underlying}**",
            embeds=[embed],
            components=[],  # Remove buttons
        )

    async def update_recommendation_rejected(
        self,
        message_id: str,
        rec: Recommendation,
    ) -> None:
        """Update recommendation message to show rejected status."""
        embed = {
            "title": f"Trade Rejected: {rec.underlying}",
            "color": 0xED4245,  # Red
            "description": f"{rec.spread_type.value.replace('_', ' ').title()} | ${rec.short_strike:.2f}/${rec.long_strike:.2f} | {rec.expiration}",
        }

        await self.update_message(
            message_id,
            content=f"**Trade Rejected: {rec.underlying}**",
            embeds=[embed],
            components=[],
        )

    # Exit alerts

    async def send_exit_alert(
        self,
        trade: Trade,
        reason: str,
        current_value: float,
        unrealized_pnl: float,
    ) -> str:
        """Send an exit alert for a position."""
        pnl_color = 0x57F287 if unrealized_pnl > 0 else 0xED4245  # Green or Red

        embed = {
            "title": f"Exit Alert: {trade.underlying}",
            "color": pnl_color,
            "fields": [
                {"name": "Reason", "value": reason, "inline": False},
                {"name": "Entry Credit", "value": f"${trade.entry_credit:.2f}", "inline": True},
                {"name": "Current Value", "value": f"${current_value:.2f}", "inline": True},
                {"name": "Unrealized P/L", "value": f"${unrealized_pnl:.2f}", "inline": True},
                {"name": "Contracts", "value": str(trade.contracts), "inline": True},
            ],
        }

        components = [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 3,  # Success
                        "label": "Close Position",
                        "custom_id": f"close_position:{trade.id}",
                    },
                    {
                        "type": 2,
                        "style": 2,  # Secondary (gray)
                        "label": "Hold",
                        "custom_id": f"hold_position:{trade.id}",
                    },
                ],
            }
        ]

        return await self.send_message(
            content=f"**Exit Alert: {trade.underlying}** - {reason}",
            embeds=[embed],
            components=components,
        )

    # Daily summary

    async def send_daily_summary(
        self,
        performance: DailyPerformance,
        open_positions: int,
        trade_stats: dict,
    ) -> str:
        """Send end-of-day summary."""
        pnl_color = 0x57F287 if performance.realized_pnl >= 0 else 0xED4245

        embed = {
            "title": f"Daily Summary - {performance.date}",
            "color": pnl_color,
            "fields": [
                {
                    "name": "Starting Balance",
                    "value": f"${performance.starting_balance:,.2f}",
                    "inline": True,
                },
                {
                    "name": "Ending Balance",
                    "value": f"${performance.ending_balance:,.2f}",
                    "inline": True,
                },
                {
                    "name": "Realized P/L",
                    "value": f"${performance.realized_pnl:,.2f}",
                    "inline": True,
                },
                {"name": "Open Positions", "value": str(open_positions), "inline": True},
                {"name": "Trades Opened", "value": str(performance.trades_opened), "inline": True},
                {"name": "Trades Closed", "value": str(performance.trades_closed), "inline": True},
                {"name": "Wins", "value": str(performance.win_count), "inline": True},
                {"name": "Losses", "value": str(performance.loss_count), "inline": True},
                {"name": "\u200b", "value": "\u200b", "inline": True},  # Empty field for alignment
            ],
            "footer": {
                "text": f"Win Rate: {trade_stats['win_rate']:.1%} | Profit Factor: {trade_stats['profit_factor']:.2f} | Net P/L: ${trade_stats['net_pnl']:,.2f}",
            },
        }

        return await self.send_message(
            content=f"**Daily Summary: {performance.date}**",
            embeds=[embed],
        )

    # Circuit breaker

    async def send_circuit_breaker_alert(self, reason: str) -> str:
        """Send circuit breaker activation alert."""
        embed = {
            "title": "Circuit Breaker Activated",
            "color": 0xED4245,  # Red
            "description": f"**Reason:** {reason}\n\nTrading has been halted. Manual intervention required to resume.",
        }

        return await self.send_message(
            content="**CIRCUIT BREAKER ACTIVATED**",
            embeds=[embed],
        )

    # Order updates

    async def send_order_filled(self, trade: Trade, filled_price: float) -> str:
        """Send order fill confirmation."""
        embed = {
            "title": f"Order Filled: {trade.underlying}",
            "color": 0x57F287,  # Green
            "fields": [
                {
                    "name": "Strategy",
                    "value": trade.spread_type.value.replace("_", " ").title(),
                    "inline": True,
                },
                {"name": "Expiration", "value": trade.expiration, "inline": True},
                {
                    "name": "Strikes",
                    "value": f"${trade.short_strike:.2f}/${trade.long_strike:.2f}",
                    "inline": True,
                },
                {"name": "Credit", "value": f"${filled_price:.2f}", "inline": True},
                {"name": "Contracts", "value": str(trade.contracts), "inline": True},
                {
                    "name": "Total Credit",
                    "value": f"${filled_price * trade.contracts * 100:.2f}",
                    "inline": True,
                },
            ],
        }

        return await self.send_message(
            content=f"**Order Filled: {trade.underlying}**",
            embeds=[embed],
        )

    # Reconciliation alerts

    async def send_reconciliation_alert(
        self,
        discrepancies: list[dict],
        broker_positions: list[dict],
        db_positions: list[dict],
    ) -> str:
        """Send reconciliation mismatch alert.

        Args:
            discrepancies: List of discrepancy descriptions
            broker_positions: Positions from broker
            db_positions: Positions from database
        """
        embed = {
            "title": "Position Reconciliation Mismatch",
            "color": 0xED4245,  # Red
            "description": "Discrepancies detected between broker and database positions. Manual review required before next trading day.",
            "fields": [
                {
                    "name": "Discrepancy Count",
                    "value": str(len(discrepancies)),
                    "inline": True,
                },
                {
                    "name": "Broker Positions",
                    "value": str(len(broker_positions)),
                    "inline": True,
                },
                {
                    "name": "DB Positions",
                    "value": str(len(db_positions)),
                    "inline": True,
                },
            ],
        }

        # Add discrepancy details (up to 5)
        discrepancy_text = "\n".join(f"- {d['message']}" for d in discrepancies[:5])
        if len(discrepancies) > 5:
            discrepancy_text += f"\n... and {len(discrepancies) - 5} more"

        embed["fields"].append({
            "name": "Discrepancies",
            "value": discrepancy_text or "None",
            "inline": False,
        })

        components = [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 3,  # Success
                        "label": "Acknowledge",
                        "custom_id": "acknowledge_reconciliation",
                    },
                ],
            }
        ]

        return await self.send_message(
            content="**RECONCILIATION ALERT - MANUAL REVIEW REQUIRED**",
            embeds=[embed],
            components=components,
        )

    async def send_reconciliation_success(self, position_count: int) -> str:
        """Send confirmation that reconciliation passed."""
        embed = {
            "title": "Position Reconciliation Complete",
            "color": 0x57F287,  # Green
            "description": f"All {position_count} positions match between broker and database.",
        }

        return await self.send_message(
            content="**Reconciliation: All Clear**",
            embeds=[embed],
        )

    # Kill switch

    async def send_kill_switch_activated(self, reason: str, activated_by: str) -> str:
        """Send kill switch activation alert."""
        embed = {
            "title": "TRADING HALTED - Kill Switch Activated",
            "color": 0xED4245,  # Red
            "fields": [
                {"name": "Reason", "value": reason, "inline": False},
                {"name": "Activated By", "value": activated_by, "inline": True},
            ],
            "description": "All trading has been halted. Use /resume to restore trading.",
        }

        return await self.send_message(
            content="**KILL SWITCH ACTIVATED**",
            embeds=[embed],
        )

    async def send_kill_switch_deactivated(self, deactivated_by: str) -> str:
        """Send kill switch deactivation alert."""
        embed = {
            "title": "Trading Resumed",
            "color": 0x57F287,  # Green
            "fields": [
                {"name": "Resumed By", "value": deactivated_by, "inline": True},
            ],
            "description": "Kill switch has been deactivated. Trading will resume on next scan.",
        }

        return await self.send_message(
            content="**Trading Resumed**",
            embeds=[embed],
        )

    # AI Calibration alerts

    async def send_calibration_alert(self, calibration_data: dict) -> str:
        """Send AI confidence calibration alert when calibration gap exceeds threshold."""
        fields = []
        issues = []

        for confidence, data in calibration_data.items():
            if not data.get("is_calibrated", True):
                gap = data.get("calibration_gap", 0)
                actual = data.get("actual_win_rate", 0)
                expected = data.get("expected_win_rate", 0)
                issues.append(confidence)

                fields.append({
                    "name": f"{confidence.upper()} Confidence",
                    "value": f"Expected: {expected:.0%} | Actual: {actual:.0%} | Gap: {gap:+.0%}",
                    "inline": False,
                })

        if not issues:
            return ""  # No alert needed

        embed = {
            "title": "AI Confidence Calibration Alert",
            "color": 0xF97316,  # Orange
            "description": f"Calibration gap exceeds 10% for {len(issues)} confidence level(s). Consider adjusting AI prompts or reviewing trade selection criteria.",
            "fields": fields,
        }

        return await self.send_message(
            content="**AI Calibration Issue Detected**",
            embeds=[embed],
        )

    async def send_calibration_summary(self, calibration_data: dict, stats: dict) -> str:
        """Send weekly calibration summary."""
        fields = []

        for confidence in ["high", "medium", "low"]:
            if confidence in calibration_data:
                data = calibration_data[confidence]
                actual = data.get("actual_win_rate", 0)
                expected = data.get("expected_win_rate", 0)
                total = data.get("total_trades", 0)
                status = "OK" if data.get("is_calibrated", True) else "MISCALIBRATED"

                fields.append({
                    "name": f"{confidence.upper()} ({total} trades)",
                    "value": f"Win Rate: {actual:.0%} (expected {expected:.0%}) - {status}",
                    "inline": False,
                })

        embed = {
            "title": "AI Confidence Calibration Summary",
            "color": 0x5865F2,  # Blurple
            "fields": fields,
            "footer": {
                "text": f"Overall win rate: {stats.get('overall_win_rate', 0):.0%} | Total: {stats.get('total_trades', 0)} trades",
            },
        }

        return await self.send_message(
            content="**Weekly AI Calibration Report**",
            embeds=[embed],
        )
