from __future__ import annotations
import json
from datetime import datetime, timedelta
from typing import Any

from core.types import CircuitBreakerStatus


class KVClient:
    """Client for Cloudflare KV state storage."""

    # Key prefixes
    CIRCUIT_BREAKER_KEY = "circuit_breaker"
    DAILY_KEY_PREFIX = "daily:"
    RATE_LIMIT_PREFIX = "rate_limit:"

    def __init__(self, kv_binding: Any):
        self.kv = kv_binding

    async def get(self, key: str) -> str | None:
        """Get a value from KV."""
        return await self.kv.get(key)

    async def get_json(self, key: str) -> dict | None:
        """Get a JSON value from KV."""
        value = await self.kv.get(key)
        if value:
            return json.loads(value)
        return None

    async def put(
        self, key: str, value: str, expiration_ttl: int | None = None
    ) -> None:
        """Put a value into KV with optional TTL in seconds."""
        options = {}
        if expiration_ttl:
            options["expirationTtl"] = expiration_ttl
        await self.kv.put(key, value, options)

    async def put_json(
        self, key: str, value: dict, expiration_ttl: int | None = None
    ) -> None:
        """Put a JSON value into KV."""
        await self.put(key, json.dumps(value), expiration_ttl)

    async def delete(self, key: str) -> None:
        """Delete a key from KV."""
        await self.kv.delete(key)

    # Circuit Breaker

    async def get_circuit_breaker(self) -> CircuitBreakerStatus:
        """Get current circuit breaker status."""
        data = await self.get_json(self.CIRCUIT_BREAKER_KEY)
        if not data:
            return CircuitBreakerStatus.active()

        return CircuitBreakerStatus(
            halted=data.get("halted", False),
            reason=data.get("reason"),
            triggered_at=datetime.fromisoformat(data["triggered_at"])
            if data.get("triggered_at")
            else None,
        )

    async def trip_circuit_breaker(self, reason: str) -> None:
        """Trip the circuit breaker."""
        await self.put_json(
            self.CIRCUIT_BREAKER_KEY,
            {
                "halted": True,
                "reason": reason,
                "triggered_at": datetime.now().isoformat(),
            },
        )

    async def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker."""
        await self.put_json(self.CIRCUIT_BREAKER_KEY, {"halted": False})

    # Daily Limits

    def _daily_key(self, date: str | None = None) -> str:
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        return f"{self.DAILY_KEY_PREFIX}{date}"

    async def get_daily_stats(self, date: str | None = None) -> dict:
        """Get daily trading stats."""
        data = await self.get_json(self._daily_key(date))
        return data or {
            "trades_count": 0,
            "realized_pnl": 0.0,
            "losses_today": 0.0,
            "last_loss_time": None,
            "rapid_loss_amount": 0.0,
        }

    async def update_daily_stats(
        self,
        trades_delta: int = 0,
        pnl_delta: float = 0.0,
        date: str | None = None,
    ) -> dict:
        """Update daily trading stats."""
        stats = await self.get_daily_stats(date)
        stats["trades_count"] += trades_delta
        stats["realized_pnl"] += pnl_delta

        if pnl_delta < 0:
            stats["losses_today"] += abs(pnl_delta)
            now = datetime.now()
            last_loss = stats.get("last_loss_time")

            # Track rapid losses (within 5 minutes)
            if last_loss:
                last_loss_dt = datetime.fromisoformat(last_loss)
                if now - last_loss_dt < timedelta(minutes=5):
                    stats["rapid_loss_amount"] += abs(pnl_delta)
                else:
                    stats["rapid_loss_amount"] = abs(pnl_delta)
            else:
                stats["rapid_loss_amount"] = abs(pnl_delta)

            stats["last_loss_time"] = now.isoformat()

        # TTL of 7 days for daily stats
        await self.put_json(self._daily_key(date), stats, expiration_ttl=7 * 24 * 3600)
        return stats

    async def reset_daily_stats(self, date: str | None = None) -> None:
        """Reset daily stats (for new trading day)."""
        await self.delete(self._daily_key(date))

    # Rate Limiting

    async def check_rate_limit(
        self, service: str, max_requests: int, window_seconds: int = 3600
    ) -> bool:
        """Check if rate limit is exceeded. Returns True if OK to proceed."""
        key = f"{self.RATE_LIMIT_PREFIX}{service}"
        data = await self.get_json(key)

        now = datetime.now()
        if not data:
            await self.put_json(
                key,
                {"count": 1, "window_start": now.isoformat()},
                expiration_ttl=window_seconds,
            )
            return True

        window_start = datetime.fromisoformat(data["window_start"])
        if now - window_start > timedelta(seconds=window_seconds):
            # Window expired, reset
            await self.put_json(
                key,
                {"count": 1, "window_start": now.isoformat()},
                expiration_ttl=window_seconds,
            )
            return True

        if data["count"] >= max_requests:
            return False

        data["count"] += 1
        remaining_ttl = window_seconds - int((now - window_start).total_seconds())
        await self.put_json(key, data, expiration_ttl=max(remaining_ttl, 1))
        return True

    async def increment_error_count(self, window_seconds: int = 60) -> int:
        """Increment API error count and return current count."""
        key = f"{self.RATE_LIMIT_PREFIX}errors"
        data = await self.get_json(key)

        now = datetime.now()
        if not data:
            await self.put_json(
                key,
                {"count": 1, "window_start": now.isoformat()},
                expiration_ttl=window_seconds,
            )
            return 1

        window_start = datetime.fromisoformat(data["window_start"])
        if now - window_start > timedelta(seconds=window_seconds):
            await self.put_json(
                key,
                {"count": 1, "window_start": now.isoformat()},
                expiration_ttl=window_seconds,
            )
            return 1

        data["count"] += 1
        remaining_ttl = window_seconds - int((now - window_start).total_seconds())
        await self.put_json(key, data, expiration_ttl=max(remaining_ttl, 1))
        return data["count"]
