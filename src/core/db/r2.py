from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class R2Client:
    """Client for Cloudflare R2 object storage (archival)."""

    def __init__(self, r2_binding: Any):
        self.r2 = r2_binding

    async def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        """Store an object in R2."""
        await self.r2.put(key, data, {"httpMetadata": {"contentType": content_type}})

    async def put_json(self, key: str, data: dict) -> None:
        """Store JSON data in R2."""
        await self.put(key, json.dumps(data).encode(), "application/json")

    async def get(self, key: str) -> bytes | None:
        """Get an object from R2."""
        obj = await self.r2.get(key)
        if obj is None:
            return None
        return await obj.arrayBuffer()

    async def get_json(self, key: str) -> dict | None:
        """Get JSON data from R2."""
        data = await self.get(key)
        if data is None:
            return None
        return json.loads(data.decode())

    async def delete(self, key: str) -> None:
        """Delete an object from R2."""
        await self.r2.delete(key)

    async def list(self, prefix: str = "", limit: int = 1000) -> list[str]:
        """List objects with a prefix."""
        result = await self.r2.list({"prefix": prefix, "limit": limit})
        return [obj.key for obj in result.objects]

    # Archive helpers

    def _options_chain_key(self, symbol: str, date: str) -> str:
        return f"options_chains/{date}/{symbol}.json"

    async def archive_options_chain(
        self, symbol: str, chain_data: dict, date: str | None = None
    ) -> str:
        """Archive options chain data for a symbol."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        key = self._options_chain_key(symbol, date)
        await self.put_json(key, {
            "symbol": symbol,
            "date": date,
            "archived_at": datetime.now().isoformat(),
            "chain": chain_data,
        })
        return key

    async def get_archived_options_chain(
        self, symbol: str, date: str
    ) -> dict | None:
        """Get archived options chain data."""
        return await self.get_json(self._options_chain_key(symbol, date))

    def _daily_snapshot_key(self, date: str) -> str:
        return f"snapshots/{date}/daily.json"

    async def archive_daily_snapshot(
        self,
        date: str,
        positions: list[dict],
        performance: dict,
        account: dict,
    ) -> str:
        """Archive end-of-day snapshot."""
        key = self._daily_snapshot_key(date)
        await self.put_json(key, {
            "date": date,
            "archived_at": datetime.now().isoformat(),
            "positions": positions,
            "performance": performance,
            "account": account,
        })
        return key

    async def get_daily_snapshot(self, date: str) -> dict | None:
        """Get archived daily snapshot."""
        return await self.get_json(self._daily_snapshot_key(date))

    def _backup_key(self, backup_type: str, timestamp: str) -> str:
        return f"backups/{backup_type}/{timestamp}.json"

    async def create_backup(self, backup_type: str, data: dict) -> str:
        """Create a backup of data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key = self._backup_key(backup_type, timestamp)
        await self.put_json(key, {
            "backup_type": backup_type,
            "created_at": datetime.now().isoformat(),
            "data": data,
        })
        return key

    async def list_backups(self, backup_type: str) -> list[str]:
        """List available backups of a type."""
        return await self.list(f"backups/{backup_type}/")
