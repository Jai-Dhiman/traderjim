from __future__ import annotations
from datetime import datetime
from typing import Any
from uuid import uuid4

from core.types import (
    Confidence,
    DailyPerformance,
    PlaybookRule,
    Position,
    Recommendation,
    RecommendationStatus,
    SpreadType,
    Trade,
    TradeStatus,
)


def js_to_python(obj):
    """Convert JsProxy objects to Python equivalents."""
    from pyodide.ffi import JsProxy
    if isinstance(obj, JsProxy):
        # Check if it's array-like
        if hasattr(obj, 'to_py'):
            return obj.to_py()
        # Check if it's an object with properties
        try:
            return {k: js_to_python(getattr(obj, k)) for k in dir(obj) if not k.startswith('_')}
        except Exception:
            return str(obj)
    return obj


class D1Client:
    """Client for Cloudflare D1 SQLite database operations."""

    def __init__(self, db_binding: Any):
        self.db = db_binding

    async def execute(self, query: str, params: list | None = None) -> Any:
        """Execute a query and return results."""
        if params:
            result = await self.db.prepare(query).bind(*params).all()
        else:
            result = await self.db.prepare(query).all()

        # Convert the results to Python
        return js_to_python(result)

    async def run(self, query: str, params: list | None = None) -> Any:
        """Execute a query without returning results (INSERT, UPDATE, DELETE)."""
        if params:
            return await self.db.prepare(query).bind(*params).run()
        return await self.db.prepare(query).run()

    # Recommendations

    async def create_recommendation(
        self,
        underlying: str,
        spread_type: SpreadType,
        short_strike: float,
        long_strike: float,
        expiration: str,
        credit: float,
        max_loss: float,
        expires_at: datetime,
        iv_rank: float | None = None,
        delta: float | None = None,
        theta: float | None = None,
        thesis: str | None = None,
        confidence: Confidence | None = None,
        suggested_contracts: int | None = None,
        analysis_price: float | None = None,
    ) -> str:
        """Create a new recommendation and return its ID."""
        rec_id = str(uuid4())
        await self.run(
            """
            INSERT INTO recommendations (
                id, expires_at, underlying, spread_type, short_strike, long_strike,
                expiration, credit, max_loss, iv_rank, delta, theta, thesis,
                confidence, suggested_contracts, analysis_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                rec_id,
                expires_at.isoformat(),
                underlying,
                spread_type.value,
                short_strike,
                long_strike,
                expiration,
                credit,
                max_loss,
                iv_rank,
                delta,
                theta,
                thesis,
                confidence.value if confidence else None,
                suggested_contracts,
                analysis_price,
            ],
        )
        return rec_id

    async def get_recommendation(self, rec_id: str) -> Recommendation | None:
        """Get a recommendation by ID."""
        result = await self.execute(
            "SELECT * FROM recommendations WHERE id = ?", [rec_id]
        )
        if not result["results"]:
            return None
        return self._row_to_recommendation(result["results"][0])

    async def get_pending_recommendations(self) -> list[Recommendation]:
        """Get all pending recommendations."""
        result = await self.execute(
            "SELECT * FROM recommendations WHERE status = 'pending' ORDER BY created_at DESC"
        )
        return [self._row_to_recommendation(row) for row in result["results"]]

    async def update_recommendation_status(
        self, rec_id: str, status: RecommendationStatus
    ) -> None:
        """Update recommendation status."""
        await self.run(
            "UPDATE recommendations SET status = ? WHERE id = ?",
            [status.value, rec_id],
        )

    async def set_recommendation_discord_message_id(self, rec_id: str, message_id: str) -> None:
        """Set the Discord message ID for a recommendation."""
        await self.run(
            "UPDATE recommendations SET discord_message_id = ? WHERE id = ?",
            [message_id, rec_id],
        )

    def _row_to_recommendation(self, row: dict) -> Recommendation:
        return Recommendation(
            id=row["id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            expires_at=datetime.fromisoformat(row["expires_at"]),
            status=RecommendationStatus(row["status"]),
            underlying=row["underlying"],
            spread_type=SpreadType(row["spread_type"]),
            short_strike=row["short_strike"],
            long_strike=row["long_strike"],
            expiration=row["expiration"],
            credit=row["credit"],
            max_loss=row["max_loss"],
            iv_rank=row["iv_rank"],
            delta=row["delta"],
            theta=row["theta"],
            thesis=row["thesis"],
            confidence=Confidence(row["confidence"]) if row["confidence"] else None,
            suggested_contracts=row["suggested_contracts"],
            analysis_price=row["analysis_price"],
            discord_message_id=row["discord_message_id"],
        )

    # Trades

    async def create_trade(
        self,
        recommendation_id: str | None,
        underlying: str,
        spread_type: SpreadType,
        short_strike: float,
        long_strike: float,
        expiration: str,
        entry_credit: float,
        contracts: int,
        broker_order_id: str | None = None,
    ) -> str:
        """Create a new trade and return its ID."""
        trade_id = str(uuid4())
        await self.run(
            """
            INSERT INTO trades (
                id, recommendation_id, opened_at, status, underlying, spread_type,
                short_strike, long_strike, expiration, entry_credit, contracts, broker_order_id
            ) VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                trade_id,
                recommendation_id,
                datetime.now().isoformat(),
                underlying,
                spread_type.value,
                short_strike,
                long_strike,
                expiration,
                entry_credit,
                contracts,
                broker_order_id,
            ],
        )
        return trade_id

    async def get_trade(self, trade_id: str) -> Trade | None:
        """Get a trade by ID."""
        result = await self.execute("SELECT * FROM trades WHERE id = ?", [trade_id])
        if not result["results"]:
            return None
        return self._row_to_trade(result["results"][0])

    async def get_open_trades(self) -> list[Trade]:
        """Get all open trades."""
        result = await self.execute(
            "SELECT * FROM trades WHERE status = 'open' ORDER BY opened_at DESC"
        )
        return [self._row_to_trade(row) for row in result["results"]]

    async def close_trade(
        self,
        trade_id: str,
        exit_debit: float,
        reflection: str | None = None,
        lesson: str | None = None,
    ) -> None:
        """Close a trade with exit details."""
        trade = await self.get_trade(trade_id)
        if not trade:
            raise ValueError(f"Trade {trade_id} not found")

        profit_loss = (trade.entry_credit - exit_debit) * trade.contracts * 100
        await self.run(
            """
            UPDATE trades
            SET status = 'closed', closed_at = ?, exit_debit = ?, profit_loss = ?,
                reflection = ?, lesson = ?
            WHERE id = ?
            """,
            [
                datetime.now().isoformat(),
                exit_debit,
                profit_loss,
                reflection,
                lesson,
                trade_id,
            ],
        )

    def _row_to_trade(self, row: dict) -> Trade:
        return Trade(
            id=row["id"],
            recommendation_id=row["recommendation_id"],
            opened_at=datetime.fromisoformat(row["opened_at"]) if row["opened_at"] else None,
            closed_at=datetime.fromisoformat(row["closed_at"]) if row["closed_at"] else None,
            status=TradeStatus(row["status"]),
            underlying=row["underlying"],
            spread_type=SpreadType(row["spread_type"]),
            short_strike=row["short_strike"],
            long_strike=row["long_strike"],
            expiration=row["expiration"],
            entry_credit=row["entry_credit"],
            exit_debit=row["exit_debit"],
            profit_loss=row["profit_loss"],
            contracts=row["contracts"],
            broker_order_id=row["broker_order_id"],
            reflection=row["reflection"],
            lesson=row["lesson"],
        )

    # Positions

    async def upsert_position(
        self,
        trade_id: str,
        underlying: str,
        short_strike: float,
        long_strike: float,
        expiration: str,
        contracts: int,
        current_value: float,
        unrealized_pnl: float,
    ) -> str:
        """Create or update a position snapshot."""
        existing = await self.execute(
            "SELECT id FROM positions WHERE trade_id = ?", [trade_id]
        )
        if existing.results:
            pos_id = existing.results[0]["id"]
            await self.run(
                """
                UPDATE positions
                SET current_value = ?, unrealized_pnl = ?, updated_at = ?
                WHERE id = ?
                """,
                [current_value, unrealized_pnl, datetime.now().isoformat(), pos_id],
            )
            return pos_id

        pos_id = str(uuid4())
        await self.run(
            """
            INSERT INTO positions (
                id, trade_id, underlying, short_strike, long_strike, expiration,
                contracts, current_value, unrealized_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                pos_id,
                trade_id,
                underlying,
                short_strike,
                long_strike,
                expiration,
                contracts,
                current_value,
                unrealized_pnl,
            ],
        )
        return pos_id

    async def delete_position(self, trade_id: str) -> None:
        """Delete position for a closed trade."""
        await self.run("DELETE FROM positions WHERE trade_id = ?", [trade_id])

    async def get_all_positions(self) -> list[Position]:
        """Get all current positions."""
        result = await self.execute("SELECT * FROM positions ORDER BY updated_at DESC")
        return [self._row_to_position(row) for row in result["results"]]

    def _row_to_position(self, row: dict) -> Position:
        return Position(
            id=row["id"],
            trade_id=row["trade_id"],
            underlying=row["underlying"],
            short_strike=row["short_strike"],
            long_strike=row["long_strike"],
            expiration=row["expiration"],
            contracts=row["contracts"],
            current_value=row["current_value"],
            unrealized_pnl=row["unrealized_pnl"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    # Daily Performance

    async def get_or_create_daily_performance(
        self, date: str, starting_balance: float
    ) -> DailyPerformance:
        """Get or create daily performance record."""
        result = await self.execute(
            "SELECT * FROM daily_performance WHERE date = ?", [date]
        )
        if result["results"]:
            return self._row_to_daily_performance(result["results"][0])

        await self.run(
            """
            INSERT INTO daily_performance (date, starting_balance, ending_balance, realized_pnl)
            VALUES (?, ?, ?, 0)
            """,
            [date, starting_balance, starting_balance],
        )
        return DailyPerformance(
            date=date,
            starting_balance=starting_balance,
            ending_balance=starting_balance,
            realized_pnl=0,
        )

    async def update_daily_performance(
        self,
        date: str,
        ending_balance: float | None = None,
        realized_pnl_delta: float = 0,
        trades_opened_delta: int = 0,
        trades_closed_delta: int = 0,
        win_delta: int = 0,
        loss_delta: int = 0,
    ) -> None:
        """Update daily performance metrics."""
        updates = []
        params = []

        if ending_balance is not None:
            updates.append("ending_balance = ?")
            params.append(ending_balance)
        if realized_pnl_delta:
            updates.append("realized_pnl = realized_pnl + ?")
            params.append(realized_pnl_delta)
        if trades_opened_delta:
            updates.append("trades_opened = trades_opened + ?")
            params.append(trades_opened_delta)
        if trades_closed_delta:
            updates.append("trades_closed = trades_closed + ?")
            params.append(trades_closed_delta)
        if win_delta:
            updates.append("win_count = win_count + ?")
            params.append(win_delta)
        if loss_delta:
            updates.append("loss_count = loss_count + ?")
            params.append(loss_delta)

        if updates:
            params.append(date)
            await self.run(
                f"UPDATE daily_performance SET {', '.join(updates)} WHERE date = ?",
                params,
            )

    def _row_to_daily_performance(self, row: dict) -> DailyPerformance:
        return DailyPerformance(
            date=row["date"],
            starting_balance=row["starting_balance"],
            ending_balance=row["ending_balance"],
            realized_pnl=row["realized_pnl"],
            trades_opened=row["trades_opened"],
            trades_closed=row["trades_closed"],
            win_count=row["win_count"],
            loss_count=row["loss_count"],
        )

    # Playbook

    async def get_playbook_rules(self) -> list[PlaybookRule]:
        """Get all playbook rules."""
        result = await self.execute("SELECT * FROM playbook ORDER BY created_at")
        return [self._row_to_playbook_rule(row) for row in result["results"]]

    async def add_playbook_rule(
        self, rule: str, source: str = "learned", supporting_trade_ids: list[str] | None = None
    ) -> str:
        """Add a new playbook rule."""
        import json

        rule_id = str(uuid4())
        await self.run(
            "INSERT INTO playbook (id, rule, source, supporting_trade_ids) VALUES (?, ?, ?, ?)",
            [rule_id, rule, source, json.dumps(supporting_trade_ids or [])],
        )
        return rule_id

    def _row_to_playbook_rule(self, row: dict) -> PlaybookRule:
        import json

        return PlaybookRule(
            id=row["id"],
            rule=row["rule"],
            source=row["source"],
            supporting_trade_ids=json.loads(row["supporting_trade_ids"] or "[]"),
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )

    # Stats

    async def get_trade_stats(self) -> dict:
        """Get aggregate trade statistics."""
        result = await self.execute(
            """
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) as closed_trades,
                SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN profit_loss < 0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN profit_loss > 0 THEN profit_loss ELSE 0 END) as total_profit,
                SUM(CASE WHEN profit_loss < 0 THEN ABS(profit_loss) ELSE 0 END) as total_loss,
                SUM(profit_loss) as net_pnl
            FROM trades
            """
        )
        row = result["results"][0] if result["results"] else {}
        wins = row.get("wins") or 0
        losses = row.get("losses") or 0
        total_profit = row.get("total_profit") or 0
        total_loss = row.get("total_loss") or 1  # Avoid division by zero

        return {
            "total_trades": row.get("total_trades") or 0,
            "closed_trades": row.get("closed_trades") or 0,
            "wins": wins,
            "losses": losses,
            "win_rate": wins / (wins + losses) if (wins + losses) > 0 else 0,
            "profit_factor": total_profit / total_loss if total_loss > 0 else 0,
            "net_pnl": row.get("net_pnl") or 0,
        }
