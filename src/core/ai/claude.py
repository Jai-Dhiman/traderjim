from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from core import http
from core.ai.prompts import (
    MARKET_CONTEXT_SYSTEM,
    MARKET_CONTEXT_USER,
    PLAYBOOK_UPDATE_SYSTEM,
    PLAYBOOK_UPDATE_USER,
    REFLECTION_SYSTEM,
    REFLECTION_USER,
    TRADE_ANALYSIS_SYSTEM,
    TRADE_ANALYSIS_USER,
)
from core.types import Confidence, CreditSpread, PlaybookRule, Trade


class ClaudeError(Exception):
    """Claude API error."""

    pass


@dataclass
class TradeAnalysis:
    """Result of AI trade analysis."""

    thesis: str
    risks: list[str]
    confidence: Confidence
    confidence_reason: str


@dataclass
class TradeReflection:
    """Result of AI trade reflection."""

    reflection: str
    lesson: str


@dataclass
class PlaybookUpdate:
    """Suggested playbook updates."""

    new_rules: list[dict]


class ClaudeClient:
    """Client for Claude AI analysis."""

    BASE_URL = "https://api.anthropic.com/v1"
    MODEL = "claude-sonnet-4-20250514"
    MAX_TOKENS = 1024

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

    async def _request(self, messages: list[dict], system: str) -> str:
        """Make a request to Claude API."""
        try:
            data = await http.request(
                "POST",
                f"{self.BASE_URL}/messages",
                headers=self._headers,
                json_data={
                    "model": self.MODEL,
                    "max_tokens": self.MAX_TOKENS,
                    "system": system,
                    "messages": messages,
                },
            )

            content = data.get("content", [])
            if not content:
                raise ClaudeError("Empty response from Claude")

            return content[0].get("text", "")
        except Exception as e:
            raise ClaudeError(f"Claude API error: {str(e)}")

    def _parse_json_response(self, text: str) -> dict:
        """Parse JSON from Claude response, handling markdown code blocks."""
        # Strip markdown code blocks if present
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first line (```json) and last line (```)
            text = "\n".join(lines[1:-1])

        return json.loads(text)

    async def analyze_trade(
        self,
        spread: CreditSpread,
        underlying_price: float,
        iv_rank: float,
        current_iv: float,
        playbook_rules: list[PlaybookRule],
    ) -> TradeAnalysis:
        """Analyze a potential credit spread trade."""
        # Calculate DTE
        from datetime import datetime

        exp_date = datetime.strptime(spread.expiration, "%Y-%m-%d")
        dte = (exp_date - datetime.now()).days

        # Format playbook rules
        rules_text = "\n".join(f"- {r.rule}" for r in playbook_rules[:5])

        prompt = TRADE_ANALYSIS_USER.format(
            underlying=spread.underlying,
            underlying_price=underlying_price,
            spread_type=spread.spread_type.value.replace("_", " ").title(),
            short_strike=spread.short_strike,
            short_delta=spread.short_contract.delta or 0,
            long_strike=spread.long_strike,
            long_delta=spread.long_contract.delta or 0,
            expiration=spread.expiration,
            dte=dte,
            credit=spread.credit,
            max_loss=spread.max_loss / 100,  # Per spread, not per contract
            risk_reward=spread.max_loss / spread.max_profit if spread.max_profit > 0 else 0,
            iv_rank=iv_rank,
            current_iv=current_iv,
            playbook_rules=rules_text or "No rules loaded",
        )

        response = await self._request(
            [{"role": "user", "content": prompt}],
            TRADE_ANALYSIS_SYSTEM,
        )

        data = self._parse_json_response(response)

        return TradeAnalysis(
            thesis=data["thesis"],
            risks=data["risks"],
            confidence=Confidence(data["confidence"]),
            confidence_reason=data["confidence_reason"],
        )

    async def generate_reflection(
        self, trade: Trade, original_thesis: str | None = None
    ) -> TradeReflection:
        """Generate reflection on a closed trade."""
        if trade.profit_loss is None or trade.exit_debit is None:
            raise ValueError("Trade must be closed to generate reflection")

        pnl_percent = (trade.profit_loss / (trade.entry_credit * trade.contracts * 100)) * 100
        outcome = "WIN" if trade.profit_loss > 0 else "LOSS"

        prompt = REFLECTION_USER.format(
            underlying=trade.underlying,
            spread_type=trade.spread_type.value.replace("_", " ").title(),
            opened_at=trade.opened_at.strftime("%Y-%m-%d") if trade.opened_at else "N/A",
            closed_at=trade.closed_at.strftime("%Y-%m-%d") if trade.closed_at else "N/A",
            short_strike=trade.short_strike,
            long_strike=trade.long_strike,
            expiration=trade.expiration,
            entry_credit=trade.entry_credit,
            exit_debit=trade.exit_debit,
            profit_loss=trade.profit_loss,
            pnl_percent=pnl_percent,
            outcome=outcome,
            original_thesis=original_thesis or "Not recorded",
        )

        response = await self._request(
            [{"role": "user", "content": prompt}],
            REFLECTION_SYSTEM,
        )

        data = self._parse_json_response(response)

        return TradeReflection(
            reflection=data["reflection"],
            lesson=data["lesson"],
        )

    async def suggest_playbook_updates(
        self,
        recent_trades: list[Trade],
        current_rules: list[PlaybookRule],
    ) -> PlaybookUpdate:
        """Suggest updates to the trading playbook based on recent trades."""
        # Format reflections
        reflections_text = ""
        for trade in recent_trades:
            if trade.reflection and trade.lesson:
                reflections_text += f"""
Trade ID: {trade.id[:8]}
Underlying: {trade.underlying}
P/L: ${trade.profit_loss:.2f}
Reflection: {trade.reflection}
Lesson: {trade.lesson}
---
"""

        if not reflections_text:
            return PlaybookUpdate(new_rules=[])

        # Format current rules
        rules_text = "\n".join(f"- {r.rule}" for r in current_rules)

        prompt = PLAYBOOK_UPDATE_USER.format(
            reflections=reflections_text,
            current_rules=rules_text,
        )

        response = await self._request(
            [{"role": "user", "content": prompt}],
            PLAYBOOK_UPDATE_SYSTEM,
        )

        data = self._parse_json_response(response)

        return PlaybookUpdate(new_rules=data.get("new_rules", []))

    async def get_market_context(
        self,
        underlying: str,
        price: float,
        vix: float,
        iv_rank: float,
    ) -> str:
        """Get brief market context for trading decisions."""
        prompt = MARKET_CONTEXT_USER.format(
            underlying=underlying,
            price=price,
            vix=vix,
            iv_rank=iv_rank,
        )

        return await self._request(
            [{"role": "user", "content": prompt}],
            MARKET_CONTEXT_SYSTEM,
        )
