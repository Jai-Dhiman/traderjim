from __future__ import annotations

"""Position sizing calculations per PRD risk rules."""

from dataclasses import dataclass

from core.types import CreditSpread, Position


@dataclass
class PositionSizeResult:
    """Result of position sizing calculation."""

    contracts: int
    risk_amount: float
    risk_percent: float
    reason: str | None = None  # If limited, why


@dataclass
class RiskLimits:
    """Risk limits from PRD."""

    # Position sizing
    max_risk_per_trade_pct: float = 0.02  # 2% max per trade
    max_single_position_pct: float = 0.05  # 5% max in one position
    max_portfolio_heat_pct: float = 0.10  # 10% total open risk

    # Volatility adjustments
    high_vix_threshold: float = 40.0
    high_vix_reduction: float = 0.75  # Reduce sizes by 75% when VIX > 40
    extreme_vix_threshold: float = 50.0  # Halt new trades


class PositionSizer:
    """Calculates appropriate position sizes based on risk limits."""

    def __init__(self, limits: RiskLimits | None = None):
        self.limits = limits or RiskLimits()

    def calculate_size(
        self,
        spread: CreditSpread,
        account_equity: float,
        current_positions: list[Position],
        current_vix: float | None = None,
    ) -> PositionSizeResult:
        """Calculate the number of contracts to trade.

        Args:
            spread: The credit spread being considered
            account_equity: Current account equity
            current_positions: List of open positions
            current_vix: Current VIX level (optional)

        Returns:
            PositionSizeResult with recommended contracts
        """
        # Check VIX constraints
        if current_vix is not None:
            if current_vix >= self.limits.extreme_vix_threshold:
                return PositionSizeResult(
                    contracts=0,
                    risk_amount=0,
                    risk_percent=0,
                    reason=f"VIX ({current_vix:.1f}) exceeds extreme threshold ({self.limits.extreme_vix_threshold})",
                )

        # Calculate max risk for this trade (2% rule)
        max_trade_risk = account_equity * self.limits.max_risk_per_trade_pct

        # Calculate max single position (5% rule)
        max_position_value = account_equity * self.limits.max_single_position_pct

        # Calculate current portfolio heat
        current_heat = sum(abs(p.current_value) for p in current_positions)
        current_heat_pct = current_heat / account_equity if account_equity > 0 else 0

        # Available heat capacity
        max_heat = account_equity * self.limits.max_portfolio_heat_pct
        available_heat = max(0, max_heat - current_heat)

        # Risk per contract (max loss)
        risk_per_contract = spread.max_loss  # Already multiplied by 100

        if risk_per_contract <= 0:
            return PositionSizeResult(
                contracts=0,
                risk_amount=0,
                risk_percent=0,
                reason="Invalid spread: no risk calculated",
            )

        # Calculate max contracts based on each limit
        max_by_trade_risk = int(max_trade_risk / risk_per_contract)
        max_by_position = int(max_position_value / risk_per_contract)
        max_by_heat = int(available_heat / risk_per_contract)

        # Find the binding constraint
        contracts = min(max_by_trade_risk, max_by_position, max_by_heat)
        contracts = max(0, contracts)

        reason = None
        if contracts == 0:
            if max_by_heat == 0:
                reason = f"Portfolio heat limit reached ({current_heat_pct:.1%} of {self.limits.max_portfolio_heat_pct:.0%})"
            elif max_by_trade_risk == 0:
                reason = "Trade risk exceeds 2% limit"
            elif max_by_position == 0:
                reason = "Position would exceed 5% limit"
        elif contracts < max_by_trade_risk:
            if contracts == max_by_heat:
                reason = f"Limited by portfolio heat ({current_heat_pct:.1%})"
            elif contracts == max_by_position:
                reason = "Limited by 5% single position rule"

        # Apply VIX adjustment
        if current_vix is not None and current_vix >= self.limits.high_vix_threshold:
            original = contracts
            contracts = max(1, int(contracts * (1 - self.limits.high_vix_reduction)))
            if contracts < original:
                reason = f"Reduced by {self.limits.high_vix_reduction:.0%} due to VIX ({current_vix:.1f})"

        # Ensure at least 1 if any contracts are allowed
        if contracts > 0:
            contracts = max(1, contracts)

        risk_amount = contracts * risk_per_contract
        risk_percent = risk_amount / account_equity if account_equity > 0 else 0

        return PositionSizeResult(
            contracts=contracts,
            risk_amount=risk_amount,
            risk_percent=risk_percent,
            reason=reason,
        )

    def calculate_portfolio_heat(
        self,
        positions: list[Position],
        account_equity: float,
    ) -> dict:
        """Calculate current portfolio heat metrics."""
        total_risk = sum(abs(p.current_value) for p in positions)
        heat_pct = total_risk / account_equity if account_equity > 0 else 0

        # Group by underlying
        by_underlying = {}
        for p in positions:
            if p.underlying not in by_underlying:
                by_underlying[p.underlying] = 0
            by_underlying[p.underlying] += abs(p.current_value)

        return {
            "total_risk": total_risk,
            "heat_percent": heat_pct,
            "max_heat_percent": self.limits.max_portfolio_heat_pct,
            "available_capacity": max(
                0, (self.limits.max_portfolio_heat_pct - heat_pct) * account_equity
            ),
            "by_underlying": by_underlying,
            "at_limit": heat_pct >= self.limits.max_portfolio_heat_pct,
        }
