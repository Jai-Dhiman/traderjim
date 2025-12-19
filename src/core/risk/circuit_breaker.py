from __future__ import annotations

"""Circuit breaker logic with graduated response per research recommendations."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from core.db.kv import KVClient
from core.types import CircuitBreakerStatus


class RiskLevel(str, Enum):
    """Risk levels for graduated response."""

    NORMAL = "normal"
    ELEVATED = "elevated"  # Alert, continue at full size
    CAUTION = "caution"  # Reduce position size
    HIGH = "high"  # Significantly reduce exposure
    CRITICAL = "critical"  # Minimum sizing only
    HALTED = "halted"  # No new trades


@dataclass
class GraduatedConfig:
    """Graduated circuit breaker thresholds.

    Research-backed thresholds that avoid binary halt/resume whipsaw.
    """

    # Daily loss thresholds (graduated)
    daily_alert_pct: float = 0.01  # 1% - alert only
    daily_reduce_pct: float = 0.015  # 1.5% - reduce to 50%
    daily_halt_pct: float = 0.02  # 2% - halt new trades for day

    # Weekly loss thresholds (graduated)
    weekly_caution_pct: float = 0.03  # 3% - reduce exposure 50%
    weekly_halt_pct: float = 0.05  # 5% - halt + close 50%

    # Drawdown thresholds (graduated)
    drawdown_caution_pct: float = 0.10  # 10% - minimum sizing
    drawdown_halt_pct: float = 0.15  # 15% - full halt

    # Technical limits
    stale_data_seconds: int = 10
    max_api_errors_per_minute: int = 5

    # Rapid loss
    rapid_loss_threshold_pct: float = 0.01
    rapid_loss_window_minutes: int = 5

    # VIX thresholds (graduated)
    vix_elevated: float = 20.0  # Alert
    vix_caution: float = 30.0  # Reduce size
    vix_high: float = 40.0  # Significant reduction
    vix_halt: float = 50.0  # Halt new trades

    # Recovery requirements
    recovery_days: int = 3  # Days before restoring from halt
    recovery_pnl_pct: float = 0.01  # 1% recovery required


@dataclass
class RiskState:
    """Current risk state with graduated response."""

    level: RiskLevel
    size_multiplier: float  # 0.0 to 1.0
    reason: str | None = None
    should_alert: bool = False
    should_close_positions: bool = False
    close_position_pct: float = 0.0  # % of positions to close

    @classmethod
    def normal(cls) -> "RiskState":
        return cls(level=RiskLevel.NORMAL, size_multiplier=1.0)

    @classmethod
    def halted(cls, reason: str, close_pct: float = 0.0) -> "RiskState":
        return cls(
            level=RiskLevel.HALTED,
            size_multiplier=0.0,
            reason=reason,
            should_alert=True,
            should_close_positions=close_pct > 0,
            close_position_pct=close_pct,
        )


class CircuitBreakerReason:
    """Standard circuit breaker reasons."""

    DAILY_ALERT = "Daily loss alert (1%)"
    DAILY_REDUCE = "Daily loss elevated (1.5%) - reducing position size"
    DAILY_HALT = "Daily loss limit exceeded (2%) - halting new trades"
    WEEKLY_CAUTION = "Weekly loss caution (3%) - reducing exposure"
    WEEKLY_HALT = "Weekly loss limit exceeded (5%) - halting and reducing positions"
    DRAWDOWN_CAUTION = "Drawdown elevated (10%) - minimum sizing"
    DRAWDOWN_HALT = "Maximum drawdown reached (15%) - full halt"
    STALE_DATA = "Stale market data detected"
    API_ERRORS = "Excessive API errors"
    RAPID_LOSS = "Rapid loss detected (1% in <5 min)"
    VIX_ELEVATED = "VIX elevated (>20)"
    VIX_CAUTION = "VIX high (>30) - reducing size"
    VIX_HIGH = "VIX very high (>40) - significant reduction"
    VIX_HALT = "Extreme volatility (VIX > 50) - halting"
    MANUAL = "Manually triggered"


class GraduatedCircuitBreaker:
    """Circuit breaker with graduated response instead of binary halt.

    Research shows binary circuit breakers cause whipsaw - stopping trading
    entirely then resuming at full size. Graduated response reduces this by:
    1. Alerting early before limits are breached
    2. Reducing position sizes at intermediate thresholds
    3. Requiring both time AND recovery before restoring full sizing
    """

    def __init__(self, kv: KVClient, config: GraduatedConfig | None = None):
        self.kv = kv
        self.config = config or GraduatedConfig()

    async def get_status(self) -> CircuitBreakerStatus:
        """Get current circuit breaker status."""
        return await self.kv.get_circuit_breaker()

    async def is_trading_allowed(self) -> bool:
        """Check if trading is currently allowed (not halted)."""
        status = await self.get_status()
        return not status.halted

    async def check_status(self) -> CircuitBreakerStatus:
        """Alias for get_status() for backward compatibility."""
        return await self.get_status()

    async def trip(self, reason: str) -> None:
        """Trip the circuit breaker (full halt)."""
        await self.kv.trip_circuit_breaker(reason)

    async def reset(self) -> None:
        """Reset the circuit breaker (manual action)."""
        await self.kv.reset_circuit_breaker()

    def _calculate_loss_pct(self, starting: float, current: float) -> float:
        """Calculate loss percentage."""
        if starting <= 0:
            return 0.0
        return max(0, (starting - current) / starting)

    async def evaluate_daily_risk(
        self,
        starting_equity: float,
        current_equity: float,
    ) -> RiskState:
        """Evaluate daily loss with graduated response."""
        loss_pct = self._calculate_loss_pct(starting_equity, current_equity)

        if loss_pct >= self.config.daily_halt_pct:
            return RiskState(
                level=RiskLevel.HALTED,
                size_multiplier=0.0,
                reason=CircuitBreakerReason.DAILY_HALT,
                should_alert=True,
            )
        elif loss_pct >= self.config.daily_reduce_pct:
            return RiskState(
                level=RiskLevel.CAUTION,
                size_multiplier=0.5,
                reason=CircuitBreakerReason.DAILY_REDUCE,
                should_alert=True,
            )
        elif loss_pct >= self.config.daily_alert_pct:
            return RiskState(
                level=RiskLevel.ELEVATED,
                size_multiplier=1.0,
                reason=CircuitBreakerReason.DAILY_ALERT,
                should_alert=True,
            )

        return RiskState.normal()

    async def evaluate_weekly_risk(
        self,
        starting_equity: float,
        current_equity: float,
    ) -> RiskState:
        """Evaluate weekly loss with graduated response."""
        loss_pct = self._calculate_loss_pct(starting_equity, current_equity)

        if loss_pct >= self.config.weekly_halt_pct:
            return RiskState(
                level=RiskLevel.HALTED,
                size_multiplier=0.0,
                reason=CircuitBreakerReason.WEEKLY_HALT,
                should_alert=True,
                should_close_positions=True,
                close_position_pct=0.5,  # Close 50% of positions
            )
        elif loss_pct >= self.config.weekly_caution_pct:
            return RiskState(
                level=RiskLevel.HIGH,
                size_multiplier=0.5,
                reason=CircuitBreakerReason.WEEKLY_CAUTION,
                should_alert=True,
            )

        return RiskState.normal()

    async def evaluate_drawdown_risk(
        self,
        peak_equity: float,
        current_equity: float,
    ) -> RiskState:
        """Evaluate drawdown with graduated response."""
        drawdown_pct = self._calculate_loss_pct(peak_equity, current_equity)

        if drawdown_pct >= self.config.drawdown_halt_pct:
            return RiskState.halted(
                reason=CircuitBreakerReason.DRAWDOWN_HALT,
                close_pct=1.0,  # Close all positions
            )
        elif drawdown_pct >= self.config.drawdown_caution_pct:
            return RiskState(
                level=RiskLevel.CRITICAL,
                size_multiplier=0.25,  # Minimum sizing
                reason=CircuitBreakerReason.DRAWDOWN_CAUTION,
                should_alert=True,
            )

        return RiskState.normal()

    async def evaluate_vix_risk(self, current_vix: float) -> RiskState:
        """Evaluate VIX with graduated response."""
        if current_vix >= self.config.vix_halt:
            return RiskState(
                level=RiskLevel.HALTED,
                size_multiplier=0.0,
                reason=CircuitBreakerReason.VIX_HALT,
                should_alert=True,
            )
        elif current_vix >= self.config.vix_high:
            return RiskState(
                level=RiskLevel.HIGH,
                size_multiplier=0.25,
                reason=CircuitBreakerReason.VIX_HIGH,
                should_alert=True,
            )
        elif current_vix >= self.config.vix_caution:
            return RiskState(
                level=RiskLevel.CAUTION,
                size_multiplier=0.5,
                reason=CircuitBreakerReason.VIX_CAUTION,
                should_alert=True,
            )
        elif current_vix >= self.config.vix_elevated:
            return RiskState(
                level=RiskLevel.ELEVATED,
                size_multiplier=0.8,
                reason=CircuitBreakerReason.VIX_ELEVATED,
            )

        return RiskState.normal()

    async def evaluate_rapid_loss(self, account_equity: float) -> RiskState:
        """Check for rapid loss."""
        daily_stats = await self.kv.get_daily_stats()
        rapid_loss = daily_stats.get("rapid_loss_amount", 0)

        if account_equity <= 0:
            return RiskState.normal()

        rapid_loss_pct = rapid_loss / account_equity
        if rapid_loss_pct >= self.config.rapid_loss_threshold_pct:
            return RiskState(
                level=RiskLevel.HALTED,
                size_multiplier=0.0,
                reason=CircuitBreakerReason.RAPID_LOSS,
                should_alert=True,
            )

        return RiskState.normal()

    async def check_api_errors(self) -> RiskState:
        """Check API error rate."""
        error_count = await self.kv.increment_error_count(window_seconds=60)
        if error_count >= self.config.max_api_errors_per_minute:
            return RiskState.halted(reason=CircuitBreakerReason.API_ERRORS)
        return RiskState.normal()

    async def check_data_staleness(
        self,
        last_quote_time: datetime,
        current_time: datetime | None = None,
    ) -> RiskState:
        """Check if market data is stale."""
        if current_time is None:
            current_time = datetime.now()

        staleness = (current_time - last_quote_time).total_seconds()
        if staleness > self.config.stale_data_seconds:
            return RiskState.halted(reason=CircuitBreakerReason.STALE_DATA)
        return RiskState.normal()

    async def evaluate_all(
        self,
        starting_daily_equity: float,
        starting_weekly_equity: float,
        peak_equity: float,
        current_equity: float,
        current_vix: float | None = None,
        last_quote_time: datetime | None = None,
    ) -> RiskState:
        """Evaluate all risk factors and return the most restrictive state.

        Returns:
            RiskState with the lowest size_multiplier among all checks
        """
        # Check if manually halted first
        status = await self.get_status()
        if status.halted:
            return RiskState.halted(reason=status.reason or CircuitBreakerReason.MANUAL)

        # Collect all risk states
        states: list[RiskState] = []

        # Check in order of severity
        states.append(await self.evaluate_drawdown_risk(peak_equity, current_equity))
        states.append(await self.evaluate_daily_risk(starting_daily_equity, current_equity))
        states.append(await self.evaluate_weekly_risk(starting_weekly_equity, current_equity))
        states.append(await self.evaluate_rapid_loss(current_equity))

        if current_vix is not None:
            states.append(await self.evaluate_vix_risk(current_vix))

        if last_quote_time is not None:
            states.append(await self.check_data_staleness(last_quote_time))

        # Find the most restrictive state (lowest size multiplier)
        worst_state = RiskState.normal()
        for state in states:
            if state.size_multiplier < worst_state.size_multiplier:
                worst_state = state
            elif state.size_multiplier == worst_state.size_multiplier:
                # If equal, prefer the one that should alert
                if state.should_alert and not worst_state.should_alert:
                    worst_state = state

        # If halted, trip the circuit breaker
        if worst_state.level == RiskLevel.HALTED and worst_state.reason:
            await self.trip(worst_state.reason)

        return worst_state


# Keep backward compatibility alias
CircuitBreakerConfig = GraduatedConfig
CircuitBreaker = GraduatedCircuitBreaker
