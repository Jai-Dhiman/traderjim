from __future__ import annotations
"""Circuit breaker logic per PRD risk rules."""

from dataclasses import dataclass
from datetime import datetime

from core.db.kv import KVClient
from core.types import CircuitBreakerStatus


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker thresholds from PRD."""
    # Drawdown limits
    daily_loss_limit_pct: float = 0.02  # 2% daily - halt new trades
    weekly_loss_limit_pct: float = 0.05  # 5% weekly - require manual review
    max_drawdown_pct: float = 0.15  # 15% - close all and disable

    # Technical limits
    stale_data_seconds: int = 10  # Halt on no quote update > 10s
    max_api_errors_per_minute: int = 5  # Halt on 5+ errors/min

    # Rapid loss
    rapid_loss_threshold_pct: float = 0.01  # 1% loss in <5 min
    rapid_loss_window_minutes: int = 5

    # Volatility
    high_vix_threshold: float = 40.0  # Reduce sizes
    extreme_vix_threshold: float = 50.0  # Halt new trades


class CircuitBreakerReason:
    """Standard circuit breaker trip reasons."""
    DAILY_LOSS = "Daily loss limit exceeded (2%)"
    WEEKLY_LOSS = "Weekly loss limit exceeded (5%)"
    MAX_DRAWDOWN = "Maximum drawdown reached (15%)"
    STALE_DATA = "Stale market data detected"
    API_ERRORS = "Excessive API errors"
    RAPID_LOSS = "Rapid loss detected (1% in <5 min)"
    EXTREME_VIX = "Extreme volatility (VIX > 50)"
    MANUAL = "Manually triggered"


class CircuitBreaker:
    """Manages trading circuit breaker state."""

    def __init__(self, kv: KVClient, config: CircuitBreakerConfig | None = None):
        self.kv = kv
        self.config = config or CircuitBreakerConfig()

    async def check_status(self) -> CircuitBreakerStatus:
        """Get current circuit breaker status."""
        return await self.kv.get_circuit_breaker()

    async def is_trading_allowed(self) -> bool:
        """Check if trading is currently allowed."""
        status = await self.check_status()
        return not status.halted

    async def trip(self, reason: str) -> None:
        """Trip the circuit breaker."""
        await self.kv.trip_circuit_breaker(reason)

    async def reset(self) -> None:
        """Reset the circuit breaker (manual action)."""
        await self.kv.reset_circuit_breaker()

    async def check_daily_loss(
        self,
        starting_equity: float,
        current_equity: float,
    ) -> bool:
        """Check if daily loss limit is breached. Returns True if OK."""
        if starting_equity <= 0:
            return True

        loss_pct = (starting_equity - current_equity) / starting_equity
        if loss_pct >= self.config.daily_loss_limit_pct:
            await self.trip(CircuitBreakerReason.DAILY_LOSS)
            return False
        return True

    async def check_weekly_loss(
        self,
        starting_equity: float,
        current_equity: float,
    ) -> bool:
        """Check if weekly loss limit is breached. Returns True if OK."""
        if starting_equity <= 0:
            return True

        loss_pct = (starting_equity - current_equity) / starting_equity
        if loss_pct >= self.config.weekly_loss_limit_pct:
            await self.trip(CircuitBreakerReason.WEEKLY_LOSS)
            return False
        return True

    async def check_max_drawdown(
        self,
        peak_equity: float,
        current_equity: float,
    ) -> bool:
        """Check if maximum drawdown is breached. Returns True if OK."""
        if peak_equity <= 0:
            return True

        drawdown_pct = (peak_equity - current_equity) / peak_equity
        if drawdown_pct >= self.config.max_drawdown_pct:
            await self.trip(CircuitBreakerReason.MAX_DRAWDOWN)
            return False
        return True

    async def check_rapid_loss(self, account_equity: float) -> bool:
        """Check for rapid loss. Returns True if OK."""
        daily_stats = await self.kv.get_daily_stats()
        rapid_loss = daily_stats.get("rapid_loss_amount", 0)

        if account_equity <= 0:
            return True

        rapid_loss_pct = rapid_loss / account_equity
        if rapid_loss_pct >= self.config.rapid_loss_threshold_pct:
            await self.trip(CircuitBreakerReason.RAPID_LOSS)
            return False
        return True

    async def check_vix(self, current_vix: float) -> bool:
        """Check VIX level. Returns True if OK to trade."""
        if current_vix >= self.config.extreme_vix_threshold:
            await self.trip(CircuitBreakerReason.EXTREME_VIX)
            return False
        return True

    async def check_api_errors(self) -> bool:
        """Record an API error and check if limit is breached. Returns True if OK."""
        error_count = await self.kv.increment_error_count(window_seconds=60)
        if error_count >= self.config.max_api_errors_per_minute:
            await self.trip(CircuitBreakerReason.API_ERRORS)
            return False
        return True

    async def check_data_staleness(
        self,
        last_quote_time: datetime,
        current_time: datetime | None = None,
    ) -> bool:
        """Check if market data is stale. Returns True if OK."""
        if current_time is None:
            current_time = datetime.now()

        staleness = (current_time - last_quote_time).total_seconds()
        if staleness > self.config.stale_data_seconds:
            await self.trip(CircuitBreakerReason.STALE_DATA)
            return False
        return True

    async def run_all_checks(
        self,
        starting_daily_equity: float,
        starting_weekly_equity: float,
        peak_equity: float,
        current_equity: float,
        current_vix: float | None = None,
        last_quote_time: datetime | None = None,
    ) -> tuple[bool, str | None]:
        """Run all circuit breaker checks.

        Returns:
            Tuple of (is_ok, reason_if_tripped)
        """
        # Check if already tripped
        status = await self.check_status()
        if status.halted:
            return False, status.reason

        # Run checks in order of severity
        if not await self.check_max_drawdown(peak_equity, current_equity):
            return False, CircuitBreakerReason.MAX_DRAWDOWN

        if not await self.check_daily_loss(starting_daily_equity, current_equity):
            return False, CircuitBreakerReason.DAILY_LOSS

        if not await self.check_weekly_loss(starting_weekly_equity, current_equity):
            return False, CircuitBreakerReason.WEEKLY_LOSS

        if not await self.check_rapid_loss(current_equity):
            return False, CircuitBreakerReason.RAPID_LOSS

        if current_vix is not None:
            if not await self.check_vix(current_vix):
                return False, CircuitBreakerReason.EXTREME_VIX

        if last_quote_time is not None:
            if not await self.check_data_staleness(last_quote_time):
                return False, CircuitBreakerReason.STALE_DATA

        return True, None
