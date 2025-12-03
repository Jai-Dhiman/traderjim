from __future__ import annotations
"""Implied Volatility Rank calculations."""

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class IVMetrics:
    """IV metrics for an underlying."""
    current_iv: float
    iv_rank: float  # Percentile rank over lookback period
    iv_percentile: float  # Percentage of days IV was lower
    iv_high: float  # Highest IV in period
    iv_low: float  # Lowest IV in period


def calculate_iv_rank(
    current_iv: float,
    historical_ivs: list[float],
) -> float:
    """Calculate IV Rank.

    IV Rank = (Current IV - 52-week Low) / (52-week High - 52-week Low) * 100

    Args:
        current_iv: Current implied volatility
        historical_ivs: List of historical IV values (typically 252 trading days)

    Returns:
        IV Rank as percentage (0-100)
    """
    if not historical_ivs:
        return 50.0  # Default to middle if no history

    iv_low = min(historical_ivs)
    iv_high = max(historical_ivs)

    if iv_high == iv_low:
        return 50.0  # Avoid division by zero

    iv_rank = ((current_iv - iv_low) / (iv_high - iv_low)) * 100
    return max(0.0, min(100.0, iv_rank))


def calculate_iv_percentile(
    current_iv: float,
    historical_ivs: list[float],
) -> float:
    """Calculate IV Percentile.

    IV Percentile = Percentage of days where IV was lower than current

    Args:
        current_iv: Current implied volatility
        historical_ivs: List of historical IV values

    Returns:
        IV Percentile as percentage (0-100)
    """
    if not historical_ivs:
        return 50.0

    days_lower = sum(1 for iv in historical_ivs if iv < current_iv)
    return (days_lower / len(historical_ivs)) * 100


def calculate_iv_metrics(
    current_iv: float,
    historical_ivs: list[float],
) -> IVMetrics:
    """Calculate comprehensive IV metrics.

    Args:
        current_iv: Current implied volatility
        historical_ivs: List of historical IV values (ideally 252 for one year)

    Returns:
        IVMetrics with rank, percentile, high, and low
    """
    if not historical_ivs:
        return IVMetrics(
            current_iv=current_iv,
            iv_rank=50.0,
            iv_percentile=50.0,
            iv_high=current_iv,
            iv_low=current_iv,
        )

    return IVMetrics(
        current_iv=current_iv,
        iv_rank=calculate_iv_rank(current_iv, historical_ivs),
        iv_percentile=calculate_iv_percentile(current_iv, historical_ivs),
        iv_high=max(historical_ivs),
        iv_low=min(historical_ivs),
    )


def is_elevated_iv(iv_rank: float, threshold: float = 50.0) -> bool:
    """Check if IV is elevated enough for selling premium.

    Per PRD: Entry trigger is IV Rank >= 50 (preferably >= 70)
    """
    return iv_rank >= threshold


def get_iv_regime(iv_rank: float) -> str:
    """Categorize the current IV regime."""
    if iv_rank >= 70:
        return "high"
    elif iv_rank >= 50:
        return "elevated"
    elif iv_rank >= 30:
        return "normal"
    else:
        return "low"


# Historical IV storage helpers

@dataclass
class IVDataPoint:
    """Single IV observation."""
    date: str
    iv: float


class IVHistory:
    """Manager for historical IV data."""

    def __init__(self, lookback_days: int = 252):
        self.lookback_days = lookback_days
        self._data: dict[str, list[IVDataPoint]] = {}  # symbol -> data points

    def add_observation(self, symbol: str, date: str, iv: float) -> None:
        """Add an IV observation for a symbol."""
        if symbol not in self._data:
            self._data[symbol] = []

        self._data[symbol].append(IVDataPoint(date=date, iv=iv))

        # Trim to lookback period
        cutoff_date = (
            datetime.now() - timedelta(days=self.lookback_days)
        ).strftime("%Y-%m-%d")
        self._data[symbol] = [
            dp for dp in self._data[symbol] if dp.date >= cutoff_date
        ]

    def get_historical_ivs(self, symbol: str) -> list[float]:
        """Get historical IV values for a symbol."""
        if symbol not in self._data:
            return []
        return [dp.iv for dp in self._data[symbol]]

    def get_metrics(self, symbol: str, current_iv: float) -> IVMetrics:
        """Get IV metrics for a symbol."""
        return calculate_iv_metrics(current_iv, self.get_historical_ivs(symbol))

    def to_dict(self) -> dict:
        """Serialize to dict for storage."""
        return {
            symbol: [{"date": dp.date, "iv": dp.iv} for dp in data]
            for symbol, data in self._data.items()
        }

    @classmethod
    def from_dict(cls, data: dict, lookback_days: int = 252) -> "IVHistory":
        """Deserialize from dict."""
        history = cls(lookback_days)
        for symbol, points in data.items():
            for point in points:
                history.add_observation(symbol, point["date"], point["iv"])
        return history
