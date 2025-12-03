from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Literal


class SpreadType(str, Enum):
    BULL_PUT = "bull_put"
    BEAR_CALL = "bear_call"


class RecommendationStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    EXECUTED = "executed"


class TradeStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class Confidence(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class Greeks:
    delta: float
    gamma: float
    theta: float
    vega: float


@dataclass
class OptionContract:
    symbol: str
    underlying: str
    expiration: str
    strike: float
    option_type: Literal["call", "put"]
    bid: float
    ask: float
    last: float
    volume: int
    open_interest: int
    implied_volatility: float
    greeks: Greeks | None = None


@dataclass
class CreditSpread:
    underlying: str
    spread_type: SpreadType
    short_strike: float
    long_strike: float
    expiration: str
    short_contract: OptionContract
    long_contract: OptionContract

    @property
    def width(self) -> float:
        return abs(self.short_strike - self.long_strike)

    @property
    def credit(self) -> float:
        short_mid = (self.short_contract.bid + self.short_contract.ask) / 2
        long_mid = (self.long_contract.bid + self.long_contract.ask) / 2
        return short_mid - long_mid

    @property
    def max_loss(self) -> float:
        return (self.width - self.credit) * 100

    @property
    def max_profit(self) -> float:
        return self.credit * 100


@dataclass
class Recommendation:
    id: str
    created_at: datetime
    expires_at: datetime
    status: RecommendationStatus

    underlying: str
    spread_type: SpreadType
    short_strike: float
    long_strike: float
    expiration: str

    credit: float
    max_loss: float

    iv_rank: float | None = None
    delta: float | None = None
    theta: float | None = None

    thesis: str | None = None
    confidence: Confidence | None = None
    suggested_contracts: int | None = None

    analysis_price: float | None = None
    discord_message_id: str | None = None


@dataclass
class Trade:
    id: str
    recommendation_id: str | None

    opened_at: datetime | None
    closed_at: datetime | None
    status: TradeStatus

    underlying: str
    spread_type: SpreadType
    short_strike: float
    long_strike: float
    expiration: str

    entry_credit: float
    exit_debit: float | None = None
    profit_loss: float | None = None

    contracts: int = 1
    broker_order_id: str | None = None

    reflection: str | None = None
    lesson: str | None = None


@dataclass
class Position:
    id: str
    trade_id: str

    underlying: str
    short_strike: float
    long_strike: float
    expiration: str
    contracts: int

    current_value: float
    unrealized_pnl: float

    updated_at: datetime


@dataclass
class DailyPerformance:
    date: str
    starting_balance: float
    ending_balance: float
    realized_pnl: float
    trades_opened: int = 0
    trades_closed: int = 0
    win_count: int = 0
    loss_count: int = 0


@dataclass
class PlaybookRule:
    id: str
    rule: str
    source: Literal["initial", "learned"]
    supporting_trade_ids: list[str] = field(default_factory=list)
    created_at: datetime | None = None


@dataclass
class CircuitBreakerStatus:
    halted: bool
    reason: str | None = None
    triggered_at: datetime | None = None

    @classmethod
    def active(cls) -> "CircuitBreakerStatus":
        return cls(halted=False)

    @classmethod
    def tripped(cls, reason: str) -> "CircuitBreakerStatus":
        return cls(halted=True, reason=reason, triggered_at=datetime.now())


@dataclass
class AccountInfo:
    equity: float
    cash: float
    buying_power: float
    portfolio_value: float
