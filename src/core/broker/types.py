from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    NEW = "new"
    PENDING = "pending_new"
    ACCEPTED = "accepted"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


@dataclass
class OptionQuote:
    """Real-time quote for an option contract."""
    symbol: str
    bid: float
    ask: float
    last: float
    bid_size: int
    ask_size: int
    volume: int
    timestamp: datetime


@dataclass
class OptionContract:
    """Option contract details from broker."""
    symbol: str  # OCC symbol (e.g., SPY240119C00500000)
    underlying: str
    expiration: str  # YYYY-MM-DD
    strike: float
    option_type: Literal["call", "put"]

    # Quote data
    bid: float
    ask: float
    last: float
    volume: int
    open_interest: int

    # Greeks (if available)
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    implied_volatility: float | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


@dataclass
class OptionsChain:
    """Full options chain for an underlying."""
    underlying: str
    underlying_price: float
    timestamp: datetime
    expirations: list[str]
    contracts: list[OptionContract]

    def get_expiration(self, expiration: str) -> list[OptionContract]:
        """Get all contracts for a specific expiration."""
        return [c for c in self.contracts if c.expiration == expiration]

    def get_puts(self, expiration: str | None = None) -> list[OptionContract]:
        """Get all put contracts, optionally filtered by expiration."""
        contracts = self.contracts if expiration is None else self.get_expiration(expiration)
        return [c for c in contracts if c.option_type == "put"]

    def get_calls(self, expiration: str | None = None) -> list[OptionContract]:
        """Get all call contracts, optionally filtered by expiration."""
        contracts = self.contracts if expiration is None else self.get_expiration(expiration)
        return [c for c in contracts if c.option_type == "call"]


@dataclass
class Order:
    """Order details."""
    id: str
    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    qty: int
    limit_price: float | None
    status: OrderStatus
    filled_qty: int
    filled_avg_price: float | None
    created_at: datetime
    updated_at: datetime
    legs: list["OrderLeg"] | None = None


@dataclass
class OrderLeg:
    """Leg of a multi-leg order."""
    symbol: str
    side: OrderSide
    qty: int
    filled_qty: int
    filled_avg_price: float | None


@dataclass
class BrokerPosition:
    """Position held at broker."""
    symbol: str
    qty: int
    side: PositionSide
    avg_entry_price: float
    market_value: float
    cost_basis: float
    unrealized_pl: float
    unrealized_plpc: float  # Percentage
    current_price: float


@dataclass
class Account:
    """Broker account information."""
    id: str
    status: str
    currency: str
    cash: float
    portfolio_value: float
    buying_power: float
    equity: float
    last_equity: float
    multiplier: int
    daytrading_buying_power: float
    regt_buying_power: float
    pattern_day_trader: bool


@dataclass
class SpreadOrder:
    """Credit spread order request."""
    underlying: str
    short_symbol: str  # OCC symbol for short leg
    long_symbol: str   # OCC symbol for long leg
    contracts: int
    limit_price: float  # Net credit per spread

    @property
    def is_credit_spread(self) -> bool:
        return self.limit_price > 0
