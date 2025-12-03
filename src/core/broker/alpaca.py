from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from core import http
from core.broker.types import (
    Account,
    BrokerPosition,
    OptionContract,
    OptionsChain,
    Order,
    OrderLeg,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    SpreadOrder,
)


class AlpacaError(Exception):
    """Alpaca API error."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class AlpacaClient:
    """Client for Alpaca Trading API with options support."""

    PAPER_BASE_URL = "https://paper-api.alpaca.markets"
    LIVE_BASE_URL = "https://api.alpaca.markets"
    DATA_BASE_URL = "https://data.alpaca.markets"

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        paper: bool = True,
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper
        self.base_url = self.PAPER_BASE_URL if paper else self.LIVE_BASE_URL

        self._headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        json_data: dict | None = None,
    ) -> Any:
        """Make an authenticated request to Alpaca."""
        try:
            return await http.request(
                method,
                url,
                headers=self._headers,
                params=params,
                json_data=json_data,
            )
        except Exception as e:
            error_msg = str(e)
            status_code = None
            if error_msg.startswith("HTTP "):
                parts = error_msg.split(":", 1)
                if parts[0].startswith("HTTP "):
                    try:
                        status_code = int(parts[0].replace("HTTP ", ""))
                    except ValueError:
                        pass
            raise AlpacaError(error_msg, status_code=status_code)

    async def _trading_request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_data: dict | None = None,
    ) -> Any:
        """Make a request to the trading API."""
        url = f"{self.base_url}{path}"
        return await self._request(method, url, params, json_data)

    async def _data_request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
    ) -> Any:
        """Make a request to the data API."""
        url = f"{self.DATA_BASE_URL}{path}"
        return await self._request(method, url, params)

    # Account

    async def get_account(self) -> Account:
        """Get account information."""
        data = await self._trading_request("GET", "/v2/account")
        return Account(
            id=data["id"],
            status=data["status"],
            currency=data["currency"],
            cash=float(data["cash"]),
            portfolio_value=float(data["portfolio_value"]),
            buying_power=float(data["buying_power"]),
            equity=float(data["equity"]),
            last_equity=float(data["last_equity"]),
            multiplier=int(data["multiplier"]),
            daytrading_buying_power=float(data["daytrading_buying_power"]),
            regt_buying_power=float(data["regt_buying_power"]),
            pattern_day_trader=data["pattern_day_trader"],
        )

    # Positions

    async def get_positions(self) -> list[BrokerPosition]:
        """Get all open positions."""
        data = await self._trading_request("GET", "/v2/positions")
        return [self._parse_position(p) for p in data]

    async def get_position(self, symbol: str) -> BrokerPosition | None:
        """Get a specific position."""
        try:
            data = await self._trading_request("GET", f"/v2/positions/{symbol}")
            return self._parse_position(data)
        except AlpacaError as e:
            if e.status_code == 404:
                return None
            raise

    def _parse_position(self, data: dict) -> BrokerPosition:
        return BrokerPosition(
            symbol=data["symbol"],
            qty=int(data["qty"]),
            side=PositionSide.LONG if int(data["qty"]) > 0 else PositionSide.SHORT,
            avg_entry_price=float(data["avg_entry_price"]),
            market_value=float(data["market_value"]),
            cost_basis=float(data["cost_basis"]),
            unrealized_pl=float(data["unrealized_pl"]),
            unrealized_plpc=float(data["unrealized_plpc"]),
            current_price=float(data["current_price"]),
        )

    # Options Chain

    async def get_options_chain(
        self,
        symbol: str,
        expiration_start: str | None = None,
        expiration_end: str | None = None,
    ) -> OptionsChain:
        """Get options chain for a symbol.

        Args:
            symbol: Underlying symbol (e.g., SPY)
            expiration_start: Start date for expirations (YYYY-MM-DD)
            expiration_end: End date for expirations (YYYY-MM-DD)
        """
        # Default to 30-60 DTE range if not specified
        if expiration_start is None:
            expiration_start = (datetime.now() + timedelta(days=25)).strftime("%Y-%m-%d")
        if expiration_end is None:
            expiration_end = (datetime.now() + timedelta(days=50)).strftime("%Y-%m-%d")

        params = {
            "underlying_symbols": symbol,
            "expiration_date_gte": expiration_start,
            "expiration_date_lte": expiration_end,
            "limit": 1000,
        }

        # Get options contracts
        data = await self._data_request("GET", "/v1beta1/options/snapshots", params)

        contracts = []
        expirations = set()

        for occ_symbol, snapshot in data.get("snapshots", {}).items():
            contract = self._parse_option_contract(occ_symbol, snapshot)
            if contract:
                contracts.append(contract)
                expirations.add(contract.expiration)

        # Get underlying price
        underlying_price = await self._get_underlying_price(symbol)

        return OptionsChain(
            underlying=symbol,
            underlying_price=underlying_price,
            timestamp=datetime.now(),
            expirations=sorted(expirations),
            contracts=contracts,
        )

    async def _get_underlying_price(self, symbol: str) -> float:
        """Get current price of underlying."""
        data = await self._data_request(
            "GET",
            f"/v2/stocks/{symbol}/quotes/latest",
        )
        quote = data.get("quote", {})
        # Use midpoint of bid/ask
        bid = float(quote.get("bp", 0))
        ask = float(quote.get("ap", 0))
        if bid and ask:
            return (bid + ask) / 2
        return float(quote.get("ap", 0) or quote.get("bp", 0))

    def _parse_option_contract(self, occ_symbol: str, snapshot: dict) -> OptionContract | None:
        """Parse option contract from Alpaca snapshot."""
        try:
            # Parse OCC symbol: SPY240119C00500000
            # Format: SYMBOL + YYMMDD + C/P + Strike*1000 (8 digits)
            underlying = ""
            i = 0
            while i < len(occ_symbol) and not occ_symbol[i].isdigit():
                underlying += occ_symbol[i]
                i += 1

            date_part = occ_symbol[i : i + 6]
            option_type = "call" if occ_symbol[i + 6] == "C" else "put"
            strike = int(occ_symbol[i + 7 :]) / 1000

            expiration = f"20{date_part[:2]}-{date_part[2:4]}-{date_part[4:6]}"

            quote = snapshot.get("latestQuote", {})
            trade = snapshot.get("latestTrade", {})
            greeks = snapshot.get("greeks", {})

            return OptionContract(
                symbol=occ_symbol,
                underlying=underlying,
                expiration=expiration,
                strike=strike,
                option_type=option_type,
                bid=float(quote.get("bp", 0)),
                ask=float(quote.get("ap", 0)),
                last=float(trade.get("p", 0)),
                volume=int(snapshot.get("dailyBar", {}).get("v", 0)),
                open_interest=int(snapshot.get("openInterest", 0)),
                delta=float(greeks.get("delta")) if greeks.get("delta") else None,
                gamma=float(greeks.get("gamma")) if greeks.get("gamma") else None,
                theta=float(greeks.get("theta")) if greeks.get("theta") else None,
                vega=float(greeks.get("vega")) if greeks.get("vega") else None,
                implied_volatility=float(greeks.get("impliedVolatility"))
                if greeks.get("impliedVolatility")
                else None,
            )
        except (ValueError, IndexError, KeyError):
            return None

    # Orders

    async def place_spread_order(self, spread: SpreadOrder) -> Order:
        """Place a credit spread order (sell short, buy long)."""
        order_data = {
            "symbol": spread.underlying,
            "qty": spread.contracts,
            "side": "sell",  # Selling the spread for credit
            "type": "limit",
            "time_in_force": "day",
            "limit_price": str(spread.limit_price),
            "order_class": "mleg",
            "legs": [
                {
                    "symbol": spread.short_symbol,
                    "side": "sell",
                    "qty": spread.contracts,
                },
                {
                    "symbol": spread.long_symbol,
                    "side": "buy",
                    "qty": spread.contracts,
                },
            ],
        }

        data = await self._trading_request("POST", "/v2/orders", json_data=order_data)
        return self._parse_order(data)

    async def place_close_spread_order(
        self,
        short_symbol: str,
        long_symbol: str,
        contracts: int,
        limit_price: float,
    ) -> Order:
        """Close a credit spread (buy back short, sell long)."""
        order_data = {
            "qty": contracts,
            "side": "buy",  # Buying back the spread
            "type": "limit",
            "time_in_force": "day",
            "limit_price": str(limit_price),
            "order_class": "mleg",
            "legs": [
                {
                    "symbol": short_symbol,
                    "side": "buy",
                    "qty": contracts,
                },
                {
                    "symbol": long_symbol,
                    "side": "sell",
                    "qty": contracts,
                },
            ],
        }

        data = await self._trading_request("POST", "/v2/orders", json_data=order_data)
        return self._parse_order(data)

    async def get_order(self, order_id: str) -> Order:
        """Get order by ID."""
        data = await self._trading_request("GET", f"/v2/orders/{order_id}")
        return self._parse_order(data)

    async def cancel_order(self, order_id: str) -> None:
        """Cancel an order."""
        await self._trading_request("DELETE", f"/v2/orders/{order_id}")

    async def get_orders(
        self,
        status: str = "open",
        limit: int = 50,
    ) -> list[Order]:
        """Get orders with optional status filter."""
        params = {"status": status, "limit": limit}
        data = await self._trading_request("GET", "/v2/orders", params=params)
        return [self._parse_order(o) for o in data]

    def _parse_order(self, data: dict) -> Order:
        legs = None
        if data.get("legs"):
            legs = [
                OrderLeg(
                    symbol=leg["symbol"],
                    side=OrderSide(leg["side"]),
                    qty=int(leg["qty"]),
                    filled_qty=int(leg.get("filled_qty", 0)),
                    filled_avg_price=float(leg["filled_avg_price"])
                    if leg.get("filled_avg_price")
                    else None,
                )
                for leg in data["legs"]
            ]

        return Order(
            id=data["id"],
            client_order_id=data["client_order_id"],
            symbol=data.get("symbol", ""),
            side=OrderSide(data["side"]),
            order_type=OrderType(data["type"]),
            qty=int(data["qty"]),
            limit_price=float(data["limit_price"]) if data.get("limit_price") else None,
            status=OrderStatus(data["status"]),
            filled_qty=int(data.get("filled_qty", 0)),
            filled_avg_price=float(data["filled_avg_price"])
            if data.get("filled_avg_price")
            else None,
            created_at=datetime.fromisoformat(data["created_at"].replace("Z", "+00:00")),
            updated_at=datetime.fromisoformat(data["updated_at"].replace("Z", "+00:00")),
            legs=legs,
        )

    # Market hours

    async def is_market_open(self) -> bool:
        """Check if the market is currently open."""
        data = await self._trading_request("GET", "/v2/clock")
        return data.get("is_open", False)

    async def get_market_hours(self) -> dict:
        """Get today's market hours."""
        data = await self._trading_request("GET", "/v2/clock")
        return {
            "is_open": data.get("is_open", False),
            "next_open": data.get("next_open"),
            "next_close": data.get("next_close"),
        }
