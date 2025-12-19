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

    async def get_option_positions(self) -> list[BrokerPosition]:
        """Get all open option positions.

        Filters positions to only include options (OCC symbols).
        OCC format: SYMBOL + YYMMDD + C/P + Strike*1000
        """
        all_positions = await self.get_positions()
        option_positions = []

        for pos in all_positions:
            # OCC symbols have letters followed by digits followed by C/P followed by more digits
            # Example: SPY240119C00500000
            symbol = pos.symbol
            if len(symbol) > 10:
                # Check if it looks like an OCC symbol
                # Find where the date portion starts (first digit after letters)
                i = 0
                while i < len(symbol) and not symbol[i].isdigit():
                    i += 1
                if i > 0 and i < len(symbol) - 7:
                    # Check for C or P after 6 digits
                    if len(symbol) > i + 6 and symbol[i + 6] in ("C", "P"):
                        option_positions.append(pos)

        return option_positions

    def parse_occ_symbol(self, occ_symbol: str) -> dict | None:
        """Parse an OCC option symbol into components.

        Args:
            occ_symbol: OCC format symbol (e.g., SPY240119C00500000)

        Returns:
            Dict with underlying, expiration, option_type, strike or None if invalid
        """
        try:
            underlying = ""
            i = 0
            while i < len(occ_symbol) and not occ_symbol[i].isdigit():
                underlying += occ_symbol[i]
                i += 1

            if not underlying or len(occ_symbol) < i + 15:
                return None

            date_part = occ_symbol[i : i + 6]
            option_type = "call" if occ_symbol[i + 6] == "C" else "put"
            strike = int(occ_symbol[i + 7 :]) / 1000

            expiration = f"20{date_part[:2]}-{date_part[2:4]}-{date_part[4:6]}"

            return {
                "underlying": underlying,
                "expiration": expiration,
                "option_type": option_type,
                "strike": strike,
            }
        except (ValueError, IndexError):
            return None

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

        # Step 1: Get option contracts from trading API
        contracts_params = {
            "underlying_symbols": symbol,
            "expiration_date_gte": expiration_start,
            "expiration_date_lte": expiration_end,
            "status": "active",
            "limit": 1000,
        }

        contracts_data = await self._trading_request(
            "GET", "/v2/options/contracts", params=contracts_params
        )

        option_contracts = contracts_data.get("option_contracts", [])
        if not option_contracts:
            # Get underlying price even if no contracts
            underlying_price = await self._get_underlying_price(symbol)
            return OptionsChain(
                underlying=symbol,
                underlying_price=underlying_price,
                timestamp=datetime.now(),
                expirations=[],
                contracts=[],
            )

        # Build a lookup of contract metadata (open_interest, etc.) from trading API
        contract_metadata = {}
        for c in option_contracts:
            contract_metadata[c["symbol"]] = {
                "open_interest": int(c.get("open_interest") or 0),
            }

        # Step 2: Get snapshots for the option symbols (batch in groups of 100)
        all_symbols = [c["symbol"] for c in option_contracts]
        contracts = []
        expirations = set()

        # Process in batches of 100 symbols
        batch_size = 100
        for i in range(0, len(all_symbols), batch_size):
            batch_symbols = all_symbols[i : i + batch_size]
            symbols_param = ",".join(batch_symbols)

            snapshot_params = {"symbols": symbols_param}
            snapshot_data = await self._data_request(
                "GET", "/v1beta1/options/snapshots", params=snapshot_params
            )

            for occ_symbol, snapshot in snapshot_data.get("snapshots", {}).items():
                # Merge open_interest from contract metadata
                metadata = contract_metadata.get(occ_symbol, {})
                contract = self._parse_option_contract(occ_symbol, snapshot, metadata)
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

    def _parse_option_contract(
        self, occ_symbol: str, snapshot: dict, metadata: dict | None = None
    ) -> OptionContract | None:
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
            metadata = metadata or {}

            # Open interest from contract metadata (trading API), fallback to snapshot
            open_interest = metadata.get("open_interest") or int(snapshot.get("openInterest", 0))

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
                open_interest=open_interest,
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
        """Place a credit spread order (sell short, buy long).

        For credit spreads, limit_price should be negative (credit received).
        """
        # Alpaca mleg orders use negative limit_price for credits
        # Round to 2 decimal places - Alpaca rejects prices with more precision
        limit_price = round(-abs(spread.limit_price), 2)

        order_data = {
            "order_class": "mleg",
            "qty": str(spread.contracts),
            "type": "limit",
            "time_in_force": "day",
            "limit_price": str(limit_price),
            "legs": [
                {
                    "symbol": spread.short_symbol,
                    "side": "sell",
                    "ratio_qty": "1",
                    "position_intent": "sell_to_open",
                },
                {
                    "symbol": spread.long_symbol,
                    "side": "buy",
                    "ratio_qty": "1",
                    "position_intent": "buy_to_open",
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
        """Close a credit spread (buy back short, sell long).

        For closing credit spreads (debit), limit_price should be positive.
        """
        # Closing a credit spread is a debit (positive limit_price)
        # Round to 2 decimal places - Alpaca rejects prices with more precision
        limit_price = round(abs(limit_price), 2)

        order_data = {
            "order_class": "mleg",
            "qty": str(contracts),
            "type": "limit",
            "time_in_force": "day",
            "limit_price": str(limit_price),
            "legs": [
                {
                    "symbol": short_symbol,
                    "side": "buy",
                    "ratio_qty": "1",
                    "position_intent": "buy_to_close",
                },
                {
                    "symbol": long_symbol,
                    "side": "sell",
                    "ratio_qty": "1",
                    "position_intent": "sell_to_close",
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
        # Debug: log raw order data for troubleshooting OrderSide issues
        print(f"DEBUG _parse_order: side={data.get('side')!r}, legs={bool(data.get('legs'))}")
        if data.get("legs"):
            for i, leg in enumerate(data["legs"]):
                print(f"DEBUG _parse_order: leg[{i}] side={leg.get('side')!r}")

        legs = None
        if data.get("legs"):
            parsed_legs = []
            for i, leg in enumerate(data["legs"]):
                # Handle empty/missing/invalid leg side
                leg_side_str = leg.get("side", "")
                # Normalize: convert to lowercase string, handle None
                if leg_side_str is None:
                    leg_side_str = ""
                leg_side_str = str(leg_side_str).lower().strip()

                # Validate side value
                if leg_side_str not in ("buy", "sell"):
                    # For credit spreads: first leg is sell (short), second is buy (long)
                    leg_side_str = "sell" if i == 0 else "buy"
                    print(f"DEBUG: Defaulted leg[{i}] side to {leg_side_str}")

                parsed_legs.append(
                    OrderLeg(
                        symbol=leg["symbol"],
                        side=OrderSide(leg_side_str),
                        qty=int(leg["qty"]),
                        filled_qty=int(leg.get("filled_qty", 0)),
                        filled_avg_price=float(leg["filled_avg_price"])
                        if leg.get("filled_avg_price")
                        else None,
                    )
                )
            legs = parsed_legs

        # For multi-leg orders, Alpaca returns empty string for top-level side
        # since each leg has its own side. Derive from first leg if available.
        side_str = data.get("side", "")
        # Normalize: convert to lowercase string, handle None
        if side_str is None:
            side_str = ""
        side_str = str(side_str).lower().strip()

        if side_str not in ("buy", "sell"):
            if legs:
                # Use first leg's side (already validated)
                side_str = legs[0].side.value
            else:
                side_str = "buy"  # Default fallback
            print(f"DEBUG: Defaulted order side to {side_str}")

        order_side = OrderSide(side_str)

        return Order(
            id=data["id"],
            client_order_id=data["client_order_id"],
            symbol=data.get("symbol", ""),
            side=order_side,
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

    # VIX Data

    async def get_vix_snapshot(self) -> dict | None:
        """Get current VIX and VIX3M levels.

        Uses CBOE VIX Index via Alpaca's market data API.
        VIX is available as a tradeable asset.

        Returns:
            Dict with 'vix' and optionally 'vix3m' values, or None if unavailable.
        """
        result = {}

        # Fetch VIX (CBOE Volatility Index)
        try:
            # VIX is available via the stocks endpoint as $VIX.X or similar
            # Alpaca uses different symbols - try common variations
            for vix_symbol in ["$VIX.X", "VIX", "VIXY"]:
                try:
                    data = await self._data_request(
                        "GET",
                        f"/v2/stocks/{vix_symbol}/quotes/latest",
                    )
                    quote = data.get("quote", {})
                    bid = float(quote.get("bp", 0))
                    ask = float(quote.get("ap", 0))
                    if bid and ask:
                        result["vix"] = (bid + ask) / 2
                        break
                    elif bid or ask:
                        result["vix"] = bid or ask
                        break
                except AlpacaError:
                    continue

            # If direct VIX not available, try to get from options-based proxy
            if "vix" not in result:
                # Calculate VIX proxy from SPY options IV
                try:
                    chain = await self.get_options_chain("SPY")
                    if chain.contracts:
                        atm_contracts = [
                            c
                            for c in chain.contracts
                            if abs(c.strike - chain.underlying_price) < chain.underlying_price * 0.02
                            and c.implied_volatility
                        ]
                        if atm_contracts:
                            avg_iv = sum(c.implied_volatility for c in atm_contracts) / len(
                                atm_contracts
                            )
                            # Convert annualized IV to VIX-like value (VIX = IV * 100)
                            result["vix"] = avg_iv * 100
                except Exception:
                    pass

        except Exception as e:
            print(f"Error fetching VIX: {e}")

        # Try to fetch VIX3M for term structure analysis
        try:
            for vix3m_symbol in ["$VIX3M.X", "VIX3M"]:
                try:
                    data = await self._data_request(
                        "GET",
                        f"/v2/stocks/{vix3m_symbol}/quotes/latest",
                    )
                    quote = data.get("quote", {})
                    bid = float(quote.get("bp", 0))
                    ask = float(quote.get("ap", 0))
                    if bid and ask:
                        result["vix3m"] = (bid + ask) / 2
                        break
                    elif bid or ask:
                        result["vix3m"] = bid or ask
                        break
                except AlpacaError:
                    continue
        except Exception:
            pass  # VIX3M is optional

        return result if result else None

    # Order Execution Protocol

    async def replace_order(
        self,
        order_id: str,
        qty: int | None = None,
        limit_price: float | None = None,
    ) -> Order:
        """Replace/modify an existing order.

        Args:
            order_id: The order to replace
            qty: New quantity (optional)
            limit_price: New limit price (optional)

        Returns:
            The new replacement order
        """
        payload = {}
        if qty is not None:
            payload["qty"] = str(qty)
        if limit_price is not None:
            payload["limit_price"] = str(round(limit_price, 2))

        data = await self._trading_request(
            "PATCH",
            f"/v2/orders/{order_id}",
            json=payload,
        )
        return self._parse_order(data)

    async def monitor_order_fill(
        self,
        order_id: str,
        timeout_seconds: int = 900,  # 15 minutes default
        poll_interval_seconds: int = 30,
        price_adjustment_schedule: list[tuple[int, float]] | None = None,
    ) -> tuple[Order, bool]:
        """Monitor an order for fill with optional price adjustment.

        Research-backed execution protocol:
        - Initial: Submit at mid-price
        - At 5 min unfilled: Adjust 1-2 cents toward natural
        - At 10 min unfilled: Adjust another 2-3 cents
        - At 13 min unfilled: Final adjustment before timeout

        Args:
            order_id: The order to monitor
            timeout_seconds: Maximum time to wait for fill
            poll_interval_seconds: How often to check order status
            price_adjustment_schedule: List of (seconds, adjustment) pairs
                Default: [(300, 0.02), (600, 0.03), (780, 0.02)]

        Returns:
            Tuple of (final_order, was_filled)
        """
        import asyncio

        if price_adjustment_schedule is None:
            # Default: adjust at 5 min, 10 min, 13 min
            price_adjustment_schedule = [
                (300, 0.02),  # 5 min: +2 cents toward natural
                (600, 0.03),  # 10 min: +3 more cents
                (780, 0.02),  # 13 min: +2 more cents (final)
            ]

        start_time = datetime.now()
        adjustments_made = 0
        current_order = await self.get_order(order_id)

        while True:
            elapsed = (datetime.now() - start_time).total_seconds()

            # Check timeout
            if elapsed >= timeout_seconds:
                return current_order, False

            # Check if filled
            if current_order.status == OrderStatus.FILLED:
                return current_order, True

            # Check if cancelled/rejected
            if current_order.status in [
                OrderStatus.CANCELED,
                OrderStatus.EXPIRED,
                OrderStatus.REJECTED,
            ]:
                return current_order, False

            # Check if we should adjust price
            if (
                current_order.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]
                and current_order.limit_price
                and adjustments_made < len(price_adjustment_schedule)
            ):
                threshold_time, adjustment = price_adjustment_schedule[adjustments_made]
                if elapsed >= threshold_time:
                    # Adjust price toward natural (worse for us = better fill chance)
                    # For selling spreads, lower price = more likely to fill
                    new_price = current_order.limit_price - adjustment
                    new_price = max(0.01, round(new_price, 2))

                    try:
                        current_order = await self.replace_order(
                            order_id=current_order.id,
                            limit_price=new_price,
                        )
                        adjustments_made += 1
                        print(f"Adjusted order price to ${new_price:.2f} (attempt {adjustments_made})")
                    except AlpacaError as e:
                        print(f"Failed to adjust order price: {e}")

            # Wait before next poll
            await asyncio.sleep(poll_interval_seconds)

            # Refresh order status
            try:
                current_order = await self.get_order(order_id)
            except AlpacaError:
                # Order might have been filled/cancelled
                break

        return current_order, current_order.status == OrderStatus.FILLED
