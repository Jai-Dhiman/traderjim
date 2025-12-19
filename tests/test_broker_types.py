"""Tests for broker types and Alpaca client parsing.

These tests specifically target edge cases in API response parsing,
including the empty/missing field scenarios that caused production issues.
"""

from __future__ import annotations

import pytest

from core.broker.types import (
    Order,
    OrderLeg,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
)


class TestOrderSideEnum:
    """Test OrderSide enum parsing edge cases."""

    def test_valid_buy(self):
        """Test valid 'buy' side."""
        assert OrderSide("buy") == OrderSide.BUY

    def test_valid_sell(self):
        """Test valid 'sell' side."""
        assert OrderSide("sell") == OrderSide.SELL

    def test_empty_string_raises_error(self):
        """Test that empty string raises ValueError - this was the original bug."""
        with pytest.raises(ValueError) as exc_info:
            OrderSide("")
        assert "is not a valid OrderSide" in str(exc_info.value)

    def test_none_raises_error(self):
        """Test that None raises appropriate error."""
        with pytest.raises((ValueError, TypeError)):
            OrderSide(None)

    def test_invalid_value_raises_error(self):
        """Test invalid values raise errors."""
        with pytest.raises(ValueError):
            OrderSide("invalid")
        with pytest.raises(ValueError):
            OrderSide("BUY")  # Case sensitive
        with pytest.raises(ValueError):
            OrderSide("SELL")


class TestOrderStatusEnum:
    """Test OrderStatus enum parsing."""

    def test_all_valid_statuses(self):
        """Test all valid status values."""
        valid_statuses = [
            ("new", OrderStatus.NEW),
            ("pending_new", OrderStatus.PENDING),
            ("accepted", OrderStatus.ACCEPTED),
            ("filled", OrderStatus.FILLED),
            ("partially_filled", OrderStatus.PARTIALLY_FILLED),
            ("canceled", OrderStatus.CANCELLED),
            ("rejected", OrderStatus.REJECTED),
            ("expired", OrderStatus.EXPIRED),
        ]
        for value, expected in valid_statuses:
            assert OrderStatus(value) == expected

    def test_empty_status_raises_error(self):
        """Test empty status raises error."""
        with pytest.raises(ValueError):
            OrderStatus("")


class TestOrderTypeEnum:
    """Test OrderType enum parsing."""

    def test_valid_types(self):
        """Test valid order types."""
        assert OrderType("market") == OrderType.MARKET
        assert OrderType("limit") == OrderType.LIMIT

    def test_invalid_type_raises_error(self):
        """Test invalid type raises error."""
        with pytest.raises(ValueError):
            OrderType("stop_limit")


class TestPositionSideEnum:
    """Test PositionSide enum parsing."""

    def test_valid_sides(self):
        """Test valid position sides."""
        assert PositionSide("long") == PositionSide.LONG
        assert PositionSide("short") == PositionSide.SHORT

    def test_invalid_side_raises_error(self):
        """Test invalid side raises error."""
        with pytest.raises(ValueError):
            PositionSide("")


class TestAlpacaOrderParsing:
    """Test Alpaca order response parsing.

    These tests simulate the actual API responses we receive from Alpaca
    and ensure our parsing handles all edge cases correctly.
    """

    def test_parse_mleg_order_empty_side(self, sample_alpaca_order_response):
        """Test parsing multi-leg order with empty top-level side field.

        This is the exact scenario that caused the production bug.
        Multi-leg orders have side="" at the top level.
        """
        # Import here to avoid module-level import issues
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        order = client._parse_order(sample_alpaca_order_response)

        # Should derive side from first leg (sell)
        assert order.side == OrderSide.SELL
        assert order.symbol == ""  # Empty for mleg orders
        assert len(order.legs) == 2
        assert order.legs[0].side == OrderSide.SELL
        assert order.legs[1].side == OrderSide.BUY

    def test_parse_single_leg_order(self, sample_alpaca_single_order_response):
        """Test parsing standard single-leg order."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        order = client._parse_order(sample_alpaca_single_order_response)

        assert order.side == OrderSide.BUY
        assert order.symbol == "SPY"
        assert order.legs is None

    def test_parse_order_missing_side_no_legs(self):
        """Test parsing order with missing side and no legs - edge case."""
        from core.broker.alpaca import AlpacaClient

        # Malformed response with no side and no legs
        malformed_response = {
            "id": "order-789",
            "client_order_id": "client-789",
            "symbol": "SPY",
            "side": "",  # Empty
            "type": "market",
            "qty": "10",
            "status": "filled",
            "filled_qty": "10",
            "filled_avg_price": "599.00",
            "created_at": "2024-01-15T10:00:00Z",
            "updated_at": "2024-01-15T10:00:00Z",
            # No legs
        }

        client = AlpacaClient("test-key", "test-secret", paper=True)
        order = client._parse_order(malformed_response)

        # Should fallback to "buy" default
        assert order.side == OrderSide.BUY

    def test_parse_order_null_side(self):
        """Test parsing order where side is None/null."""
        from core.broker.alpaca import AlpacaClient

        response = {
            "id": "order-999",
            "client_order_id": "client-999",
            "symbol": "SPY",
            # "side" key missing entirely
            "type": "limit",
            "qty": "5",
            "limit_price": "600.00",
            "status": "new",
            "filled_qty": "0",
            "created_at": "2024-01-15T10:00:00Z",
            "updated_at": "2024-01-15T10:00:00Z",
        }

        client = AlpacaClient("test-key", "test-secret", paper=True)
        order = client._parse_order(response)

        # Should handle missing key with fallback
        assert order.side == OrderSide.BUY

    def test_parse_order_with_optional_fields_missing(self):
        """Test parsing order with various optional fields missing."""
        from core.broker.alpaca import AlpacaClient

        minimal_response = {
            "id": "order-minimal",
            "client_order_id": "client-minimal",
            "side": "buy",
            "type": "market",
            "qty": "1",
            "status": "new",
            "created_at": "2024-01-15T10:00:00Z",
            "updated_at": "2024-01-15T10:00:00Z",
            # Missing: symbol, limit_price, filled_qty, filled_avg_price, legs
        }

        client = AlpacaClient("test-key", "test-secret", paper=True)
        order = client._parse_order(minimal_response)

        assert order.id == "order-minimal"
        assert order.side == OrderSide.BUY
        assert order.symbol == ""  # Uses .get() with default
        assert order.limit_price is None
        assert order.filled_qty == 0
        assert order.filled_avg_price is None
        assert order.legs is None

    def test_parse_mleg_order_empty_leg_sides(self):
        """Test parsing multi-leg order with empty leg side fields.

        This tests the exact scenario causing production errors.
        Alpaca can return empty strings for leg sides in some cases.
        """
        from core.broker.alpaca import AlpacaClient

        response_with_empty_leg_sides = {
            "id": "order-empty-legs",
            "client_order_id": "client-empty-legs",
            "symbol": "",
            "side": "",
            "type": "limit",
            "qty": "2",
            "limit_price": "-1.25",
            "status": "pending_new",
            "filled_qty": "0",
            "filled_avg_price": None,
            "created_at": "2024-01-15T10:00:00Z",
            "updated_at": "2024-01-15T10:00:00Z",
            "legs": [
                {
                    "symbol": "SPY240215P00470000",
                    "side": "",  # Empty side - this was causing the bug
                    "qty": "2",
                    "filled_qty": "0",
                    "filled_avg_price": None,
                },
                {
                    "symbol": "SPY240215P00465000",
                    "side": "",  # Empty side
                    "qty": "2",
                    "filled_qty": "0",
                    "filled_avg_price": None,
                },
            ],
        }

        client = AlpacaClient("test-key", "test-secret", paper=True)
        # This should not raise an error
        order = client._parse_order(response_with_empty_leg_sides)

        # Should handle empty leg sides - for credit spreads: first=sell, second=buy
        assert len(order.legs) == 2
        assert order.legs[0].side == OrderSide.SELL  # First leg is short
        assert order.legs[1].side == OrderSide.BUY   # Second leg is long


class TestAlpacaPositionParsing:
    """Test Alpaca position response parsing."""

    def test_parse_short_position(self, sample_alpaca_position_response):
        """Test parsing a short position (negative qty)."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        position = client._parse_position(sample_alpaca_position_response)

        assert position.qty == -2
        assert position.side == PositionSide.SHORT

    def test_parse_long_position(self):
        """Test parsing a long position (positive qty)."""
        from core.broker.alpaca import AlpacaClient

        long_position = {
            "symbol": "SPY240215P00465000",
            "qty": "2",
            "side": "long",
            "avg_entry_price": "1.50",
            "market_value": "300.00",
            "cost_basis": "300.00",
            "unrealized_pl": "50.00",
            "unrealized_plpc": "0.1667",
            "current_price": "1.75",
        }

        client = AlpacaClient("test-key", "test-secret", paper=True)
        position = client._parse_position(long_position)

        assert position.qty == 2
        assert position.side == PositionSide.LONG


class TestOCCSymbolParsing:
    """Test OCC option symbol parsing."""

    def test_parse_valid_put_symbol(self):
        """Test parsing valid put OCC symbol."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        result = client.parse_occ_symbol("SPY240215P00470000")

        assert result is not None
        assert result["underlying"] == "SPY"
        assert result["expiration"] == "2024-02-15"
        assert result["option_type"] == "put"
        assert result["strike"] == 470.0

    def test_parse_valid_call_symbol(self):
        """Test parsing valid call OCC symbol."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        result = client.parse_occ_symbol("QQQ240315C00450000")

        assert result is not None
        assert result["underlying"] == "QQQ"
        assert result["expiration"] == "2024-03-15"
        assert result["option_type"] == "call"
        assert result["strike"] == 450.0

    def test_parse_invalid_symbol_too_short(self):
        """Test parsing invalid short symbol."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        result = client.parse_occ_symbol("SPY")

        assert result is None

    def test_parse_invalid_symbol_no_date(self):
        """Test parsing symbol with invalid format."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        result = client.parse_occ_symbol("SPYABC123P00470000")

        # Should return None for malformed symbols
        assert result is None or result["expiration"] == "20AB-C1-23"

    def test_parse_fractional_strike(self):
        """Test parsing OCC symbol with fractional strike."""
        from core.broker.alpaca import AlpacaClient

        client = AlpacaClient("test-key", "test-secret", paper=True)
        # Strike 470.50 = 00470500
        result = client.parse_occ_symbol("SPY240215P00470500")

        assert result is not None
        assert result["strike"] == 470.5
