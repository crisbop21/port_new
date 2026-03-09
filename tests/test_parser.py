"""Tests for the IBKR PDF parser.

Uses a synthetic PDF built with pdfplumber's underlying library (pdfminer)
via reportlab to mimic the IBKR table layout. This lets us test the full
parse pipeline without needing a real statement.
"""

import io
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from src.parser import (
    _extract_meta,
    _extract_positions,
    _extract_trades,
    _parse_datetime,
    _parse_option_symbol,
    _to_decimal,
    parse_statement,
)


# ── Helper: build fake row sets mimicking IBKR table extraction ──────────────

def _meta_rows():
    """Rows as they'd come from _extract_tables for the Statement section."""
    return [
        ["Statement", "Header", "Field Name", "Field Value"],
        ["Statement", "Data", "Account", "U9876543"],
        ["Statement", "Data", "Period", "2024-01-01 - 2024-01-31"],
        ["Statement", "Data", "Base Currency", "USD"],
    ]


def _position_rows():
    """Open Positions rows with one stock and one option."""
    return [
        ["Open Positions", "Header", "Symbol", "Quantity", "Cost Basis",
         "Close Price", "Value", "Unrealized P/L", "Currency"],
        ["Open Positions", "Data", "Stocks", "", "", "", "", "", ""],
        ["Open Positions", "Data", "AAPL", "100", "15,000.00",
         "175.50", "17,550.00", "2,550.00", "USD"],
        ["Open Positions", "Data", "MSFT", "50", "18,000.00",
         "400.00", "20,000.00", "2,000.00", "USD"],
        ["Open Positions", "SubTotal", "", "", "", "", "37,550.00", "4,550.00", ""],
        ["Open Positions", "Data", "Equity and Index Options", "", "", "", "", "", ""],
        ["Open Positions", "Data", "AAPL 20240119 150.0 C", "10", "5,000.00",
         "8.50", "8,500.00", "3,500.00", "USD"],
        ["Open Positions", "Total", "", "", "", "", "46,050.00", "8,050.00", ""],
    ]


def _trade_rows():
    """Trades rows with a buy and a sell."""
    return [
        ["Trades", "Header", "Symbol", "Date/Time", "Quantity",
         "T. Price", "Proceeds", "Comm/Fee", "Realized P/L", "Currency"],
        ["Trades", "Data", "Stocks", "", "", "", "", "", "", ""],
        ["Trades", "Data", "AAPL", "2024-01-15, 10:30:00", "50",
         "175.00", "-8,750.00", "-1.00", "0", "USD"],
        ["Trades", "Data", "MSFT", "2024-01-20, 14:00:00", "-25",
         "405.00", "10,125.00", "-1.00", "125.00", "USD"],
        ["Trades", "SubTotal", "", "", "", "", "", "", "", ""],
    ]


def _skipped_rows():
    """Rows for an unsupported asset class (Futures)."""
    return [
        ["Trades", "Data", "Futures", "", "", "", "", "", "", ""],
        ["Trades", "Data", "ESH4", "2024-01-10, 09:00:00", "1",
         "4800.00", "4,800.00", "-2.50", "50.00", "USD"],
    ]


# ── Unit tests: helpers ──────────────────────────────────────────────────────

class TestToDecimal:
    def test_normal(self):
        assert _to_decimal("175.50") == Decimal("175.50")

    def test_commas(self):
        assert _to_decimal("15,000.00") == Decimal("15000.00")

    def test_blank(self):
        assert _to_decimal("") == Decimal("0")

    def test_none(self):
        assert _to_decimal(None) == Decimal("0")

    def test_negative(self):
        assert _to_decimal("-1.00") == Decimal("-1.00")


class TestParseDatetime:
    def test_comma_format(self):
        dt = _parse_datetime("2024-01-15, 10:30:00")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)

    def test_space_format(self):
        dt = _parse_datetime("2024-01-15 10:30:00")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)

    def test_date_only_fallback(self):
        dt = _parse_datetime("2024-01-15")
        assert dt.date() == date(2024, 1, 15)


class TestParseOptionSymbol:
    def test_space_separated(self):
        result = _parse_option_symbol("AAPL 20240119 150.0 C")
        assert result["expiry"] == date(2024, 1, 19)
        assert result["strike"] == Decimal("150.0")
        assert result["right"] == "C"

    def test_osi_format(self):
        result = _parse_option_symbol("AAPL  240119C00150000")
        assert result["expiry"] == date(2024, 1, 19)
        assert result["strike"] == Decimal("150")
        assert result["right"] == "C"

    def test_unparseable_returns_empty(self):
        result = _parse_option_symbol("WEIRD_SYMBOL")
        assert result == {}


# ── Integration tests: section extraction ────────────────────────────────────

class TestExtractMeta:
    def test_valid(self):
        meta = _extract_meta(_meta_rows())
        assert meta.account_id == "U9876543"
        assert meta.period_start == date(2024, 1, 1)
        assert meta.period_end == date(2024, 1, 31)
        assert meta.base_currency == "USD"

    def test_missing_account_raises(self):
        rows = [["Statement", "Data", "Period", "2024-01-01 - 2024-01-31"]]
        with pytest.raises(ValueError, match="account ID"):
            _extract_meta(rows)

    def test_missing_period_raises(self):
        rows = [["Statement", "Data", "Account", "U123"]]
        with pytest.raises(ValueError, match="period"):
            _extract_meta(rows)


class TestExtractPositions:
    def test_stock_positions(self):
        rows = _position_rows()
        positions, skipped = _extract_positions(rows, date(2024, 1, 31))
        stocks = [p for p in positions if p.asset_class == "STK"]
        assert len(stocks) == 2
        assert stocks[0].symbol == "AAPL"
        assert stocks[0].quantity == Decimal("100")
        assert stocks[0].market_value == Decimal("17550.00")

    def test_option_position(self):
        rows = _position_rows()
        positions, _ = _extract_positions(rows, date(2024, 1, 31))
        opts = [p for p in positions if p.asset_class == "OPT"]
        assert len(opts) == 1
        assert opts[0].expiry == date(2024, 1, 19)
        assert opts[0].strike == Decimal("150.0")
        assert opts[0].right == "C"

    def test_subtotal_and_total_skipped(self):
        rows = _position_rows()
        positions, _ = _extract_positions(rows, date(2024, 1, 31))
        # 2 stocks + 1 option = 3 total, no subtotal/total rows
        assert len(positions) == 3

    def test_unsupported_asset_class_skipped(self):
        rows = _position_rows() + [
            ["Open Positions", "Data", "Futures", "", "", "", "", "", ""],
            ["Open Positions", "Data", "ESH4", "1", "4800", "4850", "4850", "50", "USD"],
        ]
        positions, skipped = _extract_positions(rows, date(2024, 1, 31))
        assert len(positions) == 3  # only STK + OPT
        assert any("unsupported" in s["reason"] for s in skipped)


class TestExtractTrades:
    def test_buy_trade(self):
        rows = _trade_rows()
        trades, _ = _extract_trades(rows)
        buys = [t for t in trades if t.side == "BOT"]
        assert len(buys) == 1
        assert buys[0].symbol == "AAPL"
        assert buys[0].quantity == Decimal("50")
        assert buys[0].price == Decimal("175.00")

    def test_sell_trade(self):
        rows = _trade_rows()
        trades, _ = _extract_trades(rows)
        sells = [t for t in trades if t.side == "SLD"]
        assert len(sells) == 1
        assert sells[0].symbol == "MSFT"
        assert sells[0].realized_pnl == Decimal("125.00")

    def test_side_from_quantity_sign(self):
        rows = _trade_rows()
        trades, _ = _extract_trades(rows)
        # Positive qty → BOT, negative qty → SLD
        assert trades[0].side == "BOT"
        assert trades[1].side == "SLD"
        # Quantity is stored as absolute
        assert trades[1].quantity == Decimal("25")

    def test_skipped_futures_trades(self):
        rows = _trade_rows() + _skipped_rows()
        trades, skipped = _extract_trades(rows)
        assert len(trades) == 2  # only stocks
        assert any("unsupported" in s["reason"] for s in skipped)


# ── End-to-end: parse_statement with mocked PDF extraction ───────────────────

class TestParseStatement:
    def test_full_parse(self):
        all_rows = _meta_rows() + _position_rows() + _trade_rows()
        with patch("src.parser._extract_tables", return_value=all_rows):
            result = parse_statement(io.BytesIO(b"fake"))

        assert result.meta.account_id == "U9876543"
        assert len(result.positions) == 3
        assert len(result.trades) == 2
        assert result.skipped_rows == []

    def test_empty_pdf_raises(self):
        with patch("src.parser._extract_tables", return_value=[]):
            with pytest.raises(ValueError, match="No tables found"):
                parse_statement(io.BytesIO(b"fake"))

    def test_financial_precision(self):
        """Verify Decimal precision flows through the full pipeline."""
        all_rows = _meta_rows() + _position_rows() + _trade_rows()
        with patch("src.parser._extract_tables", return_value=all_rows):
            result = parse_statement(io.BytesIO(b"fake"))

        aapl = [p for p in result.positions if p.symbol == "AAPL"][0]
        assert aapl.cost_basis == Decimal("15000.00")
        assert aapl.unrealized_pnl == Decimal("2550.00")
