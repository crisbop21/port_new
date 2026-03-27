"""Tests for the IBKR PDF parser.

Unit tests use synthetic row data matching the real IBKR PDF layout.
Integration test runs against the real fixture PDF.
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
    _is_total,
    _parse_datetime,
    _parse_option_symbol,
    _split_accounts,
    _to_decimal,
    parse_statement,
)


# ── Synthetic row builders matching real IBKR format ─────────────────────────

def _account_info_rows(account_id="U9876543", currency="SGD"):
    return [
        ["Account Information", ""],
        ["Name", "TEST USER"],
        ["Account", account_id],
        ["Account Type", "Individual"],
        ["Base Currency", currency],
    ]


def _nav_rows(prior="December 31, 2025", current="March 6, 2026"):
    return [
        ["Net Asset Value", "", "", "", "", ""],
        [prior, "", current, "", "", ""],
        ["", "Total", "Long", "Short", "Total", "Change"],
        ["Cash", "10,000.00", "5,000.00", "0.00", "5,000.00", "-5,000.00"],
        ["Total", "50,000.00", "45,000.00", "0.00", "45,000.00", "-5,000.00"],
    ]


def _position_rows():
    return [
        ["Open Positions", "", "", "", "", "", "", "", ""],
        ["Symbol", "Quantity", "Mult", "Cost Price", "Cost Basis",
         "Close Price", "Value", "Unrealized P/L", "Code"],
        ["Stocks", "", "", "", "", "", "", "", ""],
        ["USD", "", "", "", "", "", "", "", ""],
        ["AAPL", "100", "1", "150.00", "15,000.00",
         "175.50", "17,550.00", "2,550.00", ""],
        ["MSFT", "50", "1", "360.00", "18,000.00",
         "400.00", "20,000.00", "2,000.00", ""],
        ["Total", "", "", "", "33,000.00", "", "37,550.00", "4,550.00", ""],
        ["Total in SGD", "", "", "", "42,000.00", "", "48,000.00", "6,000.00", ""],
        ["Symbol", "Quantity", "Mult", "Cost Price", "Cost Basis",
         "Close Price", "Value", "Unrealized P/L", "Code"],
        ["Equity and Index Options", "", "", "", "", "", "", "", ""],
        ["USD", "", "", "", "", "", "", "", ""],
        ["EEM 31MAR26 48 C", "6", "100", "3.49", "2,095.71",
         "9.68", "5,806.98", "3,711.27", ""],
        ["Total", "", "", "", "2,095.71", "", "5,806.98", "3,711.27", ""],
    ]


def _trade_rows():
    return [
        ["Trades", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["Symbol", "Date/Time", "", "Quantity", "T. Price", "C. Price",
         "Proceeds", "Comm/Fee", "Basis", "Realized P/L", "", "MTM P/L", "Code"],
        ["Stocks", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["USD", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["AAPL", "2026-01-15,\n10:30:00", "", "50", "175.00", "176.00",
         "-8,750.00", "-1.09", "8,751.09", "0.00", "", "-50.00", "O"],
        ["Total AAPL", "", "", "50", "", "", "-8,750.00", "-1.09",
         "8,751.09", "0.00", "", "-50.00", ""],
        ["MSFT", "2026-01-20,\n14:00:00", "", "-25", "405.00", "400.00",
         "10,125.00", "-1.09", "-9,000.00", "1,123.91", "", "125.00", "C"],
        ["Total MSFT", "", "", "-25", "", "", "10,125.00", "-1.09",
         "-9,000.00", "1,123.91", "", "125.00", ""],
        ["Total", "", "", "", "", "", "1,375.00", "-2.18",
         "-248.91", "1,123.91", "", "75.00", ""],
    ]


def _full_account_rows():
    return (
        _account_info_rows() + _nav_rows() + _position_rows() + _trade_rows()
    )


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
        assert _to_decimal("-1.09") == Decimal("-1.09")


class TestParseDatetime:
    def test_comma_with_newline(self):
        dt = _parse_datetime("2026-01-15,\n10:30:00")
        assert dt == datetime(2026, 1, 15, 10, 30, 0)

    def test_comma_format(self):
        dt = _parse_datetime("2026-01-15, 10:30:00")
        assert dt == datetime(2026, 1, 15, 10, 30, 0)

    def test_date_only_fallback(self):
        dt = _parse_datetime("2026-01-15")
        assert dt.date() == date(2026, 1, 15)


class TestParseOptionSymbol:
    def test_ddmmmyy_format(self):
        """Real IBKR format: 'EEM 31MAR26 48 C'"""
        result = _parse_option_symbol("EEM 31MAR26 48 C")
        assert result["expiry"] == date(2026, 3, 31)
        assert result["strike"] == Decimal("48")
        assert result["right"] == "C"

    def test_ddmmmyy_put(self):
        result = _parse_option_symbol("SPY 31DEC26 680 P")
        assert result["expiry"] == date(2026, 12, 31)
        assert result["strike"] == Decimal("680")
        assert result["right"] == "P"

    def test_ddmmmyy_decimal_strike(self):
        result = _parse_option_symbol("PYPL 18SEP26 87.5 C")
        assert result["strike"] == Decimal("87.5")
        assert result["right"] == "C"

    def test_yyyymmdd_format(self):
        result = _parse_option_symbol("AAPL 20240119 150.0 C")
        assert result["expiry"] == date(2024, 1, 19)
        assert result["strike"] == Decimal("150.0")

    def test_osi_format(self):
        result = _parse_option_symbol("AAPL  240119C00150000")
        assert result["expiry"] == date(2024, 1, 19)
        assert result["strike"] == Decimal("150")
        assert result["right"] == "C"

    def test_unparseable_returns_empty(self):
        result = _parse_option_symbol("WEIRD_SYMBOL")
        assert result == {}


class TestToDecimalEdgeCases:
    def test_whitespace_only(self):
        assert _to_decimal("   ") == Decimal("0")

    def test_newline_in_value(self):
        assert _to_decimal("1,234\n.56") == Decimal("1234.56")

    def test_non_numeric_returns_zero(self):
        assert _to_decimal("N/A") == Decimal("0")

    def test_precision_not_lost(self):
        """Financial figures must never be silently mutated."""
        val = _to_decimal("9.6783")
        assert val == Decimal("9.6783")
        assert str(val) == "9.6783"


class TestParseDatetimeEdgeCases:
    def test_semicolon_format(self):
        dt = _parse_datetime("2026-01-15;10:30:00")
        assert dt == datetime(2026, 1, 15, 10, 30, 0)

    def test_whitespace_padded(self):
        dt = _parse_datetime("  2026-01-15, 10:30:00  ")
        assert dt == datetime(2026, 1, 15, 10, 30, 0)


class TestIsTotal:
    def test_total(self):
        assert _is_total(["Total", "", "", ""])
    def test_total_symbol(self):
        assert _is_total(["Total AAPL", "", "", ""])
    def test_total_in_sgd(self):
        assert _is_total(["Total in SGD", "", "", ""])
    def test_not_total(self):
        assert not _is_total(["AAPL", "100", "1", ""])
    def test_empty_row(self):
        assert not _is_total([])
    def test_none_first_cell(self):
        assert not _is_total([None, "", ""])


# ── Metadata extraction ─────────────────────────────────────────────────────

class TestExtractMeta:
    def test_valid(self):
        rows = _account_info_rows() + _nav_rows()
        meta = _extract_meta(rows)
        assert meta.account_id == "U9876543"
        assert meta.base_currency == "SGD"
        assert meta.period_start == date(2026, 1, 1)
        assert meta.period_end == date(2026, 3, 6)

    def test_missing_account_raises(self):
        rows = _nav_rows()  # no account info
        with pytest.raises(ValueError, match="account ID"):
            _extract_meta(rows)

    def test_missing_nav_raises(self):
        rows = _account_info_rows()  # no NAV table
        with pytest.raises(ValueError, match="period"):
            _extract_meta(rows)


# ── Account splitting ────────────────────────────────────────────────────────

class TestSplitAccounts:
    def test_single_account(self):
        rows = _full_account_rows()
        groups = _split_accounts(rows)
        assert len(groups) == 1

    def test_multi_account(self):
        rows = _full_account_rows() + _account_info_rows("U1111111") + _nav_rows()
        groups = _split_accounts(rows)
        assert len(groups) == 2


# ── Position extraction ──────────────────────────────────────────────────────

class TestExtractPositions:
    def test_stock_positions(self):
        rows = _position_rows()
        positions, skipped = _extract_positions(rows, date(2026, 3, 6))
        stocks = [p for p in positions if p.asset_class == "STK"]
        assert len(stocks) == 2
        assert stocks[0].symbol == "AAPL"
        assert stocks[0].quantity == Decimal("100")
        assert stocks[0].market_value == Decimal("17550.00")

    def test_option_position(self):
        rows = _position_rows()
        positions, _ = _extract_positions(rows, date(2026, 3, 6))
        opts = [p for p in positions if p.asset_class == "OPT"]
        assert len(opts) == 1
        assert opts[0].expiry == date(2026, 3, 31)
        assert opts[0].strike == Decimal("48")
        assert opts[0].right == "C"

    def test_totals_skipped(self):
        rows = _position_rows()
        positions, _ = _extract_positions(rows, date(2026, 3, 6))
        assert len(positions) == 3  # 2 stocks + 1 option

    def test_unsupported_asset_class_skipped(self):
        extra = [
            ["Futures", "", "", "", "", "", "", "", ""],
            ["USD", "", "", "", "", "", "", "", ""],
            ["ESH4", "1", "1", "4800", "4800", "4850", "4850", "50", ""],
        ]
        rows = _position_rows() + extra
        positions, _ = _extract_positions(rows, date(2026, 3, 6))
        assert len(positions) == 3  # futures skipped

    def test_description_column_header_on_continuation_page(self):
        """IBKR PDFs use 'Description' instead of 'Symbol' on continuation pages.

        If the parser doesn't map 'Description' → symbol, all positions on
        that page are silently dropped because symbol="" → skipped.
        """
        rows = [
            ["Open Positions", "", "", "", "", "", "", "", ""],
            ["Symbol", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            ["Stocks", "", "", "", "", "", "", "", ""],
            ["USD", "", "", "", "", "", "", "", ""],
            ["AAPL", "100", "1", "150.00", "15,000.00",
             "175.50", "17,550.00", "2,550.00", ""],
            ["Total", "", "", "", "15,000.00", "", "17,550.00", "2,550.00", ""],
            # ── Page break: continuation uses "Description" header ──
            ["Description", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            ["Stocks", "", "", "", "", "", "", "", ""],
            ["USD", "", "", "", "", "", "", "", ""],
            ["SOFI", "200", "1", "10.00", "2,000.00",
             "15.25", "3,050.00", "1,050.00", ""],
            ["Total", "", "", "", "2,000.00", "", "3,050.00", "1,050.00", ""],
        ]
        positions, skipped = _extract_positions(rows, date(2026, 3, 6))
        symbols = [p.symbol for p in positions]
        assert "SOFI" in symbols, (
            f"SOFI missing from parsed positions: {symbols}. "
            "'Description' column header on continuation page causes symbol to not be mapped."
        )
        assert len(positions) == 2  # AAPL + SOFI

    def test_description_header_without_asset_class_reemission(self):
        """Continuation page with 'Description' header but NO asset-class
        sub-header re-emission — current_asset_class should be retained."""
        rows = [
            ["Open Positions", "", "", "", "", "", "", "", ""],
            ["Symbol", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            ["Stocks", "", "", "", "", "", "", "", ""],
            ["USD", "", "", "", "", "", "", "", ""],
            ["AAPL", "100", "1", "150.00", "15,000.00",
             "175.50", "17,550.00", "2,550.00", ""],
            # ── Page break: Description header, NO Stocks/USD re-emission ──
            ["Description", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            ["SOFI", "200", "1", "10.00", "2,000.00",
             "15.25", "3,050.00", "1,050.00", ""],
        ]
        positions, _ = _extract_positions(rows, date(2026, 3, 6))
        symbols = [p.symbol for p in positions]
        assert "SOFI" in symbols, (
            f"SOFI missing: {symbols}. Continuation page without asset-class "
            "re-emission should retain current_asset_class."
        )
        assert len(positions) == 2


    def test_many_positions_across_continuation_pages(self):
        """Simulate a real IBKR statement with 25+ stocks across pages.

        Page 1: Open Positions header, Stocks, first batch of symbols.
        Page 2: Continuation with "Description" header (no Stocks re-emission).
        All positions — including SOFI on page 2 — must be parsed.
        """
        rows = [
            ["Open Positions", "", "", "", "", "", "", "", ""],
            # Page 1 column header
            ["Symbol", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            ["Stocks", "", "", "", "", "", "", "", ""],
            ["USD", "", "", "", "", "", "", "", ""],
            # First batch of stocks
            ["AMZN", "35", "1", "226.30", "7,920.43",
             "213.21", "7,462.35", "-458.08", ""],
            ["GOOG", "20", "1", "313.80", "6,276.00",
             "289.59", "5,791.80", "-484.20", ""],
            ["META", "8", "1", "660.10", "5,280.80",
             "504.89", "4,039.12", "-1,241.68", ""],
            ["NFLX", "25", "1", "0.00", "0.00",
             "92.28", "2,307.00", "2,307.00", ""],
            ["PYPL", "20", "1", "58.38", "1,167.60",
             "0.00", "0.00", "-1,167.60", ""],
            ["RKLB", "10", "1", "69.76", "697.60",
             "0.00", "0.00", "-697.60", ""],
            ["Total", "", "", "", "21,342.43", "", "19,600.27", "-1,742.16", ""],
            # ── Page break: continuation with Description header ──
            ["Description", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            # No "Stocks" re-emission — retains STK from page 1
            ["SNDK", "0", "1", "0.00", "0.00",
             "0.00", "0.00", "0.00", ""],
            ["SOFI", "400", "1", "26.18", "10,472.00",
             "16.56", "6,624.00", "-3,848.00", ""],
            ["SPGI", "6", "1", "0.00", "0.00",
             "408.48", "2,450.88", "2,450.88", ""],
            ["VALE", "100", "1", "0.00", "0.00",
             "15.14", "1,514.00", "1,514.00", ""],
            ["XLE", "40", "1", "0.00", "0.00",
             "60.57", "2,422.80", "2,422.80", ""],
            ["Total", "", "", "", "10,472.00", "", "13,011.68", "2,539.68", ""],
            ["Total in SGD", "", "", "", "", "", "30,000.00", "800.00", ""],
            # Options follow
            ["Symbol", "Quantity", "Mult", "Cost Price", "Cost Basis",
             "Close Price", "Value", "Unrealized P/L", "Code"],
            ["Equity and Index Options", "", "", "", "", "", "", "", ""],
            ["USD", "", "", "", "", "", "", "", ""],
            ["EEM 31MAR26 48 C", "6", "100", "7.42", "4,454.46",
             "9.47", "5,680.92", "1,226.46", ""],
            ["Total", "", "", "", "4,454.46", "", "5,680.92", "1,226.46", ""],
        ]
        positions, skipped = _extract_positions(rows, date(2026, 3, 25))
        symbols = [p.symbol for p in positions]

        # All positions must be present — especially SOFI
        assert "SOFI" in symbols, (
            f"SOFI missing from parsed positions: {symbols}"
        )
        assert "AMZN" in symbols
        assert "VALE" in symbols
        assert "XLE" in symbols
        # 6 stocks page 1 + 5 stocks page 2 + 1 option = 12
        assert len(positions) == 12
        # SOFI values must match PDF
        sofi = [p for p in positions if p.symbol == "SOFI"][0]
        assert sofi.quantity == Decimal("400")
        assert sofi.cost_basis == Decimal("10472.00")
        assert sofi.market_value == Decimal("6624.00")
        assert sofi.asset_class == "STK"


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
        assert sells[0].realized_pnl == Decimal("1123.91")

    def test_newline_datetime_parsed(self):
        rows = _trade_rows()
        trades, _ = _extract_trades(rows)
        assert trades[0].trade_date == datetime(2026, 1, 15, 10, 30, 0)

    def test_total_rows_skipped(self):
        rows = _trade_rows()
        trades, _ = _extract_trades(rows)
        assert len(trades) == 2  # only data rows, not Total AAPL etc.


# ── End-to-end: parse_statement with mocked extraction ───────────────────────

class TestParseStatement:
    def test_single_account(self):
        all_rows = _full_account_rows()
        with patch("src.parser._extract_tables", return_value=all_rows):
            results = parse_statement(io.BytesIO(b"fake"))

        assert len(results) == 1
        r = results[0]
        assert r.meta.account_id == "U9876543"
        assert len(r.positions) == 3
        assert len(r.trades) == 2

    def test_multi_account(self):
        rows1 = _full_account_rows()
        rows2 = _account_info_rows("U1111111") + _nav_rows() + _position_rows()
        with patch("src.parser._extract_tables", return_value=rows1 + rows2):
            results = parse_statement(io.BytesIO(b"fake"))

        assert len(results) == 2
        assert results[0].meta.account_id == "U9876543"
        assert results[1].meta.account_id == "U1111111"

    def test_empty_pdf_raises(self):
        with patch("src.parser._extract_tables", return_value=[]):
            with pytest.raises(ValueError, match="No tables found"):
                parse_statement(io.BytesIO(b"fake"))

    def test_financial_precision(self):
        all_rows = _full_account_rows()
        with patch("src.parser._extract_tables", return_value=all_rows):
            results = parse_statement(io.BytesIO(b"fake"))

        aapl = [p for p in results[0].positions if p.symbol == "AAPL"][0]
        assert aapl.cost_basis == Decimal("15000.00")
        assert aapl.unrealized_pnl == Decimal("2550.00")


# ── Integration test with real PDF ───────────────────────────────────────────

FIXTURE_PATH = "tests/fixtures/MULTI_20260101_20260306.pdf"


@pytest.fixture
def real_pdf():
    """Load the real IBKR fixture PDF if available."""
    try:
        return open(FIXTURE_PATH, "rb")
    except FileNotFoundError:
        pytest.skip("Fixture PDF not available")


class TestRealPDF:
    def test_parses_two_accounts(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        assert len(results) == 2

    def test_account_ids(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        ids = {r.meta.account_id for r in results}
        assert ids == {"U10278751", "U12890661"}

    def test_period(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        for r in results:
            assert r.meta.period_start == date(2026, 1, 1)
            assert r.meta.period_end == date(2026, 3, 6)

    def test_account1_positions(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        acct1 = [r for r in results if r.meta.account_id == "U10278751"][0]
        assert len(acct1.positions) == 14  # 13 stocks + 1 option
        stocks = [p for p in acct1.positions if p.asset_class == "STK"]
        opts = [p for p in acct1.positions if p.asset_class == "OPT"]
        assert len(stocks) == 13
        assert len(opts) == 1
        assert opts[0].symbol == "EEM 31MAR26 48 C"
        assert opts[0].expiry == date(2026, 3, 31)
        assert opts[0].strike == Decimal("48")

    def test_account1_trades(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        acct1 = [r for r in results if r.meta.account_id == "U10278751"][0]
        assert len(acct1.trades) == 35
        stk_trades = [t for t in acct1.trades if t.asset_class == "STK"]
        opt_trades = [t for t in acct1.trades if t.asset_class == "OPT"]
        assert len(stk_trades) == 29
        assert len(opt_trades) == 6

    def test_account2_positions(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        acct2 = [r for r in results if r.meta.account_id == "U12890661"][0]
        assert len(acct2.positions) == 13  # all stocks, no options
        assert all(p.asset_class == "STK" for p in acct2.positions)

    def test_account2_trades(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        acct2 = [r for r in results if r.meta.account_id == "U12890661"][0]
        assert len(acct2.trades) == 50
        opt_trades = [t for t in acct2.trades if t.asset_class == "OPT"]
        assert len(opt_trades) == 4

    def test_no_skipped_rows(self, real_pdf):
        results = parse_statement(real_pdf)
        real_pdf.close()
        for r in results:
            assert r.skipped_rows == []

    def test_specific_position_values(self, real_pdf):
        """Verify exact financial values from PDF aren't mutated."""
        results = parse_statement(real_pdf)
        real_pdf.close()
        acct1 = [r for r in results if r.meta.account_id == "U10278751"][0]
        aapl = [p for p in acct1.positions if p.symbol == "AMZN"][0]
        assert aapl.quantity == Decimal("35")
        assert aapl.cost_basis == Decimal("7920.43")
        assert aapl.market_price == Decimal("213.2100")
        assert aapl.market_value == Decimal("7462.35")
        assert aapl.unrealized_pnl == Decimal("-458.08")

    def test_specific_trade_values(self, real_pdf):
        """Verify exact trade values from PDF."""
        results = parse_statement(real_pdf)
        real_pdf.close()
        acct1 = [r for r in results if r.meta.account_id == "U10278751"][0]
        # Find the EWY sell trade
        ewy_sell = [t for t in acct1.trades
                     if t.symbol == "EWY" and t.side == "SLD"][0]
        assert ewy_sell.trade_date == datetime(2026, 3, 3, 9, 30, 0)
        assert ewy_sell.quantity == Decimal("30")
        assert ewy_sell.price == Decimal("130.0400")
        assert ewy_sell.realized_pnl == Decimal("380.91")
        assert ewy_sell.commission == Decimal("-1.10")
