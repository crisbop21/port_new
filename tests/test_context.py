"""Tests for src/context.py — portfolio context assembly for the advisor."""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pytest

from src.context import (
    _compute_dte,
    _compute_moneyness,
    _extract_underlying,
    _format_number,
    build_position_context,
    serialize_context,
)


# ── Unit helpers ─────────────────────────────────────────────────────────────


class TestComputeDTE:
    def test_future_expiry(self):
        today = date(2026, 3, 25)
        assert _compute_dte(date(2026, 4, 1), today) == 7

    def test_today_expiry(self):
        today = date(2026, 3, 25)
        assert _compute_dte(today, today) == 0

    def test_past_expiry(self):
        today = date(2026, 3, 25)
        assert _compute_dte(date(2026, 3, 20), today) == -5

    def test_none_expiry(self):
        assert _compute_dte(None, date(2026, 3, 25)) is None


class TestComputeMoneyness:
    def test_itm_call(self):
        # stock at 150, strike 100 → 50% ITM
        result = _compute_moneyness(Decimal("100"), Decimal("150"), "C")
        assert result == pytest.approx(0.5, abs=0.001)

    def test_otm_call(self):
        # stock at 80, strike 100 → -20% OTM
        result = _compute_moneyness(Decimal("100"), Decimal("80"), "C")
        assert result == pytest.approx(-0.2, abs=0.001)

    def test_itm_put(self):
        # stock at 80, strike 100 → (100-80)/100 = 20% ITM
        result = _compute_moneyness(Decimal("100"), Decimal("80"), "P")
        assert result == pytest.approx(0.2, abs=0.001)

    def test_otm_put(self):
        # stock at 150, strike 100 → (100-150)/100 = -50% OTM
        result = _compute_moneyness(Decimal("100"), Decimal("150"), "P")
        assert result == pytest.approx(-0.5, abs=0.001)

    def test_atm(self):
        result = _compute_moneyness(Decimal("100"), Decimal("100"), "C")
        assert result == pytest.approx(0.0, abs=0.001)

    def test_zero_strike(self):
        assert _compute_moneyness(Decimal("0"), Decimal("100"), "C") is None

    def test_none_inputs(self):
        assert _compute_moneyness(None, Decimal("100"), "C") is None
        assert _compute_moneyness(Decimal("100"), None, "C") is None


class TestExtractUnderlying:
    def test_stock_symbol(self):
        assert _extract_underlying("AAPL", "STK") == "AAPL"

    def test_option_symbol_space_format(self):
        assert _extract_underlying("AAPL 20240119 150.0 C", "OPT") == "AAPL"

    def test_option_symbol_ddmmmyy(self):
        assert _extract_underlying("PYPL 18SEP26 87.5 P", "OPT") == "PYPL"

    def test_etf(self):
        assert _extract_underlying("SPY", "ETF") == "SPY"

    def test_option_no_space(self):
        # Edge case: single-word option symbol
        assert _extract_underlying("AAPL", "OPT") == "AAPL"


class TestFormatNumber:
    def test_large_number(self):
        assert _format_number(1_500_000_000) == "1.50B"

    def test_millions(self):
        assert _format_number(25_300_000) == "25.30M"

    def test_small_number(self):
        assert _format_number(1234.56) == "1,234.56"

    def test_none(self):
        assert _format_number(None) == "N/A"

    def test_zero(self):
        assert _format_number(0) == "0.00"


# ── Context assembly (integration-style with mocks) ─────────────────────────


class TestBuildPositionContext:
    """Test context assembly with mocked DB calls."""

    @patch("src.context.get_daily_prices")
    @patch("src.context.get_latest_price")
    @patch("src.context.get_latest_stock_metrics")
    @patch("src.context.get_trades")
    @patch("src.context.get_positions_as_of")
    @patch("src.context.get_snapshot_dates")
    @patch("src.context.get_account_ids")
    def test_builds_context_for_option_position(
        self,
        mock_account_ids,
        mock_snapshot_dates,
        mock_positions,
        mock_trades,
        mock_metrics,
        mock_latest_price,
        mock_daily_prices,
    ):
        mock_account_ids.return_value = ["U1234"]
        mock_snapshot_dates.return_value = [date(2026, 3, 20)]
        mock_positions.return_value = [
            {
                "symbol": "AAPL 20260417 200.0 C",
                "asset_class": "OPT",
                "quantity": "5",
                "cost_basis": "2500",
                "market_price": "3.50",
                "market_value": "1750",
                "unrealized_pnl": "-750",
                "expiry": "2026-04-17",
                "strike": "200",
                "right": "C",
                "statement_date": "2026-03-20",
            },
        ]
        mock_trades.return_value = []
        mock_metrics.return_value = {
            "revenue": {"metric_value": "400000000000", "period_end": "2025-12-31"},
            "eps_diluted": {"metric_value": "6.50", "period_end": "2025-12-31"},
        }
        mock_latest_price.return_value = {
            "close": "195.00",
            "price_date": "2026-03-24",
        }
        mock_daily_prices.return_value = []

        ctx = build_position_context("U1234")

        assert "positions" in ctx
        assert len(ctx["positions"]) == 1

        pos = ctx["positions"][0]
        assert pos["symbol"] == "AAPL 20260417 200.0 C"
        assert pos["underlying"] == "AAPL"
        assert pos["dte"] == 23  # Apr 17 - Mar 25
        assert pos["right"] == "C"
        assert pos["strike"] == Decimal("200")

    @patch("src.context.get_daily_prices")
    @patch("src.context.get_latest_price")
    @patch("src.context.get_latest_stock_metrics")
    @patch("src.context.get_trades")
    @patch("src.context.get_positions_as_of")
    @patch("src.context.get_snapshot_dates")
    @patch("src.context.get_account_ids")
    def test_includes_stock_positions(
        self,
        mock_account_ids,
        mock_snapshot_dates,
        mock_positions,
        mock_trades,
        mock_metrics,
        mock_latest_price,
        mock_daily_prices,
    ):
        mock_account_ids.return_value = ["U1234"]
        mock_snapshot_dates.return_value = [date(2026, 3, 20)]
        mock_positions.return_value = [
            {
                "symbol": "MSFT",
                "asset_class": "STK",
                "quantity": "100",
                "cost_basis": "38000",
                "market_price": "400",
                "market_value": "40000",
                "unrealized_pnl": "2000",
                "expiry": None,
                "strike": None,
                "right": None,
                "statement_date": "2026-03-20",
            },
        ]
        mock_trades.return_value = []
        mock_metrics.return_value = {}
        mock_latest_price.return_value = {"close": "400", "price_date": "2026-03-24"}
        mock_daily_prices.return_value = []

        ctx = build_position_context("U1234")

        assert len(ctx["positions"]) == 1
        pos = ctx["positions"][0]
        assert pos["underlying"] == "MSFT"
        assert pos["dte"] is None  # stocks have no expiry
        assert pos["moneyness"] is None

    @patch("src.context.get_daily_prices")
    @patch("src.context.get_latest_price")
    @patch("src.context.get_latest_stock_metrics")
    @patch("src.context.get_trades")
    @patch("src.context.get_positions_as_of")
    @patch("src.context.get_snapshot_dates")
    @patch("src.context.get_account_ids")
    def test_vol_override_applied(
        self,
        mock_account_ids,
        mock_snapshot_dates,
        mock_positions,
        mock_trades,
        mock_metrics,
        mock_latest_price,
        mock_daily_prices,
    ):
        mock_account_ids.return_value = ["U1234"]
        mock_snapshot_dates.return_value = [date(2026, 3, 20)]
        mock_positions.return_value = [
            {
                "symbol": "AAPL 20260417 200.0 C",
                "asset_class": "OPT",
                "quantity": "5",
                "cost_basis": "2500",
                "market_price": "3.50",
                "market_value": "1750",
                "unrealized_pnl": "-750",
                "expiry": "2026-04-17",
                "strike": "200",
                "right": "C",
                "statement_date": "2026-03-20",
            },
        ]
        mock_trades.return_value = []
        mock_metrics.return_value = {}
        mock_latest_price.return_value = {"close": "195", "price_date": "2026-03-24"}
        mock_daily_prices.return_value = []

        vol_overrides = {"AAPL": 0.35}
        ctx = build_position_context("U1234", vol_overrides=vol_overrides)

        # The underlying context should use the override
        assert ctx["underlyings"]["AAPL"]["volatility_override"] == 0.35

    @patch("src.context.get_daily_prices")
    @patch("src.context.get_latest_price")
    @patch("src.context.get_latest_stock_metrics")
    @patch("src.context.get_trades")
    @patch("src.context.get_positions_as_of")
    @patch("src.context.get_snapshot_dates")
    @patch("src.context.get_account_ids")
    def test_empty_portfolio(
        self,
        mock_account_ids,
        mock_snapshot_dates,
        mock_positions,
        mock_trades,
        mock_metrics,
        mock_latest_price,
        mock_daily_prices,
    ):
        mock_account_ids.return_value = ["U1234"]
        mock_snapshot_dates.return_value = [date(2026, 3, 20)]
        mock_positions.return_value = []
        mock_trades.return_value = []
        mock_metrics.return_value = {}
        mock_latest_price.return_value = None
        mock_daily_prices.return_value = []

        ctx = build_position_context("U1234")
        assert ctx["positions"] == []
        assert ctx["underlyings"] == {}


# ── Serialization ───────────────────────────────────────────────────────────


class TestSerializeContext:
    def test_serializes_to_string_with_tables(self):
        ctx = {
            "account_id": "U1234",
            "as_of_date": "2026-03-20",
            "positions": [
                {
                    "symbol": "AAPL 20260417 200.0 C",
                    "underlying": "AAPL",
                    "asset_class": "OPT",
                    "quantity": 5,
                    "cost_basis": 2500,
                    "market_value": 1750,
                    "unrealized_pnl": -750,
                    "dte": 23,
                    "strike": Decimal("200"),
                    "right": "C",
                    "moneyness": -0.025,
                    "breakeven": Decimal("205.0"),
                    "expiry": "2026-04-17",
                },
            ],
            "underlyings": {
                "AAPL": {
                    "current_price": 195.0,
                    "realized_vol_20d": 0.28,
                    "volatility_override": None,
                    "fundamentals": {"revenue": "400.00B", "eps_diluted": "6.50"},
                    "valuation": {
                        "pe_ttm": "22.5", "gross_margin": "0.45",
                        "score_composite": "65", "revenue_growth": "0.08",
                    },
                    "technical_signals": {
                        "raw": {"rsi_14": 55.0, "sma_trend": 0.05},
                        "scores": {"rsi_14": 45.0, "sma_trend": 60.0},
                    },
                    "ma_flags": {"above_sma50": True, "above_sma200": True},
                    "recent_trades": [],
                },
            },
        }

        result = serialize_context(ctx)
        assert isinstance(result, str)
        assert "AAPL" in result
        # Table format checks
        assert "| Underlying |" in result or "| Type |" in result  # options table header
        assert "| 23 |" in result  # DTE in table cell
        assert "| 200 |" in result  # strike in table cell
        assert "OTM" in result
        # Fundamental table
        assert "Fundamental Evaluation" in result
        assert "P/E (TTM)" in result
        assert "22.5" in result
        # Technical table
        assert "Technical Evaluation" in result
        assert "RSI" in result
        assert "SMA" in result

    def test_includes_vol_override(self):
        ctx = {
            "account_id": "U1234",
            "as_of_date": "2026-03-20",
            "positions": [],
            "underlyings": {
                "AAPL": {
                    "current_price": 195.0,
                    "realized_vol_20d": 0.28,
                    "volatility_override": 0.35,
                    "fundamentals": {},
                    "valuation": {},
                    "technical_signals": {},
                    "ma_flags": {},
                    "recent_trades": [],
                },
            },
        }

        result = serialize_context(ctx)
        assert "override" in result.lower()
        assert "35" in result

    def test_stock_position_table(self):
        ctx = {
            "account_id": "U1234",
            "as_of_date": "2026-03-20",
            "positions": [
                {
                    "symbol": "MSFT",
                    "underlying": "MSFT",
                    "asset_class": "STK",
                    "quantity": 100,
                    "cost_basis": 38000,
                    "market_value": 40000,
                    "unrealized_pnl": 2000,
                    "dte": None,
                    "strike": None,
                    "right": None,
                    "moneyness": None,
                    "breakeven": None,
                },
            ],
            "underlyings": {},
        }

        result = serialize_context(ctx)
        assert "Stock/ETF Positions" in result
        assert "| MSFT |" in result

    def test_valuation_scores_in_table(self):
        ctx = {
            "account_id": "U1234",
            "as_of_date": "2026-03-20",
            "positions": [],
            "underlyings": {
                "AAPL": {
                    "current_price": 195.0,
                    "realized_vol_20d": None,
                    "volatility_override": None,
                    "fundamentals": {},
                    "valuation": {
                        "score_composite": "72",
                        "score_valuation": "60",
                        "score_profitability": "85",
                    },
                    "technical_signals": {},
                    "ma_flags": {},
                    "recent_trades": [],
                },
            },
        }

        result = serialize_context(ctx)
        assert "72.00/100" in result
        assert "60.00/100" in result
        assert "85.00/100" in result
