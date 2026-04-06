"""Tests for PDF holdings export."""

import io
from datetime import date

import pandas as pd
import pytest

from src.pdf_holdings import generate_holdings_pdf


@pytest.fixture
def sample_positions():
    """Minimal position rows as returned by get_positions_as_of + enrichment."""
    return [
        {
            "symbol": "AAPL",
            "asset_class": "STK",
            "quantity": 100,
            "cost_basis": 15000.00,
            "market_price": 175.00,
            "market_value": 17500.00,
            "unrealized_pnl": 2500.00,
            "multiplier": 1,
            "cost_value": 15000.00,
        },
        {
            "symbol": "QQQ",
            "asset_class": "ETF",
            "quantity": 50,
            "cost_basis": 18000.00,
            "market_price": 380.00,
            "market_value": 19000.00,
            "unrealized_pnl": 1000.00,
            "multiplier": 1,
            "cost_value": 18000.00,
        },
        {
            "symbol": "AAPL 20260417 200 C",
            "asset_class": "OPT",
            "quantity": 5,
            "cost_basis": 2500.00,
            "market_price": 3.50,
            "market_value": 1750.00,
            "unrealized_pnl": -750.00,
            "multiplier": 100,
            "cost_value": 2500.00,
            "expiry": date(2026, 4, 17),
            "strike": 200.00,
            "right": "C",
        },
    ]


@pytest.fixture
def holdings_df(sample_positions):
    df = pd.DataFrame(sample_positions)
    numeric_cols = [
        "quantity", "cost_basis", "cost_value",
        "market_price", "market_value", "unrealized_pnl", "strike",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


class TestGenerateHoldingsPdf:
    def test_returns_bytes(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_pdf_header(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        # PDF files start with %PDF
        assert result[:5] == b"%PDF-"

    def test_contains_account_info(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        # The PDF text should contain account and date info
        text = result.decode("latin-1")
        assert "U1234567" in text
        assert "2026-04-04" in text

    def test_contains_symbols(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        text = result.decode("latin-1")
        assert "AAPL" in text
        assert "QQQ" in text

    def test_contains_summary_metrics(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        text = result.decode("latin-1")
        # Total market value = 17500 + 19000 + 1750 = 38250
        assert "38,250" in text

    def test_all_accounts_label(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="All Accounts",
            as_of=date(2026, 4, 4),
        )
        text = result.decode("latin-1")
        assert "All Accounts" in text

    def test_groups_by_asset_class(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        text = result.decode("latin-1")
        assert "Stocks" in text
        assert "ETFs" in text
        assert "Options" in text

    def test_no_market_data(self):
        """When market_value is absent, should still produce a valid PDF."""
        rows = [
            {
                "symbol": "MSFT",
                "asset_class": "STK",
                "quantity": 10,
                "cost_basis": 3000.00,
                "multiplier": 1,
                "cost_value": 3000.00,
            },
        ]
        df = pd.DataFrame(rows)
        result = generate_holdings_pdf(
            df=df,
            account_id="U9999999",
            as_of=date(2026, 1, 1),
        )
        assert result[:5] == b"%PDF-"
        text = result.decode("latin-1")
        assert "MSFT" in text

    def test_option_fields_included(self, holdings_df):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
        )
        text = result.decode("latin-1")
        # Option strike and right should appear
        assert "200" in text
        assert "C" in text


@pytest.fixture
def beta_result():
    """Beta result dict as stored in st.session_state."""
    return {
        "portfolio_beta": 1.15,
        "portfolio_dollar_beta": 44000.0,
        "betas": {"AAPL": 1.25, "QQQ": 1.02},
        "dollar_betas": {"AAPL": 21875.0, "QQQ": 19380.0},
        "position_betas": [
            {"symbol": "AAPL", "effective_beta": 1.25, "market_value": 17500.0},
            {"symbol": "QQQ", "effective_beta": 1.02, "market_value": 19000.0},
            {"symbol": "AAPL 20260417 200 C", "effective_beta": 5.5, "market_value": 1750.0},
        ],
    }


class TestPdfWithBeta:
    def test_portfolio_beta_in_summary(self, holdings_df, beta_result):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
            beta_result=beta_result,
            beta_benchmark="SPY",
        )
        text = result.decode("latin-1")
        assert "1.15" in text
        assert "SPY" in text

    def test_portfolio_dollar_beta_in_summary(self, holdings_df, beta_result):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
            beta_result=beta_result,
            beta_benchmark="SPY",
        )
        text = result.decode("latin-1")
        assert "44,000" in text

    def test_per_symbol_betas(self, holdings_df, beta_result):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
            beta_result=beta_result,
            beta_benchmark="SPY",
        )
        text = result.decode("latin-1")
        assert "1.25" in text  # AAPL beta
        assert "1.02" in text  # QQQ beta

    def test_beta_section_header(self, holdings_df, beta_result):
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
            beta_result=beta_result,
            beta_benchmark="SPY",
        )
        text = result.decode("latin-1")
        assert "Portfolio Beta" in text

    def test_no_beta_no_crash(self, holdings_df):
        """When beta_result is None, PDF should still generate without beta section."""
        result = generate_holdings_pdf(
            df=holdings_df,
            account_id="U1234567",
            as_of=date(2026, 4, 4),
            beta_result=None,
            beta_benchmark=None,
        )
        text = result.decode("latin-1")
        assert "Portfolio Beta" not in text
        assert result[:5] == b"%PDF-"
