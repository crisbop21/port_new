"""Tests for src/fetcher.py — SEC EDGAR data fetcher.

All HTTP calls are mocked; no real network requests are made.
"""

import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.fetcher import (
    _extract_fact_values,
    _pick_latest_annual,
    clear_cik_cache,
    fetch_metrics_for_symbol,
    get_cik,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_cache():
    """Ensure a fresh CIK cache for each test."""
    clear_cik_cache()
    yield
    clear_cik_cache()


SAMPLE_CIK_MAP = {
    "0": {"ticker": "AAPL", "cik_str": 320193},
    "1": {"ticker": "MSFT", "cik_str": 789019},
}


def _mock_sec_response(data, status_code=200):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.text = json.dumps(data) if isinstance(data, dict) else str(data)
    return resp


SAMPLE_COMPANY_FACTS = {
    "entityName": "Apple Inc.",
    "cik": 320193,
    "facts": {
        "us-gaap": {
            "Revenues": {
                "units": {
                    "USD": [
                        {"val": 394328000000, "end": "2024-09-28", "form": "10-K", "fy": 2024, "fp": "FY"},
                        {"val": 383285000000, "end": "2023-09-30", "form": "10-K", "fy": 2023, "fp": "FY"},
                    ]
                }
            },
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        {"val": 93736000000, "end": "2024-09-28", "form": "10-K", "fy": 2024, "fp": "FY"},
                    ]
                }
            },
            "EarningsPerShareDiluted": {
                "units": {
                    "USD/shares": [
                        {"val": 6.08, "end": "2024-09-28", "form": "10-K", "fy": 2024, "fp": "FY"},
                    ]
                }
            },
            "Assets": {
                "units": {
                    "USD": [
                        {"val": 364980000000, "end": "2024-09-28", "form": "10-K", "fy": 2024, "fp": "FY"},
                    ]
                }
            },
        },
        "dei": {},
    },
}


# ── CIK lookup tests ────────────────────────────────────────────────────────


class TestGetCik:
    @patch("src.fetcher._get_session")
    def test_found(self, mock_session_fn):
        session = MagicMock()
        session.get.return_value = _mock_sec_response(SAMPLE_CIK_MAP)
        mock_session_fn.return_value = session

        assert get_cik("AAPL") == "0000320193"

    @patch("src.fetcher._get_session")
    def test_not_found(self, mock_session_fn):
        session = MagicMock()
        session.get.return_value = _mock_sec_response(SAMPLE_CIK_MAP)
        mock_session_fn.return_value = session

        assert get_cik("ZZZZ") is None

    @patch("src.fetcher._get_session")
    def test_case_insensitive(self, mock_session_fn):
        session = MagicMock()
        session.get.return_value = _mock_sec_response(SAMPLE_CIK_MAP)
        mock_session_fn.return_value = session

        assert get_cik("aapl") == "0000320193"
        assert get_cik("Msft") == "0000789019"

    @patch("src.fetcher._get_session")
    def test_network_failure_returns_none(self, mock_session_fn):
        session = MagicMock()
        session.get.return_value = _mock_sec_response({}, status_code=500)
        mock_session_fn.return_value = session

        assert get_cik("AAPL") is None


# ── Fact extraction tests ────────────────────────────────────────────────────


class TestExtractFactValues:
    def test_extracts_usd_values(self):
        values = _extract_fact_values(SAMPLE_COMPANY_FACTS, "us-gaap:Revenues")
        assert len(values) == 2
        assert values[0]["val"] == 394328000000

    def test_missing_tag_returns_empty(self):
        values = _extract_fact_values(SAMPLE_COMPANY_FACTS, "us-gaap:NonexistentTag")
        assert values == []

    def test_missing_taxonomy_returns_empty(self):
        values = _extract_fact_values(SAMPLE_COMPANY_FACTS, "ifrs-full:SomeTag")
        assert values == []

    def test_usd_per_shares_unit(self):
        values = _extract_fact_values(SAMPLE_COMPANY_FACTS, "us-gaap:EarningsPerShareDiluted")
        assert len(values) == 1
        assert values[0]["val"] == 6.08


class TestPickLatestAnnual:
    def test_picks_most_recent_10k(self):
        values = [
            {"val": 100, "end": "2023-12-31", "form": "10-K"},
            {"val": 200, "end": "2024-12-31", "form": "10-K"},
            {"val": 50, "end": "2024-06-30", "form": "10-Q"},
        ]
        result = _pick_latest_annual(values)
        assert result["val"] == 200

    def test_falls_back_to_10q(self):
        values = [
            {"val": 50, "end": "2024-06-30", "form": "10-Q"},
            {"val": 30, "end": "2024-03-31", "form": "10-Q"},
        ]
        result = _pick_latest_annual(values)
        assert result["val"] == 50

    def test_empty_returns_none(self):
        assert _pick_latest_annual([]) is None

    def test_fallback_to_any_form(self):
        values = [{"val": 99, "end": "2024-01-01", "form": "8-K"}]
        result = _pick_latest_annual(values)
        assert result["val"] == 99


# ── Full orchestrator tests ──────────────────────────────────────────────────


class TestFetchMetricsForSymbol:
    @patch("src.fetcher._get_session")
    def test_success(self, mock_session_fn):
        session = MagicMock()
        # First call: CIK map; second call: company facts
        session.get.side_effect = [
            _mock_sec_response(SAMPLE_CIK_MAP),
            _mock_sec_response(SAMPLE_COMPANY_FACTS),
        ]
        mock_session_fn.return_value = session

        metrics, errors = fetch_metrics_for_symbol("AAPL")

        assert len(metrics) >= 3  # revenue, net_income, eps_diluted, total_assets
        assert all(m.symbol == "AAPL" for m in metrics)
        assert all(m.cik == "0000320193" for m in metrics)
        assert all(m.source == "SEC_EDGAR" for m in metrics)

        # Check specific metric
        revenue = next((m for m in metrics if m.metric_name == "revenue"), None)
        assert revenue is not None
        assert revenue.metric_value == Decimal("394328000000")
        assert revenue.period_end == date(2024, 9, 28)

    @patch("src.fetcher._get_session")
    def test_unknown_ticker_returns_empty(self, mock_session_fn):
        session = MagicMock()
        session.get.return_value = _mock_sec_response(SAMPLE_CIK_MAP)
        mock_session_fn.return_value = session

        metrics, errors = fetch_metrics_for_symbol("ZZZZ")

        assert metrics == []
        assert len(errors) == 1
        assert "no CIK found" in errors[0]

    @patch("src.fetcher._get_session")
    def test_facts_fetch_failure(self, mock_session_fn):
        session = MagicMock()
        session.get.side_effect = [
            _mock_sec_response(SAMPLE_CIK_MAP),
            _mock_sec_response({}, status_code=500),
        ]
        mock_session_fn.return_value = session

        metrics, errors = fetch_metrics_for_symbol("AAPL")

        assert metrics == []
        assert len(errors) == 1
        assert "failed to fetch" in errors[0].lower()

    @patch("src.fetcher._get_session")
    def test_normalises_symbol_case(self, mock_session_fn):
        session = MagicMock()
        session.get.side_effect = [
            _mock_sec_response(SAMPLE_CIK_MAP),
            _mock_sec_response(SAMPLE_COMPANY_FACTS),
        ]
        mock_session_fn.return_value = session

        metrics, _ = fetch_metrics_for_symbol("aapl")
        assert all(m.symbol == "AAPL" for m in metrics)

    @patch("src.fetcher._get_session")
    def test_partial_data_still_returns_what_it_can(self, mock_session_fn):
        """If some metrics are missing, others should still be extracted."""
        sparse_facts = {
            "entityName": "Test Co",
            "cik": 320193,
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [{"val": 100, "end": "2024-01-01", "form": "10-K"}]
                        }
                    },
                    # Everything else missing
                },
                "dei": {},
            },
        }
        session = MagicMock()
        session.get.side_effect = [
            _mock_sec_response(SAMPLE_CIK_MAP),
            _mock_sec_response(sparse_facts),
        ]
        mock_session_fn.return_value = session

        metrics, errors = fetch_metrics_for_symbol("AAPL")
        assert len(metrics) == 1
        assert metrics[0].metric_name == "revenue"
        assert len(errors) == 0  # missing metrics are not errors, just debug logs
