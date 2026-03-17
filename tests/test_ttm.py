"""Tests for TTM (Trailing Twelve Months) computation."""

from src.ttm import compute_ttm, compute_ttm_latest, is_flow_metric


def _row(period_end: str, value: float, fp: str, form: str = "10-Q") -> dict:
    return {
        "symbol": "AAPL",
        "metric_name": "revenue",
        "metric_value": value,
        "period_end": period_end,
        "fiscal_period": fp,
        "filing_type": form,
    }


class TestIsFlowMetric:
    def test_revenue_is_flow(self):
        assert is_flow_metric("revenue") is True

    def test_eps_is_flow(self):
        assert is_flow_metric("eps_diluted") is True

    def test_total_assets_is_not_flow(self):
        assert is_flow_metric("total_assets") is False

    def test_shares_outstanding_is_not_flow(self):
        assert is_flow_metric("shares_outstanding") is False


class TestComputeTTM:
    def test_empty(self):
        assert compute_ttm([]) == []

    def test_annual_passthrough(self):
        """FY values should pass through as-is."""
        rows = [_row("2023-12-31", 100000, "FY", "10-K")]
        result = compute_ttm(rows)
        assert len(result) == 1
        assert result[0]["ttm_value"] == 100000
        assert result[0]["ttm_method"] == "annual"
        assert result[0]["is_ytd"] is False

    def test_q1_ttm(self):
        """Q1 TTM = Q1_current + FY_prior - Q1_prior."""
        rows = [
            _row("2022-03-31", 20000, "Q1"),    # prior Q1
            _row("2022-12-31", 100000, "FY", "10-K"),  # prior FY
            _row("2023-03-31", 28000, "Q1"),     # current Q1
        ]
        result = compute_ttm(rows)
        q1_current = [r for r in result if r["period_end"] == "2023-03-31"][0]
        # TTM = 28000 + 100000 - 20000 = 108000
        assert q1_current["ttm_value"] == 108000
        assert q1_current["ttm_method"] == "computed"
        assert q1_current["is_ytd"] is True

    def test_q2_ttm(self):
        """Q2 TTM = Q2_YTD_current + FY_prior - Q2_YTD_prior."""
        rows = [
            _row("2022-06-30", 48000, "Q2"),     # prior Q2 (6-month YTD)
            _row("2022-12-31", 100000, "FY", "10-K"),
            _row("2023-06-30", 55000, "Q2"),     # current Q2 (6-month YTD)
        ]
        result = compute_ttm(rows)
        q2_current = [r for r in result if r["period_end"] == "2023-06-30"][0]
        # TTM = 55000 + 100000 - 48000 = 107000
        assert q2_current["ttm_value"] == 107000
        assert q2_current["ttm_method"] == "computed"

    def test_q3_ttm(self):
        """Q3 TTM = Q3_YTD_current + FY_prior - Q3_YTD_prior."""
        rows = [
            _row("2022-09-30", 72000, "Q3"),     # prior Q3 (9-month YTD)
            _row("2022-12-31", 100000, "FY", "10-K"),
            _row("2023-09-30", 80000, "Q3"),     # current Q3 (9-month YTD)
        ]
        result = compute_ttm(rows)
        q3_current = [r for r in result if r["period_end"] == "2023-09-30"][0]
        # TTM = 80000 + 100000 - 72000 = 108000
        assert q3_current["ttm_value"] == 108000
        assert q3_current["ttm_method"] == "computed"

    def test_q4_treated_as_annual(self):
        """Q4 from a 10-Q is treated as annual (full-year YTD)."""
        rows = [_row("2023-12-31", 100000, "Q4")]
        result = compute_ttm(rows)
        assert result[0]["ttm_value"] == 100000
        assert result[0]["ttm_method"] == "annual"

    def test_missing_prior_fy_returns_none(self):
        """Can't compute TTM without prior FY data."""
        rows = [_row("2023-03-31", 28000, "Q1")]
        result = compute_ttm(rows)
        assert result[0]["ttm_value"] is None
        assert result[0]["ttm_method"] is None

    def test_missing_prior_same_quarter_returns_none(self):
        """Can't compute Q2 TTM without prior Q2."""
        rows = [
            _row("2022-12-31", 100000, "FY", "10-K"),
            _row("2023-06-30", 55000, "Q2"),
        ]
        result = compute_ttm(rows)
        q2 = [r for r in result if r["period_end"] == "2023-06-30"][0]
        assert q2["ttm_value"] is None

    def test_full_year_sequence(self):
        """Typical sequence: FY, Q1, Q2, Q3, FY with TTM at each quarter."""
        rows = [
            _row("2022-12-31", 100000, "FY", "10-K"),
            _row("2022-03-31", 23000, "Q1"),      # prior year Q1
            _row("2022-06-30", 48000, "Q2"),      # prior year Q2
            _row("2022-09-30", 74000, "Q3"),      # prior year Q3
            _row("2023-03-31", 26000, "Q1"),      # current Q1
            _row("2023-06-30", 54000, "Q2"),      # current Q2 YTD
            _row("2023-09-30", 83000, "Q3"),      # current Q3 YTD
            _row("2023-12-31", 112000, "FY", "10-K"),
        ]
        result = compute_ttm(rows)

        # Q1 2023: 26000 + 100000 - 23000 = 103000
        q1 = [r for r in result if r["period_end"] == "2023-03-31"][0]
        assert q1["ttm_value"] == 103000

        # Q2 2023: 54000 + 100000 - 48000 = 106000
        q2 = [r for r in result if r["period_end"] == "2023-06-30"][0]
        assert q2["ttm_value"] == 106000

        # Q3 2023: 83000 + 100000 - 74000 = 109000
        q3 = [r for r in result if r["period_end"] == "2023-09-30"][0]
        assert q3["ttm_value"] == 109000

        # FY 2023: annual, TTM = reported value
        fy = [r for r in result if r["period_end"] == "2023-12-31"][0]
        assert fy["ttm_value"] == 112000
        assert fy["ttm_method"] == "annual"

    def test_eps_ttm(self):
        """TTM works for per-share metrics too."""
        rows = [
            _row("2022-03-31", 1.50, "Q1"),
            _row("2022-12-31", 6.00, "FY", "10-K"),
            _row("2023-03-31", 1.80, "Q1"),
        ]
        # Overwrite metric_name
        for r in rows:
            r["metric_name"] = "eps_diluted"

        result = compute_ttm(rows)
        q1 = [r for r in result if r["period_end"] == "2023-03-31"][0]
        # TTM = 1.80 + 6.00 - 1.50 = 6.30
        assert q1["ttm_value"] == 6.3


class TestComputeTTMLatest:
    def test_returns_latest_ttm(self):
        rows = [
            _row("2022-12-31", 100000, "FY", "10-K"),
            _row("2022-03-31", 23000, "Q1"),
            _row("2023-03-31", 26000, "Q1"),
        ]
        val, method = compute_ttm_latest(rows)
        # Latest Q1 2023 TTM = 26000 + 100000 - 23000 = 103000
        assert val == 103000
        assert method == "computed"

    def test_empty(self):
        val, method = compute_ttm_latest([])
        assert val is None
        assert method is None

    def test_falls_back_to_annual(self):
        rows = [_row("2023-12-31", 100000, "FY", "10-K")]
        val, method = compute_ttm_latest(rows)
        assert val == 100000
        assert method == "annual"
