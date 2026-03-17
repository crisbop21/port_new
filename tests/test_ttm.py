"""Tests for TTM (Trailing Twelve Months) via quarter isolation."""

from src.ttm import (
    IsolatedQuarter,
    compute_ttm,
    compute_ttm_latest,
    is_flow_metric,
    isolate_quarters,
)


def _row(period_end: str, value: float, fp: str, form: str = "10-Q") -> dict:
    return {
        "symbol": "AAPL",
        "metric_name": "revenue",
        "metric_value": value,
        "period_end": period_end,
        "fiscal_period": fp,
        "filing_type": form,
    }


# ── Classification ──────────────────────────────────────────────────────────


class TestIsFlowMetric:
    def test_revenue_is_flow(self):
        assert is_flow_metric("revenue") is True

    def test_eps_is_flow(self):
        assert is_flow_metric("eps_diluted") is True

    def test_total_assets_is_not_flow(self):
        assert is_flow_metric("total_assets") is False

    def test_shares_outstanding_is_not_flow(self):
        assert is_flow_metric("shares_outstanding") is False


# ── Quarter isolation ───────────────────────────────────────────────────────


class TestIsolateQuarters:
    def test_empty(self):
        assert isolate_quarters([]) == []

    def test_q1_is_direct(self):
        """Q1 YTD is already a single quarter — no subtraction needed."""
        rows = [_row("2023-03-31", 25000, "Q1")]
        result = isolate_quarters(rows)
        assert len(result) == 1
        assert result[0].quarter == "Q1"
        assert result[0].isolated_value == 25000
        assert result[0].method == "direct"

    def test_q2_subtracts_q1(self):
        """Q2 isolated = Q2 YTD − Q1 YTD."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),  # 6-month YTD
        ]
        result = isolate_quarters(rows)
        q2 = [q for q in result if q.quarter == "Q2"][0]
        assert q2.isolated_value == 28000  # 53000 - 25000
        assert q2.method == "subtracted"

    def test_q3_subtracts_q2(self):
        """Q3 isolated = Q3 YTD − Q2 YTD."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),  # 9-month YTD
        ]
        result = isolate_quarters(rows)
        q3 = [q for q in result if q.quarter == "Q3"][0]
        assert q3.isolated_value == 27000  # 80000 - 53000
        assert q3.method == "subtracted"

    def test_q4_from_fy_minus_q3(self):
        """Q4 isolated = FY − Q3 YTD."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),
            _row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = isolate_quarters(rows)
        q4 = [q for q in result if q.quarter == "Q4"][0]
        assert q4.isolated_value == 28000  # 108000 - 80000
        assert q4.method == "subtracted"

    def test_fy_without_q3_becomes_annual_only(self):
        """If no Q3, FY can't be broken down — stored as annual_only."""
        rows = [_row("2023-12-31", 108000, "FY", "10-K")]
        result = isolate_quarters(rows)
        assert len(result) == 1
        assert result[0].quarter == "Q4"
        assert result[0].isolated_value == 108000
        assert result[0].method == "annual_only"

    def test_full_year_isolation(self):
        """All 4 quarters should sum back to FY."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),
            _row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = isolate_quarters(rows)
        total = sum(q.isolated_value for q in result)
        assert total == 108000

    def test_multi_year(self):
        """Isolation works across multiple fiscal years."""
        rows = [
            # FY 2022
            _row("2022-03-31", 20000, "Q1"),
            _row("2022-06-30", 42000, "Q2"),
            _row("2022-09-30", 65000, "Q3"),
            _row("2022-12-31", 90000, "FY", "10-K"),
            # FY 2023
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),
            _row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = isolate_quarters(rows)
        assert len(result) == 8

        # 2022 sums to 90000
        fy2022 = [q for q in result if q.fiscal_year == 2022]
        assert sum(q.isolated_value for q in fy2022) == 90000

        # 2023 sums to 108000
        fy2023 = [q for q in result if q.fiscal_year == 2023]
        assert sum(q.isolated_value for q in fy2023) == 108000

    def test_missing_q2_skips_q3_isolation(self):
        """Q3 can't be isolated without Q2 YTD."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            # Q2 missing
            _row("2023-09-30", 80000, "Q3"),
        ]
        result = isolate_quarters(rows)
        # Only Q1 should be isolated; Q3 is skipped
        assert len(result) == 1
        assert result[0].quarter == "Q1"

    def test_eps_isolation(self):
        """Works with per-share values too."""
        rows = [
            _row("2023-03-31", 1.50, "Q1"),
            _row("2023-06-30", 3.20, "Q2"),
        ]
        result = isolate_quarters(rows)
        q2 = [q for q in result if q.quarter == "Q2"][0]
        assert q2.isolated_value == 1.7  # 3.20 - 1.50


# ── TTM computation ─────────────────────────────────────────────────────────


class TestComputeTTM:
    def test_empty(self):
        assert compute_ttm([]) == []

    def test_annual_passthrough(self):
        """FY with no quarterly data → TTM = FY value."""
        rows = [_row("2023-12-31", 100000, "FY", "10-K")]
        result = compute_ttm(rows)
        assert result[0]["ttm_value"] == 100000
        assert result[0]["ttm_method"] == "annual"

    def test_4_quarters_sum(self):
        """TTM = sum of 4 isolated quarters."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),
            _row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = compute_ttm(rows)

        # FY row should have TTM
        fy_row = [r for r in result if r["fiscal_period"] == "FY"][0]
        assert fy_row["ttm_value"] == 108000

    def test_rolling_ttm_across_years(self):
        """TTM at Q1 2024 = Q2_2023 + Q3_2023 + Q4_2023 + Q1_2024."""
        rows = [
            # 2023
            _row("2023-03-31", 23000, "Q1"),
            _row("2023-06-30", 48000, "Q2"),      # Q2 iso = 25000
            _row("2023-09-30", 74000, "Q3"),      # Q3 iso = 26000
            _row("2023-12-31", 100000, "FY", "10-K"),  # Q4 iso = 26000
            # 2024
            _row("2024-03-31", 26000, "Q1"),      # Q1 iso = 26000
        ]
        result = compute_ttm(rows)

        q1_2024 = [r for r in result if r["period_end"] == "2024-03-31"][0]
        # TTM = Q2_23(25000) + Q3_23(26000) + Q4_23(26000) + Q1_24(26000) = 103000
        assert q1_2024["ttm_value"] == 103000
        assert q1_2024["ttm_method"] == "sum_4q"

    def test_quarterly_value_enrichment(self):
        """Each row gets its isolated quarterly_value."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
        ]
        result = compute_ttm(rows)
        q1 = [r for r in result if r["period_end"] == "2023-03-31"][0]
        assert q1["quarterly_value"] == 25000
        q2 = [r for r in result if r["period_end"] == "2023-06-30"][0]
        assert q2["quarterly_value"] == 28000

    def test_is_ytd_flag(self):
        """Q2/Q3 are YTD, Q1/FY are not."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),
            _row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = compute_ttm(rows)
        by_fp = {r["fiscal_period"]: r for r in result}
        assert by_fp["Q1"]["is_ytd"] is False
        assert by_fp["Q2"]["is_ytd"] is True
        assert by_fp["Q3"]["is_ytd"] is True
        assert by_fp["FY"]["is_ytd"] is False

    def test_insufficient_quarters_no_ttm(self):
        """Less than 4 isolated quarters → no TTM available."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
        ]
        result = compute_ttm(rows)
        for r in result:
            assert r["ttm_value"] is None

    def test_full_sequence_two_years(self):
        """Two full years with rolling TTM at every quarter."""
        rows = [
            # 2022
            _row("2022-03-31", 20000, "Q1"),
            _row("2022-06-30", 42000, "Q2"),
            _row("2022-09-30", 65000, "Q3"),
            _row("2022-12-31", 90000, "FY", "10-K"),
            # 2023
            _row("2023-03-31", 25000, "Q1"),
            _row("2023-06-30", 53000, "Q2"),
            _row("2023-09-30", 80000, "Q3"),
            _row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = compute_ttm(rows)

        # FY 2022 TTM = 90000 (Q1=20k, Q2=22k, Q3=23k, Q4=25k)
        fy2022 = [r for r in result if r["period_end"] == "2022-12-31"][0]
        assert fy2022["ttm_value"] == 90000

        # Q1 2023 TTM = Q2_22(22k) + Q3_22(23k) + Q4_22(25k) + Q1_23(25k) = 95000
        q1_23 = [r for r in result if r["period_end"] == "2023-03-31"][0]
        assert q1_23["ttm_value"] == 95000

        # FY 2023 TTM = 108000
        fy2023 = [r for r in result if r["period_end"] == "2023-12-31"
                  and r.get("fiscal_period") == "FY"][0]
        assert fy2023["ttm_value"] == 108000

    def test_eps_ttm(self):
        """TTM works for per-share metrics."""
        rows = [
            _row("2022-03-31", 1.50, "Q1"),
            _row("2022-06-30", 3.00, "Q2"),
            _row("2022-09-30", 4.50, "Q3"),
            _row("2022-12-31", 6.00, "FY", "10-K"),
            _row("2023-03-31", 1.80, "Q1"),
        ]
        result = compute_ttm(rows)
        q1_23 = [r for r in result if r["period_end"] == "2023-03-31"][0]
        # Q2_22=1.50, Q3_22=1.50, Q4_22=1.50, Q1_23=1.80 → TTM = 6.30
        assert q1_23["ttm_value"] == 6.3


# ── compute_ttm_latest ──────────────────────────────────────────────────────


class TestComputeTTMLatest:
    def test_returns_latest_ttm(self):
        rows = [
            _row("2022-03-31", 23000, "Q1"),
            _row("2022-06-30", 48000, "Q2"),
            _row("2022-09-30", 74000, "Q3"),
            _row("2022-12-31", 100000, "FY", "10-K"),
            _row("2023-03-31", 26000, "Q1"),
        ]
        val, method = compute_ttm_latest(rows)
        # Q2=25k, Q3=26k, Q4=26k, Q1=26k → 103k
        assert val == 103000
        assert method == "sum_4q"

    def test_empty(self):
        val, method = compute_ttm_latest([])
        assert val is None
        assert method is None

    def test_falls_back_to_annual(self):
        rows = [_row("2023-12-31", 100000, "FY", "10-K")]
        val, method = compute_ttm_latest(rows)
        assert val == 100000
        assert method == "annual"

    def test_only_annual_no_quarters(self):
        """FY-only data should still return TTM = annual value."""
        rows = [
            _row("2022-12-31", 90000, "FY", "10-K"),
            _row("2023-12-31", 100000, "FY", "10-K"),
        ]
        val, method = compute_ttm_latest(rows)
        assert val == 100000
        assert method == "annual"
