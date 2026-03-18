"""Tests for TTM (Trailing Twelve Months) via quarter isolation."""

from datetime import date

from src.splits import DetectedSplit, normalize_metrics
from src.ttm import (
    IsolatedQuarter,
    compute_ttm,
    compute_ttm_latest,
    is_flow_metric,
    isolate_quarters,
)


def _row(
    period_end: str,
    value: float,
    fp: str,
    form: str = "10-Q",
    fiscal_year: int | None = None,
    duration_days: int | None = None,
    reporting_style: str | None = None,
) -> dict:
    return {
        "symbol": "AAPL",
        "metric_name": "revenue",
        "metric_value": value,
        "period_end": period_end,
        "fiscal_period": fp,
        "filing_type": form,
        "fiscal_year": fiscal_year,
        "duration_days": duration_days,
        "reporting_style": reporting_style,
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


# ── TTM + split composition ────────────────────────────────────────────────


class TestTTMWithSplits:
    """Verify that split normalization composes correctly with TTM.

    Pipeline: split-normalize all history → compute TTM on normalized_value.
    """

    def test_eps_ttm_with_4_to_1_split(self):
        """4:1 split mid-year: pre-split EPS must be divided before TTM sum.

        Raw data:
            Q1 2022: EPS YTD = $6.00  (pre-split)
            Q2 2022: EPS YTD = $12.00 (pre-split)
            Q3 2022: EPS YTD = $18.00 (pre-split)
            FY 2022: EPS     = $24.00 (pre-split)
            ── 4:1 split happens ──
            Q1 2023: EPS YTD = $1.80  (post-split)

        Without normalization, TTM would mix $6 pre-split quarters with
        $1.80 post-split → wrong.

        With normalization (divide pre-split by 4):
            Q1 2022 norm = $1.50, Q2 YTD norm = $3.00, Q3 YTD norm = $4.50, FY norm = $6.00
            Q2 iso = 1.50, Q3 iso = 1.50, Q4 iso = 1.50
            TTM = Q2(1.50) + Q3(1.50) + Q4(1.50) + Q1_23(1.80) = $6.30
        """
        split = DetectedSplit(
            symbol="AAPL",
            period_end=date(2023, 1, 15),  # split between FY2022 and Q1 2023
            prior_period_end=date(2022, 12, 31),
            shares_ratio=4.0,
            confidence="high",
            reason="test",
        )

        rows = [
            _row("2022-03-31", 6.00, "Q1"),
            _row("2022-06-30", 12.00, "Q2"),
            _row("2022-09-30", 18.00, "Q3"),
            _row("2022-12-31", 24.00, "FY", "10-K"),
            _row("2023-03-31", 1.80, "Q1"),
        ]
        for r in rows:
            r["metric_name"] = "eps_diluted"

        # Step 1: Split-normalize
        normalized = normalize_metrics(rows, [split], "eps_diluted")

        # Verify normalization happened
        pre_q1 = [r for r in normalized if r["period_end"] == "2022-03-31"][0]
        assert pre_q1["normalized_value"] == 1.5  # 6.00 / 4
        post_q1 = [r for r in normalized if r["period_end"] == "2023-03-31"][0]
        assert post_q1["split_adjusted"] is False  # post-split, not adjusted

        # Step 2: TTM on normalized values
        val, method = compute_ttm_latest(normalized, value_key="normalized_value")

        # Q2 iso = 3.00-1.50=1.50, Q3 iso = 4.50-3.00=1.50,
        # Q4 iso = 6.00-4.50=1.50, Q1_23 = 1.80
        # TTM = 1.50 + 1.50 + 1.50 + 1.80 = 6.30
        assert val == 6.3
        assert method == "sum_4q"

    def test_value_key_reads_correct_field(self):
        """compute_ttm with value_key='normalized_value' uses that field."""
        rows = [
            {"period_end": "2022-03-31", "fiscal_period": "Q1",
             "metric_value": 100, "normalized_value": 50},
            {"period_end": "2022-06-30", "fiscal_period": "Q2",
             "metric_value": 200, "normalized_value": 100},
            {"period_end": "2022-09-30", "fiscal_period": "Q3",
             "metric_value": 300, "normalized_value": 150},
            {"period_end": "2022-12-31", "fiscal_period": "FY",
             "metric_value": 400, "normalized_value": 200},
        ]
        # Using normalized_value: Q1=50, Q2=100-50=50, Q3=150-100=50, Q4=200-150=50
        result = compute_ttm(rows, value_key="normalized_value")
        fy = [r for r in result if r["fiscal_period"] == "FY"][0]
        assert fy["ttm_value"] == 200  # sum of normalized quarters

        # Using metric_value: Q1=100, Q2=200-100=100, Q3=300-200=100, Q4=400-300=100
        result2 = compute_ttm(rows, value_key="metric_value")
        fy2 = [r for r in result2 if r["fiscal_period"] == "FY"][0]
        assert fy2["ttm_value"] == 400  # sum of raw quarters

    def test_no_splits_uses_raw_values(self):
        """Without splits, normalized_value == metric_value, TTM is the same."""
        rows = [
            _row("2022-03-31", 25000, "Q1"),
            _row("2022-06-30", 53000, "Q2"),
            _row("2022-09-30", 80000, "Q3"),
            _row("2022-12-31", 108000, "FY", "10-K"),
        ]
        # Normalize with no splits (just copies metric_value → normalized_value)
        normalized = normalize_metrics(rows, [], "revenue")

        val_raw, _ = compute_ttm_latest(normalized, value_key="metric_value")
        val_norm, _ = compute_ttm_latest(normalized, value_key="normalized_value")
        assert val_raw == val_norm == 108000


# ── Standalone quarterly reporting ─────────────────────────────────────────


class TestStandaloneQuarterly:
    """Test isolation logic for companies that report standalone quarters."""

    def _standalone_row(self, period_end, value, fp, form="10-Q"):
        """Helper for standalone rows — each Q is 90 days, style is standalone."""
        return _row(
            period_end, value, fp, form,
            duration_days=90 if fp != "FY" else 365,
            reporting_style="standalone_quarterly",
        )

    def test_q1_q2_standalone(self):
        """Standalone Q1 and Q2 should be used directly, no subtraction."""
        rows = [
            self._standalone_row("2023-03-31", 25000, "Q1"),
            self._standalone_row("2023-06-30", 28000, "Q2"),
        ]
        result = isolate_quarters(rows)
        assert len(result) == 2
        assert result[0].isolated_value == 25000
        assert result[0].method == "standalone"
        assert result[1].isolated_value == 28000
        assert result[1].method == "standalone"

    def test_full_year_standalone(self):
        """All 4 standalone quarters should sum to FY."""
        rows = [
            self._standalone_row("2023-03-31", 25000, "Q1"),
            self._standalone_row("2023-06-30", 28000, "Q2"),
            self._standalone_row("2023-09-30", 27000, "Q3"),
            self._standalone_row("2023-12-31", 108000, "FY", "10-K"),
        ]
        result = isolate_quarters(rows)
        assert len(result) == 4
        # Q4 = FY - (Q1+Q2+Q3) = 108000 - 80000 = 28000
        q4 = [q for q in result if q.quarter == "Q4"][0]
        assert q4.isolated_value == 28000
        assert q4.method == "subtracted"

        # Sum should equal FY
        total = sum(q.isolated_value for q in result)
        assert total == 108000

    def test_standalone_ttm(self):
        """TTM computation works correctly for standalone reporters."""
        rows = [
            self._standalone_row("2022-03-31", 20000, "Q1"),
            self._standalone_row("2022-06-30", 22000, "Q2"),
            self._standalone_row("2022-09-30", 23000, "Q3"),
            self._standalone_row("2022-12-31", 90000, "FY", "10-K"),
            self._standalone_row("2023-03-31", 25000, "Q1"),
        ]
        result = compute_ttm(rows)
        q1_23 = [r for r in result if r["period_end"] == "2023-03-31"][0]
        # Q4_22 = 90000 - (20000+22000+23000) = 25000
        # TTM = Q2_22(22000) + Q3_22(23000) + Q4_22(25000) + Q1_23(25000) = 95000
        assert q1_23["ttm_value"] == 95000
        assert q1_23["ttm_method"] == "sum_4q"

    def test_standalone_is_ytd_false(self):
        """Standalone reporters should never have is_ytd=True."""
        rows = [
            self._standalone_row("2023-03-31", 25000, "Q1"),
            self._standalone_row("2023-06-30", 28000, "Q2"),
            self._standalone_row("2023-09-30", 27000, "Q3"),
        ]
        result = compute_ttm(rows)
        for r in result:
            assert r["is_ytd"] is False


class TestFiscalYearField:
    """Test that fiscal_year from XBRL is used instead of period_end.year."""

    def test_non_calendar_fy(self):
        """Apple: FY ends Sep, so FY2024 Q1 ends Dec 2023 (period_end.year=2023).

        With fiscal_year=2024 from XBRL, isolation should group correctly.
        """
        rows = [
            _row("2023-12-30", 30000, "Q1", fiscal_year=2024),
            _row("2024-03-30", 65000, "Q2", fiscal_year=2024),
            _row("2024-06-29", 98000, "Q3", fiscal_year=2024),
            _row("2024-09-28", 130000, "FY", "10-K", fiscal_year=2024),
        ]
        result = isolate_quarters(rows)
        assert len(result) == 4
        # All should be FY 2024
        assert all(q.fiscal_year == 2024 for q in result)
        # Q1=30000, Q2=35000, Q3=33000, Q4=32000
        q1 = [q for q in result if q.quarter == "Q1"][0]
        assert q1.isolated_value == 30000
        q2 = [q for q in result if q.quarter == "Q2"][0]
        assert q2.isolated_value == 35000
        # Sum = FY
        total = sum(q.isolated_value for q in result)
        assert total == 130000

    def test_fallback_to_period_end_year(self):
        """Without fiscal_year field, falls back to period_end.year."""
        rows = [
            _row("2023-03-31", 25000, "Q1"),  # no fiscal_year
            _row("2023-06-30", 53000, "Q2"),
        ]
        result = isolate_quarters(rows)
        assert result[0].fiscal_year == 2023
        assert result[1].fiscal_year == 2023
