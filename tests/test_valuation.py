"""Tests for valuation engine — compute_historical_ratios TTM fix."""

import math

from src.valuation import compute_historical_ratios, compute_percentile


def _make_metric_row(period_end: str, value: float, fiscal_period: str = "Q1",
                     fiscal_year: int | None = None, duration_days: int | None = None) -> dict:
    """Helper to build a metric row dict."""
    row = {
        "period_end": period_end,
        "metric_value": value,
        "fiscal_period": fiscal_period,
    }
    if fiscal_year is not None:
        row["fiscal_year"] = fiscal_year
    if duration_days is not None:
        row["duration_days"] = duration_days
    return row


def _make_price_row(price_date: str, adj_close: float) -> dict:
    return {"price_date": price_date, "adj_close": adj_close}


class TestComputeHistoricalRatiosTTM:
    """Verify that compute_historical_ratios uses TTM for flow metrics."""

    def test_pe_uses_ttm_eps_not_quarterly(self):
        """Historical P/E should use TTM EPS (sum of 4 quarters), not raw quarterly."""
        # Build 8 quarters of standalone EPS data (~$3.50/quarter → ~$14 TTM)
        eps_rows = [
            _make_metric_row("2023-03-31", 3.00, "Q1", 2023, duration_days=90),
            _make_metric_row("2023-06-30", 3.20, "Q2", 2023, duration_days=90),
            _make_metric_row("2023-09-30", 3.40, "Q3", 2023, duration_days=90),
            _make_metric_row("2023-12-31", 14.00, "FY", 2023),  # FY = full year
            _make_metric_row("2024-03-31", 3.80, "Q1", 2024, duration_days=90),
            _make_metric_row("2024-06-30", 4.00, "Q2", 2024, duration_days=90),
            _make_metric_row("2024-09-30", 4.20, "Q3", 2024, duration_days=90),
            _make_metric_row("2024-12-31", 16.00, "FY", 2024),  # FY
        ]

        shares_rows = [
            _make_metric_row("2023-03-31", 1_000_000, "Q1", 2023),
            _make_metric_row("2023-06-30", 1_000_000, "Q2", 2023),
            _make_metric_row("2023-09-30", 1_000_000, "Q3", 2023),
            _make_metric_row("2023-12-31", 1_000_000, "FY", 2023),
            _make_metric_row("2024-03-31", 1_000_000, "Q1", 2024),
            _make_metric_row("2024-06-30", 1_000_000, "Q2", 2024),
            _make_metric_row("2024-09-30", 1_000_000, "Q3", 2024),
            _make_metric_row("2024-12-31", 1_000_000, "FY", 2024),
        ]

        prices = [
            _make_price_row(f"2023-0{m}-{28 if m == 2 else 30}", 140.0)
            for m in range(1, 10)
        ] + [
            _make_price_row(f"2023-{m}-30", 140.0)
            for m in range(10, 13)
        ] + [
            _make_price_row(f"2024-0{m}-{28 if m == 2 else 30}", 160.0)
            for m in range(1, 10)
        ] + [
            _make_price_row(f"2024-{m}-30", 160.0)
            for m in range(10, 13)
        ]

        metric_hist = {
            "shares_outstanding": sorted(shares_rows, key=lambda r: r["period_end"]),
            "eps_diluted": sorted(eps_rows, key=lambda r: r["period_end"]),
        }

        results = compute_historical_ratios(metric_hist, prices)

        # Find results that have a pe_ttm
        pe_results = [r for r in results if r.get("pe_ttm") is not None]
        assert len(pe_results) > 0, "Should have at least one P/E ratio computed"

        # The FY 2023 row (price=140, FY EPS=14.00) should give P/E = 10.0
        fy_2023 = [r for r in pe_results if r["period_end"] == "2023-12-31"]
        if fy_2023:
            assert abs(fy_2023[0]["pe_ttm"] - 10.0) < 0.5, \
                f"FY2023 P/E should be ~10.0 (140/14), got {fy_2023[0]['pe_ttm']}"

        # The FY 2024 row (price=160, FY EPS=16.00) should give P/E = 10.0
        fy_2024 = [r for r in pe_results if r["period_end"] == "2024-12-31"]
        if fy_2024:
            assert abs(fy_2024[0]["pe_ttm"] - 10.0) < 0.5, \
                f"FY2024 P/E should be ~10.0 (160/16), got {fy_2024[0]['pe_ttm']}"

        # At quarterly periods, P/E must also use TTM EPS, not raw quarterly.
        # Q1 2024: price=160, raw quarterly EPS=3.80 → raw P/E=42 (WRONG)
        # Q1 2024: TTM EPS = Q2'23(3.20)+Q3'23(3.40)+Q4'23(4.40)+Q1'24(3.80) = 14.80
        #          → TTM P/E = 160/14.80 ≈ 10.8 (CORRECT)
        q1_2024 = [r for r in pe_results if r["period_end"] == "2024-03-31"]
        if q1_2024:
            assert q1_2024[0]["pe_ttm"] < 15, \
                f"Q1 2024 P/E should be ~10.8 (TTM), got {q1_2024[0]['pe_ttm']} (using raw quarterly EPS?)"

        # No P/E should be wildly inflated (>20 with these prices/EPS)
        for r in pe_results:
            assert r["pe_ttm"] < 20, \
                f"P/E at {r['period_end']} = {r['pe_ttm']} is suspiciously high (quarterly EPS bug?)"

    def test_ps_uses_ttm_revenue(self):
        """Historical P/S should use TTM revenue, not quarterly."""
        revenue_rows = [
            _make_metric_row("2023-03-31", 5_000, "Q1", 2023, duration_days=90),
            _make_metric_row("2023-06-30", 5_500, "Q2", 2023, duration_days=90),
            _make_metric_row("2023-09-30", 6_000, "Q3", 2023, duration_days=90),
            _make_metric_row("2023-12-31", 22_000, "FY", 2023),  # FY total
        ]

        shares_rows = [
            _make_metric_row("2023-03-31", 1_000, "Q1", 2023),
            _make_metric_row("2023-06-30", 1_000, "Q2", 2023),
            _make_metric_row("2023-09-30", 1_000, "Q3", 2023),
            _make_metric_row("2023-12-31", 1_000, "FY", 2023),
        ]

        prices = [
            _make_price_row("2023-03-30", 100.0),
            _make_price_row("2023-06-29", 100.0),
            _make_price_row("2023-09-29", 100.0),
            _make_price_row("2023-12-29", 100.0),
        ]

        metric_hist = {
            "shares_outstanding": sorted(shares_rows, key=lambda r: r["period_end"]),
            "revenue": sorted(revenue_rows, key=lambda r: r["period_end"]),
        }

        results = compute_historical_ratios(metric_hist, prices)
        ps_results = [r for r in results if r.get("ps") is not None]

        # At FY 2023: market_cap = 100*1000 = 100_000, TTM revenue = 22_000
        # P/S should be ~4.5, NOT ~20 (100_000/5_000 from quarterly)
        for r in ps_results:
            if r["period_end"] == "2023-12-31":
                assert r["ps"] < 10, \
                    f"P/S at FY2023 = {r['ps']} is too high (quarterly revenue bug?)"


class TestComputePercentile:
    """Sanity checks for compute_percentile."""

    def test_basic_percentile(self):
        assert compute_percentile(5.0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) == 40.0

    def test_returns_none_for_few_values(self):
        assert compute_percentile(5.0, [1, 2, 3]) is None

    def test_returns_none_for_none_current(self):
        assert compute_percentile(None, [1, 2, 3, 4, 5]) is None
