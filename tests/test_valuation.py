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

        # Daily prices: one per month, $140 in 2023, $160 in 2024
        prices = [
            _make_price_row(f"2023-0{m}-{28 if m == 2 else 15}", 140.0)
            for m in range(1, 10)
        ] + [
            _make_price_row(f"2023-{m}-15", 140.0)
            for m in range(10, 13)
        ] + [
            _make_price_row(f"2024-0{m}-{28 if m == 2 else 15}", 160.0)
            for m in range(1, 10)
        ] + [
            _make_price_row(f"2024-{m}-15", 160.0)
            for m in range(10, 13)
        ]

        metric_hist = {
            "shares_outstanding": sorted(shares_rows, key=lambda r: r["period_end"]),
            "eps_diluted": sorted(eps_rows, key=lambda r: r["period_end"]),
        }

        results = compute_historical_ratios(metric_hist, prices)

        # Should produce daily observations (one per price row that has fundamentals)
        pe_results = [r for r in results if r.get("pe_ttm") is not None]
        assert len(pe_results) > 0, "Should have at least one P/E ratio computed"

        # Should have MORE than just quarterly observations (daily resolution)
        assert len(pe_results) > 8, \
            f"Expected daily observations, got only {len(pe_results)} (still quarterly?)"

        # No P/E should be wildly inflated (>20 with these prices/EPS)
        for r in pe_results:
            assert r["pe_ttm"] < 20, \
                f"P/E at {r['period_end']} = {r['pe_ttm']} is suspiciously high (quarterly EPS bug?)"

    def test_daily_pe_reflects_price_changes(self):
        """P/E should change when price changes, even with same EPS."""
        eps_rows = [
            _make_metric_row("2023-03-31", 3.00, "Q1", 2023, duration_days=90),
            _make_metric_row("2023-06-30", 3.00, "Q2", 2023, duration_days=90),
            _make_metric_row("2023-09-30", 3.00, "Q3", 2023, duration_days=90),
            _make_metric_row("2023-12-31", 12.00, "FY", 2023),
        ]

        shares_rows = [
            _make_metric_row("2023-03-31", 1_000, "Q1", 2023),
            _make_metric_row("2023-12-31", 1_000, "FY", 2023),
        ]

        # Two different prices on consecutive days after FY filing
        prices = [
            _make_price_row("2024-01-02", 120.0),  # P/E = 120/12 = 10
            _make_price_row("2024-01-03", 180.0),  # P/E = 180/12 = 15
        ]

        metric_hist = {
            "shares_outstanding": sorted(shares_rows, key=lambda r: r["period_end"]),
            "eps_diluted": sorted(eps_rows, key=lambda r: r["period_end"]),
        }

        results = compute_historical_ratios(metric_hist, prices)
        pe_results = [r for r in results if r.get("pe_ttm") is not None]

        assert len(pe_results) == 2, f"Expected 2 daily P/E observations, got {len(pe_results)}"
        assert abs(pe_results[0]["pe_ttm"] - 10.0) < 0.01
        assert abs(pe_results[1]["pe_ttm"] - 15.0) < 0.01

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
        fy_result = [r for r in ps_results if r["period_end"] == "2023-12-29"]
        if fy_result:
            assert fy_result[0]["ps"] < 10, \
                f"P/S near FY2023 = {fy_result[0]['ps']} is too high (quarterly revenue bug?)"


class TestComputePercentile:
    """Sanity checks for compute_percentile."""

    def test_basic_percentile(self):
        assert compute_percentile(5.0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) == 40.0

    def test_returns_none_for_few_values(self):
        assert compute_percentile(5.0, [1, 2, 3]) is None

    def test_returns_none_for_none_current(self):
        assert compute_percentile(None, [1, 2, 3, 4, 5]) is None
