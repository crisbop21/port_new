"""Tests for stock-split detection and normalization."""

from datetime import date

from src.splits import (
    DetectedSplit,
    detect_splits,
    normalize_latest_value,
    normalize_metrics,
    normalize_symbol_data,
)


def _shares_row(period_end: str, value: float, symbol: str = "AAPL") -> dict:
    return {"symbol": symbol, "metric_value": value, "period_end": period_end}


def _eps_row(period_end: str, value: float, symbol: str = "AAPL") -> dict:
    return {"symbol": symbol, "metric_value": value, "period_end": period_end}


class TestDetectSplits:
    def test_no_data(self):
        assert detect_splits([]) == []

    def test_single_period(self):
        assert detect_splits([_shares_row("2024-01-01", 1000)]) == []

    def test_no_split_stable_shares(self):
        data = [
            _shares_row("2023-01-01", 1000000),
            _shares_row("2023-06-30", 1050000),  # 5% change — organic
            _shares_row("2023-12-31", 1020000),
        ]
        assert detect_splits(data) == []

    def test_detects_2_for_1_split_via_ratio(self):
        data = [
            _shares_row("2023-06-30", 1000000),
            _shares_row("2023-12-31", 2000000),  # 2:1 split
        ]
        splits = detect_splits(data)
        assert len(splits) == 1
        assert splits[0].shares_ratio == 2.0
        assert splits[0].confidence == "medium"

    def test_high_confidence_with_eps_confirmation(self):
        shares = [
            _shares_row("2023-06-30", 1000000),
            _shares_row("2023-12-31", 4000000),  # 4:1 split
        ]
        eps = [
            _eps_row("2023-06-30", 8.00),
            _eps_row("2023-12-31", 2.00),  # EPS quartered — confirms split
        ]
        splits = detect_splits(shares, eps)
        assert len(splits) == 1
        assert splits[0].confidence == "high"
        assert abs(splits[0].shares_ratio - 4.0) < 0.01

    def test_organic_buyback_not_flagged(self):
        """30% decrease that isn't a common ratio and EPS doesn't confirm."""
        shares = [
            _shares_row("2023-06-30", 1000000),
            _shares_row("2023-12-31", 700000),  # 30% buyback
        ]
        eps = [
            _eps_row("2023-06-30", 5.00),
            _eps_row("2023-12-31", 6.50),  # EPS up ~30% — organic growth
        ]
        splits = detect_splits(shares, eps)
        assert len(splits) == 0

    def test_reverse_split(self):
        shares = [
            _shares_row("2023-06-30", 1000000),
            _shares_row("2023-12-31", 100000),  # 1:10 reverse split
        ]
        splits = detect_splits(shares)
        assert len(splits) == 1
        assert splits[0].shares_ratio < 1.0

    def test_multiple_splits(self):
        shares = [
            _shares_row("2022-12-31", 500000),
            _shares_row("2023-06-30", 1000000),  # 2:1
            _shares_row("2023-12-31", 1050000),  # organic
            _shares_row("2024-06-30", 3150000),  # 3:1
        ]
        splits = detect_splits(shares)
        assert len(splits) == 2


class TestNormalizeMetrics:
    def test_no_splits_passes_through(self):
        rows = [_shares_row("2024-01-01", 1000)]
        result = normalize_metrics(rows, [], "shares_outstanding")
        assert result[0]["normalized_value"] == 1000
        assert result[0]["split_adjusted"] is False

    def test_shares_normalized_for_split(self):
        rows = [
            _shares_row("2023-06-30", 500000),
            _shares_row("2023-12-31", 1000000),
        ]
        split = DetectedSplit(
            symbol="AAPL",
            period_end=date(2023, 12, 31),
            prior_period_end=date(2023, 6, 30),
            shares_ratio=2.0,
            confidence="high",
            reason="test",
        )
        result = normalize_metrics(rows, [split], "shares_outstanding")
        # Pre-split period should be multiplied by 2
        pre = [r for r in result if str(r["period_end"]) == "2023-06-30"][0]
        assert pre["normalized_value"] == 1000000
        assert pre["split_adjusted"] is True
        # Post-split period unchanged
        post = [r for r in result if str(r["period_end"]) == "2023-12-31"][0]
        assert post["split_adjusted"] is False

    def test_eps_normalized_for_split(self):
        rows = [
            _eps_row("2023-06-30", 8.00),
            _eps_row("2023-12-31", 2.00),
        ]
        split = DetectedSplit(
            symbol="AAPL",
            period_end=date(2023, 12, 31),
            prior_period_end=date(2023, 6, 30),
            shares_ratio=4.0,
            confidence="high",
            reason="test",
        )
        result = normalize_metrics(rows, [split], "eps_diluted")
        pre = [r for r in result if str(r["period_end"]) == "2023-06-30"][0]
        assert pre["normalized_value"] == 2.0  # 8.0 / 4.0
        assert pre["split_adjusted"] is True

    def test_revenue_unaffected_by_split(self):
        rows = [{"symbol": "AAPL", "metric_value": 100000, "period_end": "2023-06-30"}]
        split = DetectedSplit(
            symbol="AAPL",
            period_end=date(2023, 12, 31),
            prior_period_end=date(2023, 6, 30),
            shares_ratio=2.0,
            confidence="high",
            reason="test",
        )
        result = normalize_metrics(rows, [split], "revenue")
        assert result[0]["normalized_value"] == 100000
        assert result[0]["split_adjusted"] is False

    def test_cumulative_splits_eps(self):
        """Two sequential splits: 2:1 then 3:1 = 6x cumulative."""
        rows = [
            _eps_row("2022-12-31", 12.00),
            _eps_row("2023-06-30", 6.00),
            _eps_row("2024-06-30", 2.00),
        ]
        splits = [
            DetectedSplit("AAPL", date(2023, 6, 30), date(2022, 12, 31), 2.0, "high", ""),
            DetectedSplit("AAPL", date(2024, 6, 30), date(2023, 6, 30), 3.0, "high", ""),
        ]
        result = normalize_metrics(rows, splits, "eps_diluted")
        # Earliest period: factor = 2 * 3 = 6, so 12.0 / 6 = 2.0
        oldest = [r for r in result if str(r["period_end"]) == "2022-12-31"][0]
        assert oldest["normalized_value"] == 2.0
        assert oldest["split_adjusted"] is True
        # Middle period: factor = 3, so 6.0 / 3 = 2.0
        mid = [r for r in result if str(r["period_end"]) == "2023-06-30"][0]
        assert mid["normalized_value"] == 2.0
        assert mid["split_adjusted"] is True
        # Latest: no adjustment
        latest = [r for r in result if str(r["period_end"]) == "2024-06-30"][0]
        assert latest["split_adjusted"] is False


class TestNormalizeLatestValue:
    def test_no_splits(self):
        val, adj = normalize_latest_value("eps_diluted", 5.0, "2024-01-01", [])
        assert val == 5.0
        assert adj is False

    def test_adjusts_eps(self):
        split = DetectedSplit("AAPL", date(2024, 6, 30), date(2024, 3, 31), 4.0, "high", "")
        val, adj = normalize_latest_value("eps_diluted", 8.0, "2024-03-31", [split])
        assert val == 2.0
        assert adj is True

    def test_adjusts_shares(self):
        split = DetectedSplit("AAPL", date(2024, 6, 30), date(2024, 3, 31), 4.0, "high", "")
        val, adj = normalize_latest_value("shares_outstanding", 250000, "2024-03-31", [split])
        assert val == 1000000
        assert adj is True

    def test_revenue_unaffected(self):
        split = DetectedSplit("AAPL", date(2024, 6, 30), date(2024, 3, 31), 4.0, "high", "")
        val, adj = normalize_latest_value("revenue", 50000, "2024-03-31", [split])
        assert val == 50000
        assert adj is False

    def test_post_split_period_unchanged(self):
        split = DetectedSplit("AAPL", date(2024, 6, 30), date(2024, 3, 31), 4.0, "high", "")
        val, adj = normalize_latest_value("eps_diluted", 2.0, "2024-09-30", [split])
        assert val == 2.0
        assert adj is False


class TestNormalizeSymbolData:
    def test_normalizes_all_metrics(self):
        split = DetectedSplit("AAPL", date(2024, 6, 30), date(2024, 3, 31), 2.0, "high", "")
        data = {
            "eps_diluted": [_eps_row("2024-03-31", 10.0), _eps_row("2024-06-30", 5.0)],
            "shares_outstanding": [_shares_row("2024-03-31", 500000), _shares_row("2024-06-30", 1000000)],
            "revenue": [{"symbol": "AAPL", "metric_value": 90000, "period_end": "2024-03-31"}],
        }
        result = normalize_symbol_data(data, [split])

        # EPS adjusted
        eps_pre = [r for r in result["eps_diluted"] if r["period_end"] == "2024-03-31"][0]
        assert eps_pre["normalized_value"] == 5.0
        assert eps_pre["split_adjusted"] is True

        # Shares adjusted
        sh_pre = [r for r in result["shares_outstanding"] if r["period_end"] == "2024-03-31"][0]
        assert sh_pre["normalized_value"] == 1000000
        assert sh_pre["split_adjusted"] is True

        # Revenue untouched
        rev = result["revenue"][0]
        assert rev["normalized_value"] == 90000
        assert rev["split_adjusted"] is False
