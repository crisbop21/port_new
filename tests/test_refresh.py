"""Tests for src.refresh — refresh_symbols and freshness helpers."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from src.refresh import (
    DEFAULT_PRICE_LOOKBACK_YEARS,
    FreshnessInfo,
    RefreshSummary,
    get_freshness,
    refresh_symbols,
)


# ── refresh_symbols ─────────────────────────────────────────────────────────


def _stub_calls():
    """Return a dict of MagicMocks with sensible defaults."""
    return {
        "fetch_metrics": MagicMock(return_value=([], [])),
        "upsert_metrics": MagicMock(return_value=(0, 0, [])),
        "fetch_missing_prices": MagicMock(return_value=([], [])),
        "upsert_prices": MagicMock(return_value=(0, 0, [])),
        "get_price_date_range": MagicMock(return_value=(None, None)),
        "clear_caches": MagicMock(),
    }


def test_refresh_symbols_returns_empty_summary_for_empty_input():
    calls = _stub_calls()
    summary = refresh_symbols([], **calls)
    assert summary.symbols == []
    assert summary.metrics_inserted == 0
    assert summary.prices_inserted == 0
    assert summary.errors == []
    calls["fetch_metrics"].assert_not_called()
    calls["fetch_missing_prices"].assert_not_called()


def test_refresh_symbols_dedupes_and_uppercases_symbols():
    calls = _stub_calls()
    refresh_symbols(["aapl", "AAPL", " msft ", ""], **calls)
    fetched = [c.args[0] for c in calls["fetch_metrics"].call_args_list]
    assert fetched == ["AAPL", "MSFT"]


def test_refresh_symbols_pulls_metrics_and_prices_and_aggregates_counts():
    calls = _stub_calls()
    fake_metrics = [object(), object()]
    fake_prices = [object(), object(), object()]
    calls["fetch_metrics"].return_value = (fake_metrics, [])
    calls["fetch_missing_prices"].return_value = (fake_prices, [])
    calls["upsert_metrics"].return_value = (4, 1, [])
    calls["upsert_prices"].return_value = (3, 0, [])

    today = date(2026, 5, 15)
    summary = refresh_symbols(["AAPL", "MSFT"], today=today, **calls)

    # Each price-fetch call must use a (symbol, start, end) signature.
    assert calls["fetch_missing_prices"].call_count == 2
    sample_call = calls["fetch_missing_prices"].call_args_list[0]
    sym, start, end = sample_call.args
    assert sym == "AAPL"
    assert end == today
    assert start == today - timedelta(days=365 * DEFAULT_PRICE_LOOKBACK_YEARS)

    # Metrics are upserted once in bulk at the end.
    calls["upsert_metrics"].assert_called_once()
    bulk = calls["upsert_metrics"].call_args.args[0]
    assert len(bulk) == len(fake_metrics) * 2  # both symbols contributed

    # Prices are upserted per symbol.
    assert calls["upsert_prices"].call_count == 2

    assert summary.metrics_inserted == 4
    assert summary.metrics_updated == 1
    # Prices: 3 inserted per symbol, called twice
    assert summary.prices_inserted == 6
    assert summary.errors == []
    assert summary.duration_seconds >= 0
    calls["clear_caches"].assert_called_once()


def test_refresh_symbols_collects_fetcher_errors_with_symbol_prefix():
    calls = _stub_calls()
    calls["fetch_metrics"].side_effect = [
        ([], ["bad concept"]),
        ([], []),
    ]
    calls["fetch_missing_prices"].side_effect = [
        ([], ["timeout from yfinance"]),
        ([], []),
    ]
    summary = refresh_symbols(["AAPL", "MSFT"], **calls)
    # Errors are tagged with the symbol that produced them.
    assert "AAPL: bad concept" in summary.errors
    assert "AAPL: timeout from yfinance" in summary.errors
    # MSFT had no errors -> no MSFT-tagged entries
    assert not any(e.startswith("MSFT:") for e in summary.errors)


def test_refresh_symbols_uses_existing_price_range_to_minimise_fetch_window():
    calls = _stub_calls()
    today = date(2026, 5, 15)
    last_stored = date(2026, 5, 10)
    calls["get_price_date_range"].return_value = (date(2024, 1, 1), last_stored)

    refresh_symbols(["AAPL"], today=today, **calls)

    sym, start, end = calls["fetch_missing_prices"].call_args.args
    # Start should be max(default_start, min_stored) i.e. min_stored=2024-01-01
    assert start == date(2024, 1, 1)
    assert end == today


def test_refresh_symbols_progress_cb_fired_in_order():
    calls = _stub_calls()
    seen: list[tuple[str, int, int]] = []
    refresh_symbols(
        ["AAPL", "MSFT", "GOOGL"],
        progress_cb=lambda s, i, t: seen.append((s, i, t)),
        **calls,
    )
    assert seen == [("AAPL", 1, 3), ("GOOGL", 2, 3), ("MSFT", 3, 3)]


def test_refresh_symbols_progress_cb_failure_does_not_abort():
    calls = _stub_calls()

    def bad_cb(*_args, **_kw):
        raise RuntimeError("ui blew up")

    summary = refresh_symbols(["AAPL"], progress_cb=bad_cb, **calls)
    # The refresh still completes successfully.
    assert summary.errors == []
    calls["fetch_metrics"].assert_called_once_with("AAPL")


def test_refresh_symbols_requires_collaborators():
    with pytest.raises(ValueError):
        refresh_symbols(["AAPL"])  # all callables missing


# ── get_freshness ───────────────────────────────────────────────────────────


def test_get_freshness_picks_newest_period_end_across_metrics():
    metrics = {
        "AAPL": {
            "revenue": {"period_end": "2025-12-31"},
            "eps_diluted": {"period_end": "2026-03-31"},
            "ebitda": {"period_end": "2025-09-30"},
        }
    }
    prices = {
        "AAPL": {"price_date": "2026-05-13"},
    }

    info = get_freshness(
        ["aapl"],
        get_latest_metrics=lambda s: metrics.get(s, {}),
        get_latest_price=lambda s: prices.get(s),
    )["AAPL"]

    assert info.latest_metric_period_end == date(2026, 3, 31)
    assert info.latest_price_date == date(2026, 5, 13)


def test_get_freshness_handles_missing_data():
    info = get_freshness(
        ["AAPL"],
        get_latest_metrics=lambda s: {},
        get_latest_price=lambda s: None,
    )["AAPL"]
    assert info.latest_metric_period_end is None
    assert info.latest_price_date is None
    assert info.is_stale(today=date(2026, 5, 15)) is True


def test_freshness_is_stale_thresholds():
    today = date(2026, 5, 15)
    fresh = FreshnessInfo("X", date(2026, 3, 31), date(2026, 5, 13))
    stale_metric = FreshnessInfo("X", date(2025, 1, 1), date(2026, 5, 13))
    stale_price = FreshnessInfo("X", date(2026, 3, 31), date(2026, 5, 1))

    assert fresh.is_stale(today=today) is False
    assert stale_metric.is_stale(today=today) is True
    assert stale_price.is_stale(today=today) is True


def test_refresh_summary_duration_is_finite():
    s = RefreshSummary(symbols=["AAPL"])
    s.finished_at = s.started_at + timedelta(seconds=1.5)
    assert s.duration_seconds == pytest.approx(1.5)
