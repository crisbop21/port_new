"""Refresh helpers — pull the latest SEC fundamentals and market prices into the DB.

Used by the Valuation page (and any other page) to guarantee the user is looking
at fresh data, without forcing them to navigate to the Metrics / Prices pages.

The helpers are deliberately UI-free so they can be unit-tested in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Iterable

import logging

logger = logging.getLogger(__name__)


@dataclass
class RefreshSummary:
    """Result of a refresh call — counts and any errors collected per symbol."""
    symbols: list[str]
    metrics_inserted: int = 0
    metrics_updated: int = 0
    prices_inserted: int = 0
    prices_updated: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None

    @property
    def duration_seconds(self) -> float:
        end = self.finished_at or datetime.utcnow()
        return (end - self.started_at).total_seconds()

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)


# Default lookback for prices when a symbol has no stored price history yet.
DEFAULT_PRICE_LOOKBACK_YEARS = 5


def refresh_symbols(
    symbols: Iterable[str],
    *,
    fetch_metrics: Callable | None = None,
    upsert_metrics: Callable | None = None,
    fetch_missing_prices: Callable | None = None,
    upsert_prices: Callable | None = None,
    get_price_date_range: Callable | None = None,
    clear_caches: Callable | None = None,
    today: date | None = None,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> RefreshSummary:
    """Refresh SEC fundamentals + missing prices for ``symbols`` and write to DB.

    All collaborators are injected so this function is trivially unit-testable.
    Production callers will pass the real ``src.fetcher`` / ``src.price_fetcher``
    / ``src.db`` helpers — see :func:`refresh_symbols_default` below.

    Args:
        symbols: Iterable of ticker symbols to refresh (deduped + uppercased).
        fetch_metrics: callable(symbol) -> (list[StockMetric], list[str])
        upsert_metrics: callable(list[StockMetric]) -> (inserted, updated, errors)
        fetch_missing_prices: callable(symbol, start, end) -> (list[DailyPrice], errors)
        upsert_prices: callable(list[DailyPrice]) -> (inserted, updated, errors)
        get_price_date_range: callable(symbol) -> (min_date|None, max_date|None)
        clear_caches: optional callable() to invalidate Streamlit query caches.
        today: override "today" for deterministic tests.
        progress_cb: optional callback (symbol, idx, total) for UI progress bars.

    Returns:
        :class:`RefreshSummary` with row counts, errors and timing info.
    """
    if (fetch_metrics is None or upsert_metrics is None
            or fetch_missing_prices is None or upsert_prices is None
            or get_price_date_range is None):
        raise ValueError(
            "refresh_symbols requires fetch/upsert callables — "
            "use refresh_symbols_default for the production wiring."
        )

    syms = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    summary = RefreshSummary(symbols=syms)
    if not syms:
        summary.finished_at = datetime.utcnow()
        return summary

    today = today or date.today()
    default_start = today - timedelta(days=365 * DEFAULT_PRICE_LOOKBACK_YEARS)

    all_metrics: list = []
    for idx, sym in enumerate(syms, start=1):
        if progress_cb is not None:
            try:
                progress_cb(sym, idx, len(syms))
            except Exception:  # progress bars must never break the refresh
                logger.exception("progress_cb raised for %s", sym)
        try:
            metrics, fetch_errors = fetch_metrics(sym)
        except Exception as exc:  # pragma: no cover — defensive
            summary.errors.append(f"{sym}: metrics fetch crashed: {exc}")
            logger.exception("Metrics fetch crashed for %s", sym)
            continue

        if fetch_errors:
            summary.errors.extend(f"{sym}: {e}" for e in fetch_errors)
        if metrics:
            all_metrics.extend(metrics)

        # Prices: fetch only the gap between latest stored and today.
        try:
            min_d, max_d = get_price_date_range(sym)
        except Exception as exc:  # pragma: no cover — defensive
            summary.errors.append(f"{sym}: price-range lookup crashed: {exc}")
            logger.exception("Price-range lookup crashed for %s", sym)
            min_d, max_d = (None, None)

        price_start = default_start if min_d is None else max(default_start, min_d)
        try:
            prices, price_errors = fetch_missing_prices(sym, price_start, today)
        except Exception as exc:  # pragma: no cover — defensive
            summary.errors.append(f"{sym}: price fetch crashed: {exc}")
            logger.exception("Price fetch crashed for %s", sym)
            prices, price_errors = ([], [])

        if price_errors:
            summary.errors.extend(f"{sym}: {e}" for e in price_errors)

        if prices:
            try:
                p_ins, p_upd, p_errs = upsert_prices(prices)
            except Exception as exc:  # pragma: no cover — defensive
                summary.errors.append(f"{sym}: price upsert crashed: {exc}")
                logger.exception("Price upsert crashed for %s", sym)
                p_ins, p_upd, p_errs = (0, 0, [])
            summary.prices_inserted += int(p_ins or 0)
            summary.prices_updated += int(p_upd or 0)
            if p_errs:
                summary.errors.extend(f"{sym}: {e}" for e in p_errs)

    # Single bulk metrics upsert at the end for efficiency.
    if all_metrics:
        try:
            m_ins, m_upd, m_errs = upsert_metrics(all_metrics)
        except Exception as exc:  # pragma: no cover — defensive
            summary.errors.append(f"metrics upsert crashed: {exc}")
            logger.exception("Bulk metrics upsert crashed")
            m_ins, m_upd, m_errs = (0, 0, [])
        summary.metrics_inserted += int(m_ins or 0)
        summary.metrics_updated += int(m_upd or 0)
        if m_errs:
            summary.errors.extend(m_errs)

    if clear_caches is not None:
        try:
            clear_caches()
        except Exception:  # pragma: no cover — defensive
            logger.exception("clear_caches raised")

    summary.finished_at = datetime.utcnow()
    return summary


def refresh_symbols_default(
    symbols: Iterable[str],
    *,
    today: date | None = None,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> RefreshSummary:
    """Production wiring — uses real fetcher + db helpers."""
    # Imported lazily so test suites don't hit Streamlit / Supabase on import.
    from src.fetcher import fetch_metrics_for_symbol
    from src.price_fetcher import fetch_missing_prices
    from src.db import (
        upsert_stock_metrics,
        upsert_daily_prices,
        get_price_date_range,
        clear_query_caches,
    )
    return refresh_symbols(
        symbols,
        fetch_metrics=fetch_metrics_for_symbol,
        upsert_metrics=upsert_stock_metrics,
        fetch_missing_prices=fetch_missing_prices,
        upsert_prices=upsert_daily_prices,
        get_price_date_range=get_price_date_range,
        clear_caches=clear_query_caches,
        today=today,
        progress_cb=progress_cb,
    )


# ── Freshness helpers ───────────────────────────────────────────────────────


@dataclass
class FreshnessInfo:
    """Latest known data points per symbol — used by the UI freshness badge."""
    symbol: str
    latest_metric_period_end: date | None
    latest_price_date: date | None

    def is_stale(self, *, today: date | None = None,
                 metric_max_age_days: int = 100,
                 price_max_age_days: int = 4) -> bool:
        """Return True when either source is older than the allowed thresholds."""
        today = today or date.today()
        if self.latest_metric_period_end is None or self.latest_price_date is None:
            return True
        if (today - self.latest_metric_period_end).days > metric_max_age_days:
            return True
        if (today - self.latest_price_date).days > price_max_age_days:
            return True
        return False


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def get_freshness(
    symbols: Iterable[str],
    *,
    get_latest_metrics: Callable | None = None,
    get_latest_price: Callable | None = None,
) -> dict[str, FreshnessInfo]:
    """Return per-symbol freshness info for the latest metric + price."""
    if get_latest_metrics is None or get_latest_price is None:
        from src.db import get_latest_stock_metrics, get_latest_price as _glp
        get_latest_metrics = get_latest_stock_metrics
        get_latest_price = _glp

    out: dict[str, FreshnessInfo] = {}
    for sym in {s.strip().upper() for s in symbols if s and s.strip()}:
        latest_metrics = get_latest_metrics(sym) or {}
        # Pick the newest period_end across all metrics.
        latest_period: date | None = None
        for row in latest_metrics.values():
            d = _coerce_date(row.get("period_end") if isinstance(row, dict) else None)
            if d is not None and (latest_period is None or d > latest_period):
                latest_period = d

        latest_price_row = get_latest_price(sym) or {}
        latest_price_date = _coerce_date(
            latest_price_row.get("price_date") if isinstance(latest_price_row, dict) else None
        )

        out[sym] = FreshnessInfo(
            symbol=sym,
            latest_metric_period_end=latest_period,
            latest_price_date=latest_price_date,
        )
    return out
