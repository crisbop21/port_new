"""Supabase database layer for the IBKR Trade Journal.

Tables: statements, positions, trades, stock_metrics (see sql/).
All financial columns use Postgres numeric via Python Decimal.
"""

import logging
import os
from datetime import date, datetime
from decimal import Decimal

import streamlit as st
from supabase import Client, create_client

from src.models import (
    DailyPrice,
    ParsedStatement,
    Position,
    StockMetric,
    Trade,
    ValuationSnapshot,
)

logger = logging.getLogger(__name__)

OPTION_MULTIPLIER = 100


# ── Client ───────────────────────────────────────────────────────────────────

@st.cache_resource
def get_client() -> Client:
    """Return a cached Supabase client. Reads SUPABASE_URL and SUPABASE_KEY."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


# ── Serialisation helpers ────────────────────────────────────────────────────

def _ser(value) -> str | None:
    """Serialise Decimal/date/datetime for JSON transport to Supabase."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _position_row(pos: Position, statement_id: str) -> dict:
    return {
        "statement_id": statement_id,
        "symbol": pos.symbol,
        "asset_class": pos.asset_class,
        "quantity": _ser(pos.quantity),
        "cost_basis": _ser(pos.cost_basis),
        "market_price": _ser(pos.market_price),
        "market_value": _ser(pos.market_value),
        "unrealized_pnl": _ser(pos.unrealized_pnl),
        "currency": pos.currency,
        "statement_date": _ser(pos.statement_date),
        "expiry": _ser(pos.expiry),
        "strike": _ser(pos.strike),
        "right": pos.right,
    }


def _trade_row(trade: Trade, statement_id: str) -> dict:
    return {
        "statement_id": statement_id,
        "symbol": trade.symbol,
        "asset_class": trade.asset_class,
        "trade_date": _ser(trade.trade_date),
        "side": trade.side,
        "quantity": _ser(trade.quantity),
        "price": _ser(trade.price),
        "proceeds": _ser(trade.proceeds),
        "commission": _ser(trade.commission),
        "realized_pnl": _ser(trade.realized_pnl),
        "currency": trade.currency,
        "expiry": _ser(trade.expiry),
        "strike": _ser(trade.strike),
        "right": trade.right,
    }


# ── Key / fingerprint helpers ────────────────────────────────────────────────

def _position_key(row: dict) -> tuple:
    """Unique key for a position/trade: includes option fields for OPT."""
    if row.get("asset_class") == "OPT":
        return (row["symbol"], row.get("expiry"), row.get("strike"), row.get("right"))
    return (row["symbol"],)


def _trade_fingerprint(t: dict) -> tuple:
    """Fingerprint for deduplicating the same trade across overlapping PDFs."""
    return (
        t.get("trade_date"), t.get("symbol"), t.get("asset_class"),
        t.get("side"), str(t.get("quantity")), str(t.get("price")),
    )


def _position_fingerprint(p: dict) -> tuple:
    """Fingerprint for deduplicating the same position across overlapping PDFs."""
    return (
        p.get("symbol"), p.get("asset_class"), p.get("statement_date"),
        p.get("expiry"), p.get("strike"), p.get("right"),
    )


def _get_account_statement_ids(account_id: str) -> list[str]:
    """Get all statement IDs for an account."""
    result = (
        get_client()
        .table("statements")
        .select("id")
        .eq("account_id", account_id)
        .execute()
    )
    return [s["id"] for s in result.data] if result.data else []


def _get_existing_trade_fingerprints(account_id: str, exclude_statement_id: str | None = None) -> set[tuple]:
    """Collect fingerprints of all trades already stored for an account."""
    stmt_ids = _get_account_statement_ids(account_id)
    if exclude_statement_id:
        stmt_ids = [s for s in stmt_ids if s != exclude_statement_id]
    if not stmt_ids:
        return set()

    fingerprints: set[tuple] = set()
    client = get_client()
    for sid in stmt_ids:
        trades = (
            client.table("trades")
            .select("trade_date,symbol,asset_class,side,quantity,price")
            .eq("statement_id", sid)
            .execute()
        )
        for t in trades.data:
            fingerprints.add(_trade_fingerprint(t))
    return fingerprints


def _get_existing_position_fingerprints(
    account_id: str, exclude_statement_id: str | None = None,
) -> set[tuple]:
    """Collect fingerprints of all positions already stored for an account."""
    stmt_ids = _get_account_statement_ids(account_id)
    if exclude_statement_id:
        stmt_ids = [s for s in stmt_ids if s != exclude_statement_id]
    if not stmt_ids:
        return set()

    fingerprints: set[tuple] = set()
    client = get_client()
    for sid in stmt_ids:
        positions = (
            client.table("positions")
            .select("symbol,asset_class,statement_date,expiry,strike,right")
            .eq("statement_id", sid)
            .execute()
        )
        for p in positions.data:
            fingerprints.add(_position_fingerprint(p))
    return fingerprints


# ── Duplicate analysis ────────────────────────────────────────────────────────


def check_duplicates(parsed: ParsedStatement) -> dict:
    """Analyse a parsed statement against the DB to find new vs duplicate data.

    Returns a dict with keys:
        new_trades, dup_trades, new_positions, dup_positions,
        existing_statement_id (UUID str or None).
    """
    client = get_client()
    meta = parsed.meta

    # Check if this exact statement period already exists
    try:
        existing_stmt = (
            client.table("statements")
            .select("id")
            .eq("account_id", meta.account_id)
            .eq("period_start", _ser(meta.period_start))
            .eq("period_end", _ser(meta.period_end))
            .execute()
        )
        existing_statement_id = (
            existing_stmt.data[0]["id"] if existing_stmt.data else None
        )
    except Exception:
        existing_statement_id = None

    # Trade duplicates
    existing_trade_fps = _get_existing_trade_fingerprints(
        meta.account_id, exclude_statement_id=existing_statement_id,
    )
    new_trades = 0
    dup_trades = 0
    for t in parsed.trades:
        row = _trade_row(t, "dummy")
        fp = _trade_fingerprint(row)
        if fp in existing_trade_fps:
            dup_trades += 1
        else:
            new_trades += 1

    # Positions are always stored per-statement (no cross-statement dedup),
    # so every position in the parsed statement is "new" for this statement.
    new_positions = len(parsed.positions)
    dup_positions = 0

    return {
        "new_trades": new_trades,
        "dup_trades": dup_trades,
        "new_positions": new_positions,
        "dup_positions": dup_positions,
        "existing_statement_id": existing_statement_id,
    }


# ── Upsert ───────────────────────────────────────────────────────────────────


def get_existing_period(account_id: str) -> tuple[date, date] | None:
    """Return the widest (period_start, period_end) across all statements for an account."""
    try:
        result = (
            get_client()
            .table("statements")
            .select("period_start,period_end")
            .eq("account_id", account_id)
            .execute()
        )
        if result.data:
            starts = [date.fromisoformat(r["period_start"]) for r in result.data]
            ends = [date.fromisoformat(r["period_end"]) for r in result.data]
            return (min(starts), max(ends))
    except Exception:
        logger.debug("Could not fetch existing period for %s", account_id)
    return None


def upsert_statement(parsed: ParsedStatement) -> tuple[str, int]:
    """Idempotently save a parsed statement to Supabase.

    Upserts the statement row on (account_id, period_start, period_end)
    conflict, then replaces positions and deduplicates trades for that
    statement.  Multiple PDFs for the same account with different periods
    coexist; trades that already exist in another statement are skipped.

    Returns:
        (statement_id, trades_skipped, positions_skipped) — the UUID and
        how many duplicate trades/positions were filtered out.

    Raises:
        st.error and re-raises on any Supabase/network failure.
    """
    client = get_client()
    meta = parsed.meta

    try:
        # Upsert statement row (unique on account + period)
        stmt_data = {
            "account_id": meta.account_id,
            "period_start": _ser(meta.period_start),
            "period_end": _ser(meta.period_end),
            "base_currency": meta.base_currency,
        }
        result = (
            client.table("statements")
            .upsert(stmt_data, on_conflict="account_id,period_start,period_end")
            .execute()
        )
        if not result.data:
            raise RuntimeError(
                "Statement upsert returned no data. "
                "Check that RLS policies allow INSERT/UPDATE on the 'statements' table "
                "for your API key."
            )
        statement_id = result.data[0]["id"]

        # Delete old child rows for THIS statement only
        client.table("positions").delete().eq(
            "statement_id", statement_id
        ).execute()
        client.table("trades").delete().eq(
            "statement_id", statement_id
        ).execute()

        # Insert positions — each statement keeps its own complete snapshot.
        # No cross-statement dedup: if another statement has the same position,
        # we still store it here so that deleting/re-uploading another statement
        # can never cause positions to vanish.
        positions_skipped = 0
        if parsed.positions:
            pos_rows = [_position_row(p, statement_id) for p in parsed.positions]
            pos_result = client.table("positions").insert(pos_rows).execute()
            if not pos_result.data:
                raise RuntimeError(
                    f"Positions insert returned no data ({len(pos_rows)} rows sent). "
                    "Check RLS policies on the 'positions' table."
                )

        # Insert trades — deduplicate against existing trades in other statements
        trades_skipped = 0
        if parsed.trades:
            existing_fps = _get_existing_trade_fingerprints(
                meta.account_id, exclude_statement_id=statement_id,
            )
            trade_rows = []
            for t in parsed.trades:
                fp = _trade_fingerprint(_trade_row(t, statement_id))
                if fp in existing_fps:
                    trades_skipped += 1
                    continue
                trade_rows.append(_trade_row(t, statement_id))

            if trade_rows:
                trade_result = client.table("trades").insert(trade_rows).execute()
                if not trade_result.data:
                    raise RuntimeError(
                        f"Trades insert returned no data ({len(trade_rows)} rows sent). "
                        "Check RLS policies on the 'trades' table."
                    )

        logger.info(
            "Upserted statement %s: %d positions (%d dupes), %d trades (%d dupes)",
            statement_id,
            len(parsed.positions) - positions_skipped, positions_skipped,
            len(parsed.trades) - trades_skipped, trades_skipped,
        )
        return statement_id, trades_skipped, positions_skipped

    except Exception as e:
        logger.exception("Failed to upsert statement for %s", meta.account_id)
        st.error(f"Database error saving statement: {e}")
        raise


def clear_query_caches() -> None:
    """Clear all cached query results so fresh data is fetched."""
    get_statements.clear()
    get_positions.clear()
    get_trades.clear()
    get_account_ids.clear()
    _get_account_statements.clear()
    get_snapshot_dates.clear()
    get_positions_as_of.clear()
    get_stock_metrics.clear()
    get_latest_stock_metrics.clear()
    get_portfolio_symbols.clear()
    get_daily_prices.clear()
    get_latest_price.clear()


# ── Queries ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def get_statements() -> list[dict]:
    """Fetch all statements, ordered by period descending."""
    try:
        result = (
            get_client()
            .table("statements")
            .select("*")
            .order("period_end", desc=True)
            .execute()
        )
        return result.data
    except Exception as e:
        logger.exception("Failed to fetch statements")
        st.error(f"Database error fetching statements: {e}")
        return []


@st.cache_data(ttl=60)
def get_positions(statement_id: str) -> list[dict]:
    """Fetch positions for a given statement."""
    try:
        result = (
            get_client()
            .table("positions")
            .select("*")
            .eq("statement_id", statement_id)
            .execute()
        )
        return result.data
    except Exception as e:
        logger.exception("Failed to fetch positions for %s", statement_id)
        st.error(f"Database error fetching positions: {e}")
        return []


@st.cache_data(ttl=60)
def get_account_ids() -> list[str]:
    """Return distinct account IDs from all uploaded statements."""
    try:
        result = (
            get_client()
            .table("statements")
            .select("account_id")
            .order("account_id")
            .execute()
        )
        # Deduplicate while preserving order
        seen: set[str] = set()
        ids: list[str] = []
        for row in result.data:
            aid = row["account_id"]
            if aid not in seen:
                seen.add(aid)
                ids.append(aid)
        return ids
    except Exception as e:
        logger.exception("Failed to fetch account IDs")
        st.error(f"Database error fetching account IDs: {e}")
        return []


@st.cache_data(ttl=60)
def get_trades(
    statement_id: str | None = None,
    account_id: str | None = None,
    symbol: str | None = None,
    asset_class: str | None = None,
    side: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict]:
    """Fetch trades with optional filters.

    Args:
        statement_id: Filter by statement.
        account_id: Filter by account (looks up statement IDs for the account).
        symbol: Filter by ticker symbol.
        asset_class: Filter by asset class (STK, OPT, ETF).
        side: Filter by side (BOT, SLD).
        date_from: Trades on or after this date.
        date_to: Trades on or before this date.
    """
    try:
        query = get_client().table("trades").select("*")

        if statement_id:
            query = query.eq("statement_id", statement_id)
        elif account_id:
            stmt_ids = _get_account_statement_ids(account_id)
            if not stmt_ids:
                return []
            query = query.in_("statement_id", stmt_ids)
        if symbol:
            query = query.eq("symbol", symbol)
        if asset_class:
            query = query.eq("asset_class", asset_class)
        if side:
            query = query.eq("side", side)
        if date_from:
            query = query.gte("trade_date", date_from.isoformat())
        if date_to:
            # Include the full day
            query = query.lte("trade_date", f"{date_to.isoformat()}T23:59:59")

        result = query.order("trade_date", desc=True).execute()
        return result.data

    except Exception as e:
        logger.exception("Failed to fetch trades")
        st.error(f"Database error fetching trades: {e}")
        return []


@st.cache_data(ttl=60)
def _get_account_statements(account_id: str) -> list[dict]:
    """Fetch all statements for an account, ordered by period_end ascending."""
    try:
        result = (
            get_client()
            .table("statements")
            .select("id,period_start,period_end")
            .eq("account_id", account_id)
            .order("period_end", desc=False)
            .execute()
        )
        return result.data or []
    except Exception:
        logger.debug("Could not fetch statements for %s", account_id)
        return []


# ── Reconciliation (statement_date-driven) ───────────────────────────────────


@st.cache_data(ttl=60)
def get_snapshot_dates(account_id: str) -> list[date]:
    """Return sorted distinct statement_date values from positions for an account."""
    stmt_ids = _get_account_statement_ids(account_id)
    if not stmt_ids:
        return []

    try:
        result = (
            get_client()
            .table("positions")
            .select("statement_date")
            .in_("statement_id", stmt_ids)
            .order("statement_date", desc=False)
            .execute()
        )
        seen: set[str] = set()
        dates: list[date] = []
        for row in result.data:
            d = row["statement_date"]
            if d not in seen:
                seen.add(d)
                dates.append(date.fromisoformat(d) if isinstance(d, str) else d)
        return dates
    except Exception as e:
        logger.exception("Failed to fetch snapshot dates for %s", account_id)
        st.error(f"Database error fetching snapshot dates: {e}")
        return []


@st.cache_data(ttl=60)
def get_positions_as_of(account_id: str, as_of_date: date) -> list[dict]:
    """Fetch all positions for an account at a specific statement_date.

    Deduplicates by position key (symbol + option fields) keeping the last
    occurrence (in case overlapping PDFs inserted duplicates).
    """
    stmt_ids = _get_account_statement_ids(account_id)
    if not stmt_ids:
        return []

    try:
        result = (
            get_client()
            .table("positions")
            .select("*")
            .in_("statement_id", stmt_ids)
            .eq("statement_date", as_of_date.isoformat())
            .execute()
        )
        # Deduplicate by position key — last wins
        deduped: dict[tuple, dict] = {}
        for p in result.data:
            key = _position_key(p)
            deduped[key] = p
        return list(deduped.values())
    except Exception as e:
        logger.exception("Failed to fetch positions as of %s for %s", as_of_date, account_id)
        st.error(f"Database error fetching positions: {e}")
        return []


def get_trades_between(account_id: str, after_date: date, up_to_date: date) -> list[dict]:
    """Fetch deduplicated trades for an account where after_date < trade_date <= up_to_date."""
    stmt_ids = _get_account_statement_ids(account_id)
    if not stmt_ids:
        return []

    try:
        result = (
            get_client()
            .table("trades")
            .select("*")
            .in_("statement_id", stmt_ids)
            .gt("trade_date", after_date.isoformat())
            .lte("trade_date", f"{up_to_date.isoformat()}T23:59:59")
            .order("trade_date", desc=False)
            .execute()
        )
    except Exception as e:
        logger.exception("Failed to fetch trades between %s and %s", after_date, up_to_date)
        st.error(f"Database error fetching trades: {e}")
        return []

    seen: set[tuple] = set()
    unique: list[dict] = []
    for t in result.data:
        fp = _trade_fingerprint(t)
        if fp not in seen:
            seen.add(fp)
            unique.append(t)
    return unique


def reconcile_pair(
    account_id: str,
    base_date: date,
    target_date: date,
) -> dict:
    """Reconcile holdings between two snapshot dates for an account.

    Rolls forward from holdings at *base_date* through all trades in
    (base_date, target_date] and compares the result against the actual
    holdings snapshot at *target_date*.

    Returns a dict with:
        base_date: str
        target_date: str
        ok: bool — True if reconstructed matches the actual snapshot
        holdings: dict[str, dict] — keyed by symbol, each containing:
            base_qty: str
            trades: list[dict] — each trade with date, side, qty, running_qty
            reconstructed_qty: str — running qty after all trades
            expected_qty: str — qty in the target snapshot (or "0" if absent)
            match: bool
            diff: str — reconstructed - expected
        gaps: dict with:
            missing_from_target: list[dict] — reconstructed but not in target
            missing_from_reconstruction: list[dict] — in target but not reconstructed
    """
    # ── Load base snapshot ───────────────────────────────────────────────────
    base_positions = get_positions_as_of(account_id, base_date)

    # ledger keyed by position key
    ledger: dict[tuple, dict] = {}
    for p in base_positions:
        key = _position_key(p)
        ledger[key] = {
            "symbol": p["symbol"],
            "asset_class": p["asset_class"],
            "base_qty": Decimal(str(p["quantity"])),
            "trades": [],
            "running_qty": Decimal(str(p["quantity"])),
        }

    # ── Apply trades ─────────────────────────────────────────────────────────
    trades = get_trades_between(account_id, base_date, target_date)
    for t in trades:
        key = _position_key(t)
        qty = Decimal(str(t["quantity"]))

        if key not in ledger:
            ledger[key] = {
                "symbol": t["symbol"],
                "asset_class": t["asset_class"],
                "base_qty": Decimal("0"),
                "trades": [],
                "running_qty": Decimal("0"),
            }

        if t["side"] == "BOT":
            ledger[key]["running_qty"] += qty
        else:
            ledger[key]["running_qty"] -= qty

        td_raw = t.get("trade_date", "")
        trade_dt = td_raw[:10] if isinstance(td_raw, str) else str(td_raw)

        ledger[key]["trades"].append({
            "date": trade_dt,
            "side": t["side"],
            "quantity": str(qty),
            "running_qty": str(ledger[key]["running_qty"]),
        })

    # ── Load target snapshot ─────────────────────────────────────────────────
    target_positions = get_positions_as_of(account_id, target_date)
    actual: dict[tuple, Decimal] = {}
    for p in target_positions:
        key = _position_key(p)
        actual[key] = Decimal(str(p["quantity"]))
        if key not in ledger:
            ledger[key] = {
                "symbol": p["symbol"],
                "asset_class": p["asset_class"],
                "base_qty": Decimal("0"),
                "trades": [],
                "running_qty": Decimal("0"),
            }

    # ── Compare and build result ─────────────────────────────────────────────
    all_ok = True
    holdings: dict[str, dict] = {}
    missing_from_target: list[dict] = []
    missing_from_reconstruction: list[dict] = []

    for key in sorted(ledger.keys(), key=lambda k: k[0]):
        entry = ledger[key]
        reconstructed_qty = entry["running_qty"]
        expected_qty = actual.get(key, Decimal("0"))

        # Skip expired options with zero quantity on both sides
        if entry["asset_class"] == "OPT" and len(key) == 4 and key[1]:
            expiry_val = key[1]
            expiry_date = (
                date.fromisoformat(expiry_val)
                if isinstance(expiry_val, str) else expiry_val
            )
            if expiry_date < target_date and reconstructed_qty <= 0 and expected_qty <= 0:
                continue

        match = reconstructed_qty == expected_qty
        if not match:
            all_ok = False

        diff = reconstructed_qty - expected_qty
        display_key = entry["symbol"]

        holdings[display_key] = {
            "base_qty": str(entry["base_qty"]),
            "trades": entry["trades"],
            "reconstructed_qty": str(reconstructed_qty),
            "expected_qty": str(expected_qty),
            "match": match,
            "diff": str(diff),
        }

        # Categorise gaps
        if key not in actual and reconstructed_qty > 0:
            missing_from_target.append({
                "symbol": display_key,
                "reconstructed_qty": str(reconstructed_qty),
            })
        elif key in actual and reconstructed_qty <= 0 and expected_qty > 0:
            missing_from_reconstruction.append({
                "symbol": display_key,
                "expected_qty": str(expected_qty),
            })

    return {
        "base_date": base_date.isoformat(),
        "target_date": target_date.isoformat(),
        "ok": all_ok,
        "holdings": holdings,
        "gaps": {
            "missing_from_target": missing_from_target,
            "missing_from_reconstruction": missing_from_reconstruction,
        },
    }


def reconcile_account(account_id: str) -> list[dict]:
    """Run reconciliation across ALL consecutive snapshot pairs for an account.

    Returns a list of per-pair results (same shape as reconcile_pair output),
    one for each consecutive pair of snapshot dates.  Returns an empty list
    if fewer than 2 snapshot dates exist.
    """
    snapshot_dates = get_snapshot_dates(account_id)
    if len(snapshot_dates) < 2:
        return []

    results: list[dict] = []
    for i in range(len(snapshot_dates) - 1):
        pair_result = reconcile_pair(
            account_id, snapshot_dates[i], snapshot_dates[i + 1],
        )
        results.append(pair_result)
    return results


# ── Stock metrics ────────────────────────────────────────────────────────────


def _check_metric_columns() -> set[str]:
    """Detect which columns exist on stock_metrics table.

    Returns a set of column names that are safe to include in upsert rows.
    Caches the result for the session to avoid repeated queries.
    """
    if hasattr(_check_metric_columns, "_cache"):
        return _check_metric_columns._cache

    # These columns always exist (from 004 + 005 migrations)
    base_cols = {
        "symbol", "metric_name", "metric_value", "period_end",
        "period_start", "fiscal_period", "source", "cik", "filing_type",
    }

    # Columns added by 007_reporting_frequency.sql
    new_cols = {"fiscal_year", "duration_days", "reporting_style"}

    try:
        # Probe by selecting the new columns; if they don't exist, Supabase
        # returns a PGRST204 error.
        client = get_client()
        result = (
            client.table("stock_metrics")
            .select("fiscal_year")
            .limit(1)
            .execute()
        )
        # If we get here without error, the columns exist
        _check_metric_columns._cache = base_cols | new_cols
    except Exception:
        logger.info(
            "stock_metrics table missing new columns (fiscal_year, duration_days, "
            "reporting_style). Run sql/007_reporting_frequency.sql to enable them."
        )
        _check_metric_columns._cache = base_cols

    return _check_metric_columns._cache


def _metric_row(metric: StockMetric) -> dict:
    """Serialise a StockMetric to a dict for Supabase insert.

    Only includes columns that exist in the DB schema to avoid
    PGRST204 errors when migration 007 hasn't been applied yet.
    """
    allowed = _check_metric_columns()

    row = {
        "symbol": metric.symbol,
        "metric_name": metric.metric_name,
        "metric_value": _ser(metric.metric_value),
        "period_end": _ser(metric.period_end),
        "period_start": _ser(metric.period_start),
        "fiscal_period": metric.fiscal_period,
        "source": metric.source,
        "cik": metric.cik,
        "filing_type": metric.filing_type,
    }

    # Only include new columns if they exist in the DB
    if "fiscal_year" in allowed:
        row["fiscal_year"] = metric.fiscal_year
    if "duration_days" in allowed:
        row["duration_days"] = metric.duration_days
    if "reporting_style" in allowed:
        row["reporting_style"] = metric.reporting_style

    return row


def _metric_fingerprint(row: dict) -> tuple:
    """Fingerprint for deduplicating stock metrics."""
    return (row.get("symbol"), row.get("metric_name"), row.get("period_end"), row.get("fiscal_period"))


def upsert_stock_metrics(
    metrics: list[StockMetric],
) -> tuple[int, int, list[str]]:
    """Save stock metrics to Supabase, skipping duplicates.

    Uses the DB unique constraint (symbol, metric_name, period_end) via
    upsert to handle conflicts — existing rows get their value updated,
    new rows are inserted.

    Returns:
        (inserted, updated, errors) — counts and a list of error messages.
    """
    if not metrics:
        return 0, 0, []

    client = get_client()
    errors: list[str] = []
    inserted = 0
    updated = 0

    # Fetch existing fingerprints to know what's an insert vs update
    symbols = list({m.symbol for m in metrics})
    existing_fps: set[tuple] = set()
    try:
        for sym in symbols:
            result = (
                client.table("stock_metrics")
                .select("symbol,metric_name,period_end")
                .eq("symbol", sym)
                .execute()
            )
            for row in result.data:
                existing_fps.add(_metric_fingerprint(row))
    except Exception as e:
        msg = f"Failed to check existing metrics: {e}"
        logger.error(msg)
        errors.append(msg)
        # Continue anyway — upsert will handle conflicts

    rows = [_metric_row(m) for m in metrics]

    # Count what's new vs what will be updated
    for row in rows:
        fp = _metric_fingerprint(row)
        if fp in existing_fps:
            updated += 1
        else:
            inserted += 1

    # Upsert in batches (Supabase has payload limits)
    batch_size = 50
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            result = (
                client.table("stock_metrics")
                .upsert(batch, on_conflict="symbol,metric_name,period_end,fiscal_period")
                .execute()
            )
            if not result.data:
                msg = (
                    f"Metrics upsert returned no data (batch {i // batch_size + 1}, "
                    f"{len(batch)} rows). Check RLS policies on 'stock_metrics' table."
                )
                logger.error(msg)
                errors.append(msg)
        except Exception as e:
            msg = f"Metrics upsert failed (batch {i // batch_size + 1}): {e}"
            logger.exception(msg)
            errors.append(msg)

    logger.info(
        "Stock metrics upsert: %d inserted, %d updated, %d errors",
        inserted, updated, len(errors),
    )
    return inserted, updated, errors


def delete_stock_metrics(symbols: list[str]) -> tuple[int, list[str]]:
    """Delete all stock metrics for the given symbols.

    Used for clean re-fetch: wipe old data before inserting fresh data
    with updated reporting_style, fiscal_year, and duration_days fields.

    Returns:
        (deleted_count, errors) — count of deleted rows and error messages.
    """
    if not symbols:
        return 0, []

    client = get_client()
    errors: list[str] = []
    total_deleted = 0

    for sym in symbols:
        try:
            result = (
                client.table("stock_metrics")
                .delete()
                .eq("symbol", sym.upper().strip())
                .execute()
            )
            count = len(result.data) if result.data else 0
            total_deleted += count
            logger.info("Deleted %d metrics for %s", count, sym)
        except Exception as e:
            msg = f"Failed to delete metrics for {sym}: {e}"
            logger.error(msg)
            errors.append(msg)

    return total_deleted, errors


@st.cache_data(ttl=60)
def get_stock_metrics(
    symbol: str | None = None,
    metric_name: str | None = None,
) -> list[dict]:
    """Fetch stock metrics with optional filters.

    Returns rows ordered by period_end descending.
    """
    try:
        query = get_client().table("stock_metrics").select("*")
        if symbol:
            query = query.eq("symbol", symbol.upper())
        if metric_name:
            query = query.eq("metric_name", metric_name)
        result = query.order("period_end", desc=True).execute()
        return result.data
    except Exception as e:
        logger.exception("Failed to fetch stock metrics (symbol=%s, metric=%s)", symbol, metric_name)
        st.error(f"Database error fetching stock metrics: {e}")
        return []


@st.cache_data(ttl=60)
def get_latest_stock_metrics(symbol: str) -> dict[str, dict]:
    """Fetch the most recent value of each metric for a symbol.

    Returns a dict keyed by metric_name, each value being the full row dict.
    Example: {"revenue": {"metric_value": "123456", "period_end": "2025-12-31", ...}}
    """
    all_metrics = get_stock_metrics(symbol=symbol)
    latest: dict[str, dict] = {}
    for row in all_metrics:
        name = row["metric_name"]
        if name not in latest:
            latest[name] = row  # already sorted by period_end desc
    return latest


@st.cache_data(ttl=60)
def get_portfolio_symbols(account_id: str | None = None) -> list[str]:
    """Return unique stock/ETF symbols from the latest positions.

    Excludes options (OPT) since they share the underlying symbol.
    """
    try:
        statements = get_statements()
        if not statements:
            return []

        if account_id:
            statements = [s for s in statements if s["account_id"] == account_id]
        if not statements:
            return []

        # Get latest statement per account
        latest_by_account: dict[str, dict] = {}
        for s in statements:
            acct = s["account_id"]
            if acct not in latest_by_account:
                latest_by_account[acct] = s

        symbols: set[str] = set()
        for s in latest_by_account.values():
            positions = get_positions(s["id"])
            for p in positions:
                if p["asset_class"] in ("STK", "ETF"):
                    symbols.add(p["symbol"])

        return sorted(symbols)
    except Exception as e:
        logger.exception("Failed to get portfolio symbols")
        st.error(f"Database error fetching portfolio symbols: {e}")
        return []


# ── Daily prices ─────────────────────────────────────────────────────────────


def _price_row(price: DailyPrice) -> dict:
    """Serialise a DailyPrice to a dict for Supabase insert."""
    return {
        "symbol": price.symbol,
        "price_date": _ser(price.price_date),
        "open": _ser(price.open),
        "high": _ser(price.high),
        "low": _ser(price.low),
        "close": _ser(price.close),
        "adj_close": _ser(price.adj_close),
        "volume": price.volume,
    }


def upsert_daily_prices(
    prices: list[DailyPrice],
) -> tuple[int, int, list[str]]:
    """Save daily prices to Supabase, upserting on (symbol, price_date).

    Returns:
        (inserted, updated, errors)
    """
    if not prices:
        return 0, 0, []

    client = get_client()
    errors: list[str] = []
    inserted = 0
    updated = 0

    # Check existing to distinguish insert vs update
    symbols = list({p.symbol for p in prices})
    existing_keys: set[tuple] = set()
    try:
        for sym in symbols:
            result = (
                client.table("daily_prices")
                .select("symbol,price_date")
                .eq("symbol", sym)
                .execute()
            )
            for row in result.data:
                existing_keys.add((row["symbol"], row["price_date"]))
    except Exception as e:
        msg = f"Failed to check existing prices: {e}"
        logger.error(msg)
        errors.append(msg)

    rows = [_price_row(p) for p in prices]

    for row in rows:
        if (row["symbol"], row["price_date"]) in existing_keys:
            updated += 1
        else:
            inserted += 1

    # Upsert in batches
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            result = (
                client.table("daily_prices")
                .upsert(batch, on_conflict="symbol,price_date")
                .execute()
            )
            if not result.data:
                msg = (
                    f"Prices upsert returned no data (batch {i // batch_size + 1}, "
                    f"{len(batch)} rows). Check RLS policies on 'daily_prices' table."
                )
                logger.error(msg)
                errors.append(msg)
        except Exception as e:
            msg = f"Prices upsert failed (batch {i // batch_size + 1}): {e}"
            logger.exception(msg)
            errors.append(msg)

    logger.info(
        "Daily prices upsert: %d inserted, %d updated, %d errors",
        inserted, updated, len(errors),
    )
    return inserted, updated, errors


def get_price_date_range(symbol: str) -> tuple[date | None, date | None]:
    """Return (min_date, max_date) of stored prices for a symbol, or (None, None)."""
    try:
        result = (
            get_client()
            .table("daily_prices")
            .select("price_date")
            .eq("symbol", symbol.upper())
            .order("price_date", desc=False)
            .limit(1)
            .execute()
        )
        if not result.data:
            return None, None
        min_date = date.fromisoformat(result.data[0]["price_date"])

        result2 = (
            get_client()
            .table("daily_prices")
            .select("price_date")
            .eq("symbol", symbol.upper())
            .order("price_date", desc=True)
            .limit(1)
            .execute()
        )
        max_date = date.fromisoformat(result2.data[0]["price_date"])
        return min_date, max_date
    except Exception as e:
        logger.exception("Failed to get price date range for %s", symbol)
        return None, None


@st.cache_data(ttl=60)
def get_daily_prices(
    symbol: str,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict]:
    """Fetch daily prices for a symbol, optionally filtered by date range."""
    try:
        query = (
            get_client()
            .table("daily_prices")
            .select("*")
            .eq("symbol", symbol.upper())
        )
        if date_from:
            query = query.gte("price_date", date_from.isoformat())
        if date_to:
            query = query.lte("price_date", date_to.isoformat())
        result = query.order("price_date", desc=False).execute()
        return result.data
    except Exception as e:
        logger.exception("Failed to fetch daily prices for %s", symbol)
        st.error(f"Database error fetching daily prices: {e}")
        return []


@st.cache_data(ttl=60)
def get_latest_price(symbol: str) -> dict | None:
    """Fetch the most recent daily price row for a symbol."""
    try:
        result = (
            get_client()
            .table("daily_prices")
            .select("*")
            .eq("symbol", symbol.upper())
            .order("price_date", desc=True)
            .limit(1)
            .execute()
        )
        return result.data[0] if result.data else None
    except Exception as e:
        logger.exception("Failed to fetch latest price for %s", symbol)
        st.error(f"Database error fetching latest price: {e}")
        return None


def get_metrics_for_symbols(symbols: list[str]) -> dict[str, dict[str, dict]]:
    """Fetch latest metrics for multiple symbols.

    Returns nested dict: {symbol: {metric_name: row_dict}}.
    """
    result: dict[str, dict[str, dict]] = {}
    for sym in symbols:
        latest = get_latest_stock_metrics(sym)
        if latest:
            result[sym] = latest
    return result


# ── Valuation snapshots ─────────────────────────────────────────────────────


# Fields to persist from the ValuationSnapshot model (excluding symbol/date/preset/price
# which are handled separately as the upsert key).
_SNAPSHOT_FIELDS = [
    "pe_ttm", "pb", "ps", "ev_ebitda", "ev_revenue", "peg",
    "earnings_yield", "fcf_yield", "market_cap", "enterprise_value",
    "gross_margin", "operating_margin", "net_margin", "roe", "roa",
    "debt_to_equity", "current_ratio", "interest_coverage", "cash_to_assets",
    "dividend_yield", "payout_ratio",
    "revenue_growth", "eps_growth", "net_income_growth",
    "pe_percentile", "pb_percentile", "ps_percentile", "ev_ebitda_percentile",
    "score_composite", "score_valuation", "score_profitability",
    "score_health", "score_growth",
]


def _snapshot_row(snap: ValuationSnapshot) -> dict:
    """Serialise a ValuationSnapshot for Supabase upsert."""
    row = {
        "symbol": snap.symbol,
        "snapshot_date": _ser(snap.snapshot_date),
        "preset": snap.preset,
        "price_used": _ser(snap.price_used),
    }
    for field in _SNAPSHOT_FIELDS:
        row[field] = _ser(getattr(snap, field))
    return row


def upsert_valuation_snapshots(
    snapshots: list[ValuationSnapshot],
) -> tuple[int, int, list[str]]:
    """Save valuation snapshots, upserting on (symbol, snapshot_date, preset).

    Returns (inserted, updated, errors).
    """
    if not snapshots:
        return 0, 0, []

    client = get_client()
    errors: list[str] = []
    inserted = 0
    updated = 0

    # Check existing to distinguish insert vs update
    symbols = list({s.symbol for s in snapshots})
    existing_keys: set[tuple] = set()
    try:
        for sym in symbols:
            result = (
                client.table("valuation_snapshots")
                .select("symbol,snapshot_date,preset")
                .eq("symbol", sym)
                .execute()
            )
            for row in result.data:
                existing_keys.add((row["symbol"], row["snapshot_date"], row["preset"]))
    except Exception as e:
        msg = f"Failed to check existing valuation snapshots: {e}"
        logger.error(msg)
        errors.append(msg)

    rows = [_snapshot_row(s) for s in snapshots]

    for row in rows:
        if (row["symbol"], row["snapshot_date"], row["preset"]) in existing_keys:
            updated += 1
        else:
            inserted += 1

    # Upsert in batches
    batch_size = 50
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            result = (
                client.table("valuation_snapshots")
                .upsert(batch, on_conflict="symbol,snapshot_date,preset")
                .execute()
            )
            if not result.data:
                msg = (
                    f"Valuation snapshot upsert returned no data "
                    f"(batch {i // batch_size + 1}, {len(batch)} rows). "
                    f"Check RLS policies on 'valuation_snapshots' table."
                )
                logger.error(msg)
                errors.append(msg)
        except Exception as e:
            msg = f"Valuation snapshot upsert failed (batch {i // batch_size + 1}): {e}"
            logger.exception(msg)
            errors.append(msg)

    logger.info(
        "Valuation snapshots upsert: %d inserted, %d updated, %d errors",
        inserted, updated, len(errors),
    )
    return inserted, updated, errors


@st.cache_data(ttl=60)
def get_valuation_snapshots(
    symbol: str,
    preset: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict]:
    """Fetch valuation snapshots for a symbol, optionally filtered."""
    try:
        query = (
            get_client()
            .table("valuation_snapshots")
            .select("*")
            .eq("symbol", symbol.upper())
        )
        if preset:
            query = query.eq("preset", preset)
        if date_from:
            query = query.gte("snapshot_date", date_from.isoformat())
        if date_to:
            query = query.lte("snapshot_date", date_to.isoformat())
        result = query.order("snapshot_date", desc=False).execute()
        return result.data
    except Exception as e:
        logger.exception("Failed to fetch valuation snapshots for %s", symbol)
        st.error(f"Database error fetching valuation snapshots: {e}")
        return []


@st.cache_data(ttl=60)
def get_latest_valuation_snapshots(
    symbols: list[str],
    preset: str = "Balanced",
) -> dict[str, dict]:
    """Fetch the most recent snapshot for each symbol + preset.

    Returns {symbol: snapshot_row}.
    """
    result: dict[str, dict] = {}
    for sym in symbols:
        try:
            resp = (
                get_client()
                .table("valuation_snapshots")
                .select("*")
                .eq("symbol", sym.upper())
                .eq("preset", preset)
                .order("snapshot_date", desc=True)
                .limit(1)
                .execute()
            )
            if resp.data:
                result[sym] = resp.data[0]
        except Exception as e:
            logger.exception("Failed to fetch latest valuation for %s", sym)
    return result
