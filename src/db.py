"""Supabase database layer for the IBKR Trade Journal.

Tables: statements, positions, trades (see sql/001_schema.sql).
All financial columns use Postgres numeric via Python Decimal.
"""

import logging
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

import streamlit as st
from supabase import Client, create_client

from src.models import ParsedStatement, Position, Trade

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

    # Position duplicates
    existing_pos_fps = _get_existing_position_fingerprints(
        meta.account_id, exclude_statement_id=existing_statement_id,
    )
    new_positions = 0
    dup_positions = 0
    for p in parsed.positions:
        row = _position_row(p, "dummy")
        fp = _position_fingerprint(row)
        if fp in existing_pos_fps:
            dup_positions += 1
        else:
            new_positions += 1

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

        # Insert positions — deduplicate against other statements
        positions_skipped = 0
        if parsed.positions:
            existing_pos_fps = _get_existing_position_fingerprints(
                meta.account_id, exclude_statement_id=statement_id,
            )
            pos_rows = []
            for p in parsed.positions:
                row = _position_row(p, statement_id)
                fp = _position_fingerprint(row)
                if fp in existing_pos_fps:
                    positions_skipped += 1
                    continue
                pos_rows.append(row)

            if pos_rows:
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
    reconstruct_holdings.clear()


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


# ── Holdings reconstruction ──────────────────────────────────────────────────

def _get_account_trades_between(account_id: str, after_date: date, up_to_date: date) -> list[dict]:
    """Fetch deduplicated trades for an account strictly after *after_date* up to *up_to_date*."""
    stmt_ids = _get_account_statement_ids(account_id)
    if not stmt_ids:
        return []

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

    seen: set[tuple] = set()
    unique: list[dict] = []
    for t in result.data:
        fp = _trade_fingerprint(t)
        if fp not in seen:
            seen.add(fp)
            unique.append(t)
    return unique


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


def _find_best_base(account_id: str, as_of_date: date) -> dict | None:
    """Find the statement whose period_end is closest to *as_of_date* without exceeding it.

    Returns the statement dict (id, period_start, period_end) or None.
    """
    stmts = _get_account_statements(account_id)
    best = None
    for s in stmts:
        p_end = date.fromisoformat(s["period_end"])
        if p_end <= as_of_date:
            best = s  # list is sorted asc, so last match wins
    return best


def check_coverage_gap(account_id: str, as_of_date: date) -> str | None:
    """Check whether we can accurately reconstruct holdings as of *as_of_date*.

    Returns a human-readable gap description if there is a problem, or None
    if coverage is sufficient.
    """
    stmts = _get_account_statements(account_id)
    if not stmts:
        return "No statements uploaded for this account."

    # Find best base snapshot
    base = _find_best_base(account_id, as_of_date)
    if base is None:
        earliest_end = date.fromisoformat(stmts[0]["period_end"])
        # as_of_date is before all snapshots — check backward coverage
        earliest_start = date.fromisoformat(stmts[0]["period_start"])
        if as_of_date < earliest_start:
            return (
                f"Date {as_of_date} is before the earliest statement period "
                f"({earliest_start}). No trade data available before that."
            )
        # as_of_date is within the first statement period but before its end —
        # we can reverse from the earliest snapshot
        return None

    base_end = date.fromisoformat(base["period_end"])
    if base_end == as_of_date:
        return None  # Exact snapshot match

    # Forward reconstruction: need trade coverage from base_end to as_of_date
    # Check that statement periods continuously cover [base_end, as_of_date]
    periods = [
        (date.fromisoformat(s["period_start"]), date.fromisoformat(s["period_end"]))
        for s in stmts
    ]
    # Filter to periods that overlap with [base_end, as_of_date]
    relevant = [
        (s, e) for s, e in periods if e >= base_end and s <= as_of_date
    ]
    if not relevant:
        return (
            f"No statement covers the period between {base_end} and {as_of_date}."
        )

    # Merge overlapping/adjacent intervals
    merged: list[tuple[date, date]] = []
    for s, e in sorted(relevant):
        if merged and s <= merged[-1][1] + timedelta(days=1):
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))

    if len(merged) > 1:
        # Find the actual gap(s)
        gaps = []
        for i in range(len(merged) - 1):
            gap_start = merged[i][1] + timedelta(days=1)
            gap_end = merged[i + 1][0] - timedelta(days=1)
            gaps.append(f"{gap_start} to {gap_end}")
        return (
            f"Missing statement coverage for: {'; '.join(gaps)}. "
            "Trades in these periods are unknown."
        )

    if merged[0][0] > base_end + timedelta(days=1):
        return (
            f"No statement covers {base_end + timedelta(days=1)} to "
            f"{merged[0][0] - timedelta(days=1)}."
        )

    return None


@st.cache_data(ttl=60)
def reconstruct_holdings(account_id: str, as_of_date: date) -> list[dict]:
    """Reconstruct holdings as of *as_of_date* using a forward-roll approach.

    Algorithm:
    1. Find the best base snapshot — the statement whose period_end is
       closest to as_of_date without exceeding it.
    2. If as_of_date == base snapshot date → return the snapshot as-is.
    3. If as_of_date > base snapshot → roll FORWARD by applying trades
       from base_date to as_of_date (BOT adds qty, SLD subtracts qty).
    4. If as_of_date < all snapshots → fall back to reversing trades from
       as_of_date back to the earliest snapshot.

    Options with expiry < as_of_date are filtered out.
    A ``multiplier`` field (100 for OPT, 1 otherwise) and ``cost_value``
    (quantity × cost_basis × multiplier) are added to each row.
    """
    client = get_client()

    # Find all statements for this account
    stmts = _get_account_statements(account_id)
    if not stmts:
        return []

    base = _find_best_base(account_id, as_of_date)

    if base is not None:
        # ── Forward roll (or exact snapshot) ──────────────────────────────
        base_id = base["id"]
        base_end = date.fromisoformat(base["period_end"])

        positions = get_positions(base_id)
        pos_map: dict[tuple, dict] = {}
        for p in positions:
            key = _position_key(p)
            pos_map[key] = {
                "symbol": p["symbol"],
                "asset_class": p["asset_class"],
                "quantity": Decimal(str(p["quantity"])),
                "cost_basis": Decimal(str(p["cost_basis"])),
                "currency": p.get("currency", "USD"),
                "expiry": p.get("expiry"),
                "strike": p.get("strike"),
                "right": p.get("right"),
                "market_price": p.get("market_price"),
                "market_value": p.get("market_value"),
                "unrealized_pnl": p.get("unrealized_pnl"),
            }

        is_snapshot = as_of_date == base_end

        if not is_snapshot:
            # Apply trades FORWARD from base_end to as_of_date
            trades = _get_account_trades_between(account_id, base_end, as_of_date)

            for t in trades:
                key = _position_key(t)
                qty = Decimal(str(t["quantity"]))

                if key not in pos_map:
                    # New position opened after the base snapshot
                    pos_map[key] = {
                        "symbol": t["symbol"],
                        "asset_class": t["asset_class"],
                        "quantity": Decimal("0"),
                        "cost_basis": Decimal(str(t["price"])),
                        "currency": t.get("currency", "USD"),
                        "expiry": t.get("expiry"),
                        "strike": t.get("strike"),
                        "right": t.get("right"),
                        "market_price": None,
                        "market_value": None,
                        "unrealized_pnl": None,
                    }

                if t["side"] == "BOT":
                    pos_map[key]["quantity"] += qty   # bought → add
                else:  # SLD
                    pos_map[key]["quantity"] -= qty   # sold → subtract

            # Market data from the base snapshot is stale after rolling forward
            for pos in pos_map.values():
                pos["market_price"] = None
                pos["market_value"] = None
                pos["unrealized_pnl"] = None

    else:
        # ── Fallback: reverse from earliest snapshot ──────────────────────
        earliest = stmts[0]
        earliest_id = earliest["id"]
        earliest_end = date.fromisoformat(earliest["period_end"])

        positions = get_positions(earliest_id)
        pos_map = {}
        for p in positions:
            key = _position_key(p)
            pos_map[key] = {
                "symbol": p["symbol"],
                "asset_class": p["asset_class"],
                "quantity": Decimal(str(p["quantity"])),
                "cost_basis": Decimal(str(p["cost_basis"])),
                "currency": p.get("currency", "USD"),
                "expiry": p.get("expiry"),
                "strike": p.get("strike"),
                "right": p.get("right"),
                "market_price": None,
                "market_value": None,
                "unrealized_pnl": None,
            }

        # Reverse trades between as_of_date and earliest_end
        trades = _get_account_trades_between(account_id, as_of_date, earliest_end)
        for t in trades:
            key = _position_key(t)
            qty = Decimal(str(t["quantity"]))

            if key not in pos_map:
                pos_map[key] = {
                    "symbol": t["symbol"],
                    "asset_class": t["asset_class"],
                    "quantity": Decimal("0"),
                    "cost_basis": Decimal(str(t["price"])),
                    "currency": t.get("currency", "USD"),
                    "expiry": t.get("expiry"),
                    "strike": t.get("strike"),
                    "right": t.get("right"),
                    "market_price": None,
                    "market_value": None,
                    "unrealized_pnl": None,
                }

            if t["side"] == "BOT":
                pos_map[key]["quantity"] -= qty   # hadn't bought yet
            else:  # SLD
                pos_map[key]["quantity"] += qty   # hadn't sold yet

    # ── Filter and build result ───────────────────────────────────────────
    result: list[dict] = []
    for pos in pos_map.values():
        if pos["quantity"] <= 0:
            continue

        # Drop expired options
        if pos["asset_class"] == "OPT" and pos.get("expiry"):
            expiry_val = pos["expiry"]
            expiry_date = (
                date.fromisoformat(expiry_val) if isinstance(expiry_val, str) else expiry_val
            )
            if expiry_date < as_of_date:
                continue

        multiplier = OPTION_MULTIPLIER if pos["asset_class"] == "OPT" else 1
        cost_value = pos["quantity"] * pos["cost_basis"] * multiplier

        is_snapshot = base is not None and as_of_date == date.fromisoformat(base["period_end"])

        result.append({
            "symbol": pos["symbol"],
            "asset_class": pos["asset_class"],
            "quantity": str(pos["quantity"]),
            "cost_basis": str(pos["cost_basis"]),
            "currency": pos["currency"],
            "expiry": pos.get("expiry"),
            "strike": pos.get("strike"),
            "right": pos.get("right"),
            "multiplier": multiplier,
            "cost_value": str(cost_value),
            "market_price": pos.get("market_price") if is_snapshot else None,
            "market_value": pos.get("market_value") if is_snapshot else None,
            "unrealized_pnl": pos.get("unrealized_pnl") if is_snapshot else None,
        })

    return sorted(result, key=lambda r: r["symbol"])


def reconcile_holdings(account_id: str) -> dict:
    """Verify data integrity by rolling forward from the earliest snapshot to
    the latest and comparing against the actual latest snapshot.

    Returns a dict with:
        ok: bool — True if reconstructed matches the actual snapshot
        base_date: str — period_end of the earliest snapshot used
        target_date: str — period_end of the latest snapshot compared against
        mismatches: list[dict] — per-symbol details where qty differs
            Each dict has: symbol, expected_qty (from snapshot), reconstructed_qty, diff
        missing_from_reconstruction: list[dict] — in snapshot but not reconstructed
        extra_in_reconstruction: list[dict] — reconstructed but not in snapshot
        coverage_gap: str | None — gap warning if applicable
    """
    stmts = _get_account_statements(account_id)
    if len(stmts) < 2:
        return {
            "ok": True,
            "base_date": stmts[0]["period_end"] if stmts else None,
            "target_date": stmts[0]["period_end"] if stmts else None,
            "mismatches": [],
            "missing_from_reconstruction": [],
            "extra_in_reconstruction": [],
            "coverage_gap": None,
            "skipped": "Need at least 2 statements to reconcile.",
        }

    earliest = stmts[0]
    latest = stmts[-1]
    earliest_end = date.fromisoformat(earliest["period_end"])
    latest_end = date.fromisoformat(latest["period_end"])

    # Check coverage between earliest and latest
    coverage_gap = check_coverage_gap(account_id, latest_end)

    # Build reconstructed positions: earliest snapshot + all trades forward
    base_positions = get_positions(earliest["id"])
    pos_map: dict[tuple, dict] = {}
    for p in base_positions:
        key = _position_key(p)
        pos_map[key] = {
            "symbol": p["symbol"],
            "asset_class": p["asset_class"],
            "quantity": Decimal(str(p["quantity"])),
        }

    trades = _get_account_trades_between(account_id, earliest_end, latest_end)
    for t in trades:
        key = _position_key(t)
        qty = Decimal(str(t["quantity"]))
        if key not in pos_map:
            pos_map[key] = {
                "symbol": t["symbol"],
                "asset_class": t["asset_class"],
                "quantity": Decimal("0"),
            }
        if t["side"] == "BOT":
            pos_map[key]["quantity"] += qty
        else:
            pos_map[key]["quantity"] -= qty

    # Filter out zero/negative and expired options
    reconstructed: dict[tuple, Decimal] = {}
    for key, pos in pos_map.items():
        if pos["quantity"] <= 0:
            continue
        # Skip expired options
        if pos["asset_class"] == "OPT" and len(key) == 4 and key[1]:
            expiry_val = key[1]
            expiry_date = (
                date.fromisoformat(expiry_val) if isinstance(expiry_val, str) else expiry_val
            )
            if expiry_date < latest_end:
                continue
        reconstructed[key] = pos["quantity"]

    # Build actual latest snapshot
    actual_positions = get_positions(latest["id"])
    actual: dict[tuple, Decimal] = {}
    for p in actual_positions:
        key = _position_key(p)
        actual[key] = Decimal(str(p["quantity"]))

    # Compare
    all_keys = set(reconstructed.keys()) | set(actual.keys())
    mismatches = []
    missing_from_reconstruction = []
    extra_in_reconstruction = []

    for key in sorted(all_keys, key=lambda k: k[0]):
        r_qty = reconstructed.get(key, Decimal("0"))
        a_qty = actual.get(key, Decimal("0"))

        symbol = key[0]

        if key not in reconstructed and key in actual:
            missing_from_reconstruction.append({
                "symbol": symbol,
                "expected_qty": str(a_qty),
            })
        elif key in reconstructed and key not in actual:
            extra_in_reconstruction.append({
                "symbol": symbol,
                "reconstructed_qty": str(r_qty),
            })
        elif r_qty != a_qty:
            mismatches.append({
                "symbol": symbol,
                "expected_qty": str(a_qty),
                "reconstructed_qty": str(r_qty),
                "diff": str(r_qty - a_qty),
            })

    ok = not mismatches and not missing_from_reconstruction and not extra_in_reconstruction

    return {
        "ok": ok,
        "base_date": earliest["period_end"],
        "target_date": latest["period_end"],
        "mismatches": mismatches,
        "missing_from_reconstruction": missing_from_reconstruction,
        "extra_in_reconstruction": extra_in_reconstruction,
        "coverage_gap": coverage_gap,
    }
