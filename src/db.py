"""Supabase database layer for the IBKR Trade Journal.

Tables: statements, positions, trades (see sql/001_schema.sql).
All financial columns use Postgres numeric via Python Decimal.
"""

import logging
import os
from datetime import date, datetime
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
        (statement_id, trades_skipped) — the UUID and how many duplicate
        trades were filtered out.

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

        # Insert positions
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
            "Upserted statement %s: %d positions, %d trades (%d dupes skipped)",
            statement_id, len(parsed.positions),
            len(parsed.trades) - trades_skipped, trades_skipped,
        )
        return statement_id, trades_skipped

    except Exception as e:
        logger.exception("Failed to upsert statement for %s", meta.account_id)
        st.error(f"Database error saving statement: {e}")
        raise


def clear_query_caches() -> None:
    """Clear all cached query results so fresh data is fetched."""
    get_statements.clear()
    get_positions.clear()
    get_trades.clear()
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
def get_trades(
    statement_id: str | None = None,
    symbol: str | None = None,
    asset_class: str | None = None,
    side: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict]:
    """Fetch trades with optional filters.

    Args:
        statement_id: Filter by statement.
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
def reconstruct_holdings(statement_id: str, as_of_date: date) -> list[dict]:
    """Reconstruct holdings as of *as_of_date*.

    Starts from the Open Positions snapshot attached to *statement_id*
    (which reflects holdings at that statement's period_end) and reverses
    trades between *as_of_date* and period_end.

    Options with expiry < as_of_date are filtered out.
    A ``multiplier`` field (100 for OPT, 1 otherwise) and ``cost_value``
    (quantity * cost_basis * multiplier) are added to each row.
    """
    client = get_client()

    # Statement metadata
    stmt = (
        client.table("statements")
        .select("period_end,account_id")
        .eq("id", statement_id)
        .single()
        .execute()
    )
    if not stmt.data:
        return []

    period_end = date.fromisoformat(stmt.data["period_end"])
    account_id = stmt.data["account_id"]

    # Seed from snapshot
    positions = get_positions(statement_id)
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

    is_snapshot = as_of_date >= period_end

    if not is_snapshot:
        # Reverse every trade that happened AFTER as_of_date up to period_end
        trades = _get_account_trades_between(account_id, as_of_date, period_end)

        for t in trades:
            key = _position_key(t)
            qty = Decimal(str(t["quantity"]))

            if key not in pos_map:
                # Position was fully opened and closed between as_of_date and
                # period_end — reversing the trade resurfaces it.
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

        # Snapshot-only fields are no longer valid for a historical date
        for pos in pos_map.values():
            pos["market_price"] = None
            pos["market_value"] = None
            pos["unrealized_pnl"] = None

    # Filter and build result
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
            "market_price": pos.get("market_price"),
            "market_value": pos.get("market_value"),
            "unrealized_pnl": pos.get("unrealized_pnl"),
        })

    return sorted(result, key=lambda r: r["symbol"])
