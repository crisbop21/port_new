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


# ── Upsert ───────────────────────────────────────────────────────────────────

def upsert_statement(parsed: ParsedStatement) -> str:
    """Idempotently save a parsed statement to Supabase.

    Upserts the statement row on account_id conflict (one statement per
    account), then replaces all child positions and trades.  Uploading a
    new PDF for the same account always fully replaces the old data —
    no duplicates are possible.

    Returns:
        The statement UUID.

    Raises:
        st.error and re-raises on any Supabase/network failure.
    """
    client = get_client()
    meta = parsed.meta

    try:
        # Upsert statement row (account_id is the unique key)
        stmt_data = {
            "account_id": meta.account_id,
            "period_start": _ser(meta.period_start),
            "period_end": _ser(meta.period_end),
            "base_currency": meta.base_currency,
        }
        result = (
            client.table("statements")
            .upsert(stmt_data, on_conflict="account_id")
            .execute()
        )
        if not result.data:
            raise RuntimeError(
                "Statement upsert returned no data. "
                "Check that RLS policies allow INSERT/UPDATE on the 'statements' table "
                "for your API key."
            )
        statement_id = result.data[0]["id"]

        # Delete old child rows (cascade would handle this on statement
        # delete, but we're upserting so we need to clean up explicitly)
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

        # Insert trades
        if parsed.trades:
            trade_rows = [_trade_row(t, statement_id) for t in parsed.trades]
            trade_result = client.table("trades").insert(trade_rows).execute()
            if not trade_result.data:
                raise RuntimeError(
                    f"Trades insert returned no data ({len(trade_rows)} rows sent). "
                    "Check RLS policies on the 'trades' table."
                )

        logger.info(
            "Upserted statement %s: %d positions, %d trades",
            statement_id, len(parsed.positions), len(parsed.trades),
        )
        return statement_id

    except Exception as e:
        logger.exception("Failed to upsert statement for %s", meta.account_id)
        st.error(f"Database error saving statement: {e}")
        raise


def clear_query_caches() -> None:
    """Clear all cached query results so fresh data is fetched."""
    get_statements.clear()
    get_positions.clear()
    get_trades.clear()


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
