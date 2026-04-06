"""Dashboard — portfolio overview with charts and key metrics."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.db import get_account_ids, get_positions, get_statements, get_trades

st.title("Dashboard")

# ── Account selector ─────────────────────────────────────────────────────────

account_ids = get_account_ids()
if not account_ids:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

account_options = ["All Accounts"] + account_ids
selected_account = st.selectbox("Account", account_options)

# ── Load data ────────────────────────────────────────────────────────────────

with st.spinner("Loading portfolio data..."):
    statements = get_statements()

    if not statements:
        st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
        st.stop()

    if selected_account == "All Accounts":
        # Collect positions from the latest statement per account
        latest_by_account: dict[str, dict] = {}
        for s in statements:
            acct = s["account_id"]
            if acct not in latest_by_account:
                latest_by_account[acct] = s  # already sorted by period_end desc
        positions = []
        for s in latest_by_account.values():
            positions.extend(get_positions(s["id"]))
        latest = statements[0]
    else:
        acct_statements = [s for s in statements if s["account_id"] == selected_account]
        if not acct_statements:
            st.info("No statements found for this account.")
            st.stop()
        latest = acct_statements[0]
        positions = get_positions(latest["id"])

    account_filter = None if selected_account == "All Accounts" else selected_account
    trades = get_trades(
        account_id=account_filter,
        date_from=date.today() - timedelta(days=365),
        date_to=date.today(),
    )

# ── DataFrames ───────────────────────────────────────────────────────────────

pos_df = pd.DataFrame(positions) if positions else pd.DataFrame()
trade_df = pd.DataFrame(trades) if trades else pd.DataFrame()

if not pos_df.empty:
    for col in ["market_value", "unrealized_pnl", "cost_basis"]:
        if col in pos_df.columns:
            pos_df[col] = pd.to_numeric(pos_df[col], errors="coerce")

if not trade_df.empty:
    for col in ["realized_pnl", "commission", "proceeds"]:
        if col in trade_df.columns:
            trade_df[col] = pd.to_numeric(trade_df[col], errors="coerce")
    if "trade_date" in trade_df.columns:
        trade_df["trade_date"] = pd.to_datetime(trade_df["trade_date"], errors="coerce")

# ── Top-level metrics ────────────────────────────────────────────────────────

total_mv = pos_df["market_value"].sum() if not pos_df.empty else 0
total_unrealized = pos_df["unrealized_pnl"].sum() if not pos_df.empty else 0
total_realized = trade_df["realized_pnl"].sum() if not trade_df.empty else 0
total_commission = trade_df["commission"].sum() if not trade_df.empty else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Portfolio Value", f"${total_mv:,.2f}")
c2.metric("Unrealized P&L", f"${total_unrealized:,.2f}",
          delta=f"{total_unrealized:,.2f}")
c3.metric("Realized P&L (1Y)", f"${total_realized:,.2f}",
          delta=f"{total_realized:,.2f}")
c4.metric("Commissions (1Y)", f"${total_commission:,.2f}")

st.divider()

# ── Charts ───────────────────────────────────────────────────────────────────

left, right = st.columns(2)

# -- Holdings by asset class (pie-style bar chart) --
if not pos_df.empty:
    with left:
        st.subheader("Holdings by Asset Class")
        alloc = (
            pos_df.groupby("asset_class")["market_value"]
            .sum()
            .reset_index()
            .rename(columns={"asset_class": "Asset Class", "market_value": "Market Value"})
        )
        st.bar_chart(alloc, x="Asset Class", y="Market Value")

# -- Top positions by market value --
if not pos_df.empty:
    with right:
        st.subheader("Top 10 Positions")
        top = (
            pos_df.nlargest(10, "market_value")[["symbol", "market_value"]]
            .reset_index(drop=True)
        )
        st.bar_chart(top, x="symbol", y="market_value")

# -- Daily realized P&L over time --
if not trade_df.empty:
    st.subheader("Daily Realized P&L")
    daily_pnl = (
        trade_df
        .dropna(subset=["trade_date"])
        .groupby(trade_df["trade_date"].dt.date)["realized_pnl"]
        .sum()
        .reset_index()
        .rename(columns={"trade_date": "Date", "realized_pnl": "Realized P&L"})
    )
    st.line_chart(daily_pnl, x="Date", y="Realized P&L")

# -- Cumulative P&L --
if not trade_df.empty:
    st.subheader("Cumulative Realized P&L")
    cum_pnl = daily_pnl.copy()
    cum_pnl["Cumulative P&L"] = cum_pnl["Realized P&L"].cumsum()
    st.area_chart(cum_pnl, x="Date", y="Cumulative P&L")

# ── Recent trades ────────────────────────────────────────────────────────────

if not trade_df.empty:
    st.subheader("Recent Trades")
    recent = trade_df.head(10)
    show_cols = [c for c in ["trade_date", "symbol", "side", "quantity", "price", "realized_pnl"]
                 if c in recent.columns]
    recent_disp = recent[show_cols].reset_index(drop=True).copy()
    st.dataframe(
        recent_disp,
        use_container_width=True,
        column_config={
            "trade_date": st.column_config.DatetimeColumn("Date", format="YYYY-MM-DD HH:mm"),
            "price": st.column_config.NumberColumn("Price", format="$%.2f"),
            "realized_pnl": st.column_config.NumberColumn("Realized P&L", format="$%.2f"),
        },
    )

# ── Statement info ───────────────────────────────────────────────────────────

with st.expander("Statement info"):
    if selected_account == "All Accounts":
        st.write(f"**Accounts:** {', '.join(account_ids)}")
        st.write(f"**Statements uploaded:** {len(statements)}")
    else:
        st.write(f"**Account:** {latest['account_id']}")
        st.write(f"**Period:** {latest['period_start']} → {latest['period_end']}")
        st.write(f"**Statements uploaded:** {len([s for s in statements if s['account_id'] == selected_account])}")
