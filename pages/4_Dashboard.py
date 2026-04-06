"""Dashboard — portfolio overview with charts and key metrics."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.db import (
    get_account_ids,
    get_daily_prices,
    get_latest_price,
    get_metrics_for_symbols,
    get_portfolio_symbols,
    get_positions,
    get_statements,
    get_trades,
    get_latest_valuation_snapshots,
)

st.title("Dashboard")

# ── Account selector ─────────────────────────────────────────────────────────

account_ids = get_account_ids()
if not account_ids:
    st.info("No statements uploaded yet.")
    st.page_link("pages/1_Upload.py", label="Go to Upload", icon="📤")
    st.stop()

with st.sidebar:
    account_options = ["All Accounts"] + account_ids
    selected_account = st.selectbox("Account", account_options)

# ── Setup Progress Tracker ─────────────────────────────────────────────────

account_filter = None if selected_account == "All Accounts" else selected_account


def _check_data_readiness(acct_filter):
    """Check which data sources are populated and return a status dict."""
    status = {}

    # 1. Statements & positions
    stmts = get_statements()
    acct_stmts = [s for s in stmts if not acct_filter or s["account_id"] == acct_filter]
    status["statements"] = len(acct_stmts)

    pos_count = 0
    for s in acct_stmts[:1]:  # check latest only
        pos_count = len(get_positions(s["id"])) if acct_stmts else 0
    status["positions"] = pos_count

    # 2. Trades
    trades = get_trades(account_id=acct_filter)
    status["trades"] = len(trades)

    # 3. Symbols
    symbols = get_portfolio_symbols(account_id=acct_filter)
    status["symbols"] = symbols

    # 4. Daily prices
    prices_ok = 0
    newest_price_date = None
    for sym in symbols[:5]:  # sample check
        p = get_latest_price(sym)
        if p:
            prices_ok += 1
            pd_str = p.get("price_date")
            if pd_str and (newest_price_date is None or str(pd_str) > str(newest_price_date)):
                newest_price_date = pd_str
    status["has_prices"] = prices_ok > 0 if symbols else False
    status["latest_price_date"] = newest_price_date

    # 5. Fundamentals
    if symbols:
        metrics = get_metrics_for_symbols(symbols)
        status["has_metrics"] = len(metrics) > 0
    else:
        status["has_metrics"] = False

    # 6. Valuation snapshots
    if symbols:
        snaps = get_latest_valuation_snapshots(symbols[:3])
        status["has_valuation"] = len(snaps) > 0
    else:
        status["has_valuation"] = False

    return status


from src.ui_helpers import freshness_badge

with st.expander("Data Setup Progress", expanded=False):
    readiness = _check_data_readiness(account_filter)

    def _status_icon(ok):
        return "✅" if ok else "⬜"

    # Build freshness suffix for prices
    price_fresh = ""
    if readiness["has_prices"] and readiness.get("latest_price_date"):
        price_fresh = f" · {freshness_badge(readiness['latest_price_date'])}"

    steps = [
        (_status_icon(readiness["statements"] > 0),
         f"Statements uploaded ({readiness['statements']})",
         None),
        (_status_icon(readiness["positions"] > 0),
         f"Holdings loaded ({readiness['positions']} positions)",
         None),
        (_status_icon(readiness["trades"] > 0),
         f"Trades imported ({readiness['trades']} trades)",
         None),
        (_status_icon(readiness["has_prices"]),
         f"Daily prices fetched{price_fresh}",
         "pages/6_Prices.py" if not readiness["has_prices"] else None),
        (_status_icon(readiness["has_metrics"]),
         "SEC fundamentals loaded",
         "pages/5_Metrics.py" if not readiness["has_metrics"] else None),
        (_status_icon(readiness["has_valuation"]),
         "Valuation scores computed",
         "pages/8_Valuation.py" if not readiness["has_valuation"] else None),
    ]

    for icon, label, link in steps:
        if link:
            col_icon, col_label, col_link = st.columns([0.5, 6, 2])
            col_icon.markdown(icon)
            col_label.markdown(label)
            col_link.page_link(link, label="Set up →")
        else:
            col_icon, col_label = st.columns([0.5, 8])
            col_icon.markdown(icon)
            col_label.markdown(label)

    done = sum(1 for i, l, _ in steps if i == "✅")
    st.progress(done / len(steps), text=f"{done}/{len(steps)} complete")

    # Fetch All Data button — populates prices + metrics in one click
    if readiness["symbols"] and (not readiness["has_prices"] or not readiness["has_metrics"]):
        if st.button("Fetch All Data", type="primary",
                     help="Fetch daily prices and SEC fundamentals for all portfolio symbols."):
            from src.price_fetcher import fetch_prices_for_symbols
            from src.fetcher import fetch_metrics_for_symbol
            from src.db import upsert_daily_prices, upsert_stock_metrics, clear_query_caches

            syms = readiness["symbols"]
            total_steps = len(syms) * 2  # prices + metrics
            prog = st.progress(0, text="Fetching data...")
            step = 0

            # Fetch prices
            if not readiness["has_prices"]:
                for i, sym in enumerate(syms):
                    step += 1
                    prog.progress(step / total_steps, text=f"Prices: {sym} ({i+1}/{len(syms)})")
                    from src.price_fetcher import fetch_daily_prices as _fdp
                    prices, _ = _fdp(sym, start=date.today() - timedelta(days=365))
                    if prices:
                        upsert_daily_prices(prices)
            else:
                step += len(syms)

            # Fetch metrics
            if not readiness["has_metrics"]:
                for i, sym in enumerate(syms):
                    step += 1
                    prog.progress(step / total_steps, text=f"Metrics: {sym} ({i+1}/{len(syms)})")
                    metrics, _ = fetch_metrics_for_symbol(sym)
                    if metrics:
                        upsert_stock_metrics(metrics)
            else:
                step += len(syms)

            prog.empty()
            clear_query_caches()
            st.success("Data fetch complete! Refresh the page to see updated progress.")
            st.rerun()

# ── Load data ────────────────────────────────────────────────────────────────

with st.spinner("Loading portfolio data..."):
    statements = get_statements()

    if not statements:
        st.info("No statements uploaded yet.")
        st.page_link("pages/1_Upload.py", label="Go to Upload", icon="📤")
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
