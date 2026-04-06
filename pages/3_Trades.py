"""Trade History page — filterable, sortable trade table with summary metrics."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.db import get_account_ids, get_positions_as_of, get_snapshot_dates, get_trades

st.title("Trade History")

# ── Filters ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    account_ids = get_account_ids()
    account_options = ["All Accounts"] + account_ids
    selected_account = st.selectbox("Account", account_options)
    account_filter = None if selected_account == "All Accounts" else selected_account

    # Date range presets
    preset = st.selectbox(
        "Date range",
        options=["Custom", "1W", "1M", "3M", "YTD", "1Y"],
        index=3,  # default to 3M
    )
    today = date.today()
    preset_ranges = {
        "1W": today - timedelta(days=7),
        "1M": today - timedelta(days=30),
        "3M": today - timedelta(days=90),
        "YTD": date(today.year, 1, 1),
        "1Y": today - timedelta(days=365),
    }
    if preset == "Custom":
        date_col1, date_col2 = st.columns(2)
        date_from = date_col1.date_input("From", value=today - timedelta(days=90))
        date_to = date_col2.date_input("To", value=today)
    else:
        date_from = preset_ranges[preset]
        date_to = today

    symbol = st.text_input("Symbol", placeholder="e.g. AAPL").strip().upper() or None

    asset_class = st.selectbox("Asset class", ["All", "STK", "OPT", "ETF"])
    asset_class_filter = None if asset_class == "All" else asset_class

    side = st.selectbox("Side", ["All", "BOT", "SLD"])
    side_filter = None if side == "All" else side

# ── Load trades ──────────────────────────────────────────────────────────────

with st.spinner("Loading trades..."):
    rows = get_trades(
        account_id=account_filter,
        symbol=symbol,
        asset_class=asset_class_filter,
        side=side_filter,
        date_from=date_from,
        date_to=date_to,
    )

if not rows:
    if not account_ids:
        st.info("No statements uploaded yet.")
        st.page_link("pages/1_Upload.py", label="Go to Upload", icon="📤")
    else:
        st.info("No trades match the current filters.")
    st.stop()

df = pd.DataFrame(rows)

numeric_cols = ["quantity", "price", "proceeds", "commission", "realized_pnl"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

if "trade_date" in df.columns:
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

# ── Pre-compute summary data ─────────────────────────────────────────────

total_realized = df["realized_pnl"].sum()
total_commission = df["commission"].sum()
trade_count = len(df)

closing_trades = df[df["realized_pnl"] != 0]

# Daily P&L (used in multiple tabs)
daily_pnl = (
    df.dropna(subset=["trade_date"])
    .groupby(df["trade_date"].dt.date)["realized_pnl"]
    .sum()
    .reset_index()
    .rename(columns={"trade_date": "Date", "realized_pnl": "Realized P&L"})
)

# Per-symbol stats
closing_for_symbols = closing_trades.copy()
closing_for_symbols["underlying"] = closing_for_symbols["symbol"].str.split().str[0]

symbol_stats = (
    closing_for_symbols.groupby("underlying")
    .agg(
        total_pnl=("realized_pnl", "sum"),
        trades=("realized_pnl", "count"),
        wins=("realized_pnl", lambda x: (x > 0).sum()),
        avg_pnl=("realized_pnl", "mean"),
        total_commission=("commission", "sum"),
    )
    .reset_index()
    .rename(columns={"underlying": "symbol"})
)
if not symbol_stats.empty:
    symbol_stats["win_rate"] = (symbol_stats["wins"] / symbol_stats["trades"] * 100).round(1)
    symbol_stats = symbol_stats.sort_values("total_pnl", ascending=False)

# ── Tabs ────────────────────────────────────────────────────────────────────

tab_summary, tab_charts, tab_symbols, tab_log = st.tabs(
    ["Summary", "Charts", "P&L by Symbol", "Trade Log"]
)

# ── Tab: Summary ────────────────────────────────────────────────────────────

with tab_summary:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Realized P&L", f"${total_realized:,.2f}",
                delta=f"{total_realized:,.2f}")
    col2.metric("Total Commissions", f"${total_commission:,.2f}")
    col3.metric("Trades", trade_count)

    if not closing_trades.empty:
        winners = closing_trades[closing_trades["realized_pnl"] > 0]
        losers = closing_trades[closing_trades["realized_pnl"] < 0]

        win_rate = len(winners) / len(closing_trades) * 100
        avg_win = winners["realized_pnl"].mean() if not winners.empty else 0
        avg_loss = losers["realized_pnl"].mean() if not losers.empty else 0
        gross_wins = winners["realized_pnl"].sum() if not winners.empty else 0
        gross_losses = abs(losers["realized_pnl"].sum()) if not losers.empty else 0
        profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")
        largest_win = winners["realized_pnl"].max() if not winners.empty else 0
        largest_loss = losers["realized_pnl"].min() if not losers.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Win Rate", f"{win_rate:.1f}%")
        c2.metric("Profit Factor", f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞")
        c3.metric("Avg Win", f"${avg_win:,.2f}")
        c4.metric("Avg Loss", f"${avg_loss:,.2f}")

        c5, c6, c7 = st.columns(3)
        c5.metric("Largest Win", f"${largest_win:,.2f}")
        c6.metric("Largest Loss", f"${largest_loss:,.2f}")
        comm_pct = (abs(total_commission) / gross_wins * 100) if gross_wins > 0 else 0
        c7.metric("Commissions / Gross Wins", f"{comm_pct:.1f}%")

# ── Tab: Charts ─────────────────────────────────────────────────────────────

with tab_charts:
    chart_left, chart_right = st.columns(2)

    with chart_left:
        st.subheader("Daily P&L")
        if not daily_pnl.empty:
            st.bar_chart(daily_pnl, x="Date", y="Realized P&L")

    with chart_right:
        st.subheader("Cumulative P&L")
        if not daily_pnl.empty:
            cum = daily_pnl.copy()
            cum["Cumulative P&L"] = cum["Realized P&L"].cumsum()
            st.line_chart(cum, x="Date", y="Cumulative P&L")

    if not closing_trades.empty:
        st.subheader("P&L Distribution")
        hist_data = closing_trades[["realized_pnl"]].rename(columns={"realized_pnl": "Realized P&L"})
        st.bar_chart(
            hist_data["Realized P&L"]
            .value_counts(bins=20)
            .sort_index()
            .rename("Count")
        )

# ── Tab: P&L by Symbol ─────────────────────────────────────────────────────

with tab_symbols:
    st.subheader("Realized P&L by Symbol")

    if not symbol_stats.empty:
        disp_stats = symbol_stats[["symbol", "total_pnl", "trades", "win_rate", "avg_pnl", "total_commission"]].copy()
        disp_stats = disp_stats.rename(columns={
            "symbol": "Symbol", "total_pnl": "Total P&L", "trades": "Trades",
            "win_rate": "Win Rate %", "avg_pnl": "Avg P&L", "total_commission": "Commissions",
        })
        st.dataframe(
            disp_stats,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Total P&L": st.column_config.NumberColumn(format="$%.2f"),
                "Avg P&L": st.column_config.NumberColumn(format="$%.2f"),
                "Commissions": st.column_config.NumberColumn(format="$%.2f"),
                "Win Rate %": st.column_config.NumberColumn(format="%.1f%%"),
            },
        )

        top_n = min(10, len(symbol_stats))
        st.bar_chart(
            symbol_stats.head(top_n).set_index("symbol")["total_pnl"].rename("P&L by Symbol")
        )

    st.divider()
    st.subheader("Unrealized P&L by Symbol")

    # Load ALL open positions from the latest snapshot for each account.
    _accts = [account_filter] if account_filter else get_account_ids()

    _pos_rows: list[dict] = []
    for _acct in _accts:
        _snap_dates = get_snapshot_dates(_acct)
        if _snap_dates:
            _pos_rows.extend(get_positions_as_of(_acct, _snap_dates[-1]))

    if not _pos_rows:
        st.info("No position snapshots available — upload a statement with open positions.")
    else:
        pos_df = pd.DataFrame(_pos_rows)
        for _nc in ["quantity", "market_value", "unrealized_pnl", "cost_basis"]:
            if _nc in pos_df.columns:
                pos_df[_nc] = pd.to_numeric(pos_df[_nc], errors="coerce")

        if "unrealized_pnl" not in pos_df.columns:
            pos_df["unrealized_pnl"] = 0.0
        else:
            pos_df["unrealized_pnl"] = pos_df["unrealized_pnl"].fillna(0.0)

        if "market_value" in pos_df.columns:
            pos_df["market_value"] = pos_df["market_value"].fillna(0.0)
        else:
            pos_df["market_value"] = 0.0

        if "cost_basis" not in pos_df.columns:
            pos_df["cost_basis"] = 0.0
        else:
            pos_df["cost_basis"] = pos_df["cost_basis"].fillna(0.0)

        pos_df["underlying"] = pos_df["symbol"].str.split().str[0]

        if "strike" not in pos_df.columns:
            pos_df["strike"] = float("nan")
        else:
            pos_df["strike"] = pd.to_numeric(pos_df["strike"], errors="coerce")
        if "right" not in pos_df.columns:
            pos_df["right"] = None
        if "asset_class" not in pos_df.columns:
            pos_df["asset_class"] = "STK"

        is_opt = pos_df["asset_class"] == "OPT"
        is_call = pos_df["right"] == "C"
        abs_cost = pos_df["cost_basis"].abs()
        abs_qty = pos_df["quantity"].abs()

        pos_df["pos_breakeven"] = float("nan")
        safe_abs_qty = abs_qty.replace(0, float("nan"))
        pos_df.loc[~is_opt, "pos_breakeven"] = abs_cost[~is_opt] / safe_abs_qty[~is_opt]
        opt_premium_per_share = abs_cost / (safe_abs_qty * 100)
        pos_df.loc[is_opt & is_call, "pos_breakeven"] = (
            pos_df.loc[is_opt & is_call, "strike"] + opt_premium_per_share[is_opt & is_call]
        )
        pos_df.loc[is_opt & ~is_call, "pos_breakeven"] = (
            pos_df.loc[is_opt & ~is_call, "strike"] - opt_premium_per_share[is_opt & ~is_call]
        )

        pos_df["weight"] = abs_qty.where(~is_opt, abs_qty * 100)
        pos_df["weighted_be"] = pos_df["pos_breakeven"] * pos_df["weight"]

        unrealized_stats = (
            pos_df.groupby("underlying")
            .agg(
                unrealized_pnl=("unrealized_pnl", "sum"),
                market_value=("market_value", "sum"),
                positions=("symbol", "count"),
                quantity=("quantity", "sum"),
                weighted_be=("weighted_be", "sum"),
                weight=("weight", "sum"),
            )
            .reset_index()
            .rename(columns={"underlying": "symbol"})
            .sort_values("unrealized_pnl", ascending=False)
        )

        safe_weight = unrealized_stats["weight"].replace(0, float("nan"))
        unrealized_stats["breakeven"] = (
            unrealized_stats["weighted_be"] / safe_weight
        ).abs()

        total_unrealized = unrealized_stats["unrealized_pnl"].sum()
        st.metric("Total Unrealized P&L", f"${total_unrealized:,.2f}",
                  delta=f"{total_unrealized:,.2f}")

        disp_unreal = unrealized_stats[["symbol", "quantity", "breakeven", "market_value", "unrealized_pnl", "positions"]].copy()
        disp_unreal = disp_unreal.rename(columns={
            "symbol": "Symbol", "quantity": "Total Qty", "breakeven": "Avg Breakeven",
            "market_value": "Market Value", "unrealized_pnl": "Unrealized P&L", "positions": "Positions",
        })
        st.dataframe(
            disp_unreal,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Total Qty": st.column_config.NumberColumn(format="%.2f"),
                "Avg Breakeven": st.column_config.NumberColumn(format="$%.2f"),
                "Market Value": st.column_config.NumberColumn(format="$%.2f"),
                "Unrealized P&L": st.column_config.NumberColumn(format="$%.2f"),
            },
        )

        top_u = min(10, len(unrealized_stats))
        st.bar_chart(
            unrealized_stats.head(top_u).set_index("symbol")["unrealized_pnl"]
            .rename("Unrealized P&L by Symbol")
        )

# ── Tab: Trade Log ──────────────────────────────────────────────────────────

with tab_log:
    display_cols = [
        "trade_date", "symbol", "asset_class", "side",
        "quantity", "price", "proceeds", "commission", "realized_pnl",
    ]
    if df["asset_class"].eq("OPT").any():
        display_cols += ["expiry", "strike", "right"]

    show_cols = [c for c in display_cols if c in df.columns]

    trade_log = df[show_cols].reset_index(drop=True).copy()
    st.dataframe(
        trade_log,
        use_container_width=True,
        column_config={
            "trade_date": st.column_config.DatetimeColumn("Date/Time", format="YYYY-MM-DD HH:mm"),
            "price": st.column_config.NumberColumn("Price", format="$%.2f"),
            "proceeds": st.column_config.NumberColumn("Proceeds", format="$%.2f"),
            "commission": st.column_config.NumberColumn("Commission", format="$%.2f"),
            "realized_pnl": st.column_config.NumberColumn("Realized P&L", format="$%.2f"),
            "strike": st.column_config.NumberColumn("Strike", format="$%.2f"),
        },
    )

    # CSV export
    st.download_button(
        label="Download Trade Log CSV",
        data=trade_log.to_csv(index=False),
        file_name=f"trades_{date_from.isoformat()}_{date_to.isoformat()}.csv",
        mime="text/csv",
    )
