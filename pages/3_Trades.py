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

    date_col1, date_col2 = st.columns(2)
    date_from = date_col1.date_input("From", value=date.today() - timedelta(days=90))
    date_to = date_col2.date_input("To", value=date.today())

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
    st.info("No trades match the current filters.")
    st.stop()

df = pd.DataFrame(rows)

numeric_cols = ["quantity", "price", "proceeds", "commission", "realized_pnl"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

if "trade_date" in df.columns:
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

# ── Summary metrics (row 1) ─────────────────────────────────────────────────

total_realized = df["realized_pnl"].sum()
total_commission = df["commission"].sum()
trade_count = len(df)

col1, col2, col3 = st.columns(3)
col1.metric("Total Realized P&L", f"${total_realized:,.2f}",
            delta=f"{total_realized:,.2f}")
col2.metric("Total Commissions", f"${total_commission:,.2f}")
col3.metric("Trades", trade_count)

# ── Win / Loss metrics (row 2) ──────────────────────────────────────────────

# Only count trades where P&L is non-zero (opening legs typically have 0 P&L)
closing_trades = df[df["realized_pnl"] != 0]

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

st.divider()

# ── Charts ───────────────────────────────────────────────────────────────────

chart_left, chart_right = st.columns(2)

# Daily P&L bar chart
with chart_left:
    st.subheader("Daily P&L")
    daily_pnl = (
        df.dropna(subset=["trade_date"])
        .groupby(df["trade_date"].dt.date)["realized_pnl"]
        .sum()
        .reset_index()
        .rename(columns={"trade_date": "Date", "realized_pnl": "Realized P&L"})
    )
    if not daily_pnl.empty:
        st.bar_chart(daily_pnl, x="Date", y="Realized P&L")

# Cumulative P&L line chart
with chart_right:
    st.subheader("Cumulative P&L")
    if not daily_pnl.empty:
        cum = daily_pnl.copy()
        cum["Cumulative P&L"] = cum["Realized P&L"].cumsum()
        st.line_chart(cum, x="Date", y="Cumulative P&L")

# P&L Distribution histogram
if not closing_trades.empty:
    st.subheader("P&L Distribution")
    hist_data = closing_trades[["realized_pnl"]].rename(columns={"realized_pnl": "Realized P&L"})
    st.bar_chart(
        hist_data["Realized P&L"]
        .value_counts(bins=20)
        .sort_index()
        .rename("Count")
    )

st.divider()

# ── Per-symbol breakdown (consolidated by underlying) ────────────────────────

st.subheader("P&L by Symbol")

# Extract the underlying ticker (first word) so option contracts
# like "AAPL 20240119 150.0 C" consolidate under "AAPL".
closing_trades = closing_trades.copy()
closing_trades["underlying"] = closing_trades["symbol"].str.split().str[0]

symbol_stats = (
    closing_trades.groupby("underlying")
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

    disp_stats = symbol_stats[["symbol", "total_pnl", "trades", "win_rate", "avg_pnl", "total_commission"]].copy()
    disp_stats["total_pnl"] = disp_stats["total_pnl"].map(lambda v: f"${v:,.2f}")
    disp_stats["avg_pnl"] = disp_stats["avg_pnl"].map(lambda v: f"${v:,.2f}")
    disp_stats["total_commission"] = disp_stats["total_commission"].map(lambda v: f"${v:,.2f}")
    disp_stats["win_rate"] = disp_stats["win_rate"].map(lambda v: f"{v:.1f}%")
    disp_stats = disp_stats.rename(columns={
        "symbol": "Symbol", "total_pnl": "Total P&L", "trades": "Trades",
        "win_rate": "Win Rate %", "avg_pnl": "Avg P&L", "total_commission": "Commissions",
    })
    st.dataframe(disp_stats, use_container_width=True, hide_index=True)

    # Top winners / losers bar chart
    top_n = min(10, len(symbol_stats))
    st.bar_chart(
        symbol_stats.head(top_n).set_index("symbol")["total_pnl"].rename("P&L by Symbol")
    )

st.divider()

# ── Unrealized P&L by Symbol ────────────────────────────────────────────────

st.subheader("Unrealized P&L by Symbol")

# Load ALL open positions from the latest snapshot for each account.
# This is independent of the trade-date filters above — it always
# reflects the most recent position snapshot so you see every holding.
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

    # Fill missing unrealized_pnl with 0 (positions with no market movement)
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

    # Consolidate by underlying ticker
    pos_df["underlying"] = pos_df["symbol"].str.split().str[0]

    # Ensure strike/right columns exist for breakeven calc
    if "strike" not in pos_df.columns:
        pos_df["strike"] = float("nan")
    else:
        pos_df["strike"] = pd.to_numeric(pos_df["strike"], errors="coerce")
    if "right" not in pos_df.columns:
        pos_df["right"] = None
    if "asset_class" not in pos_df.columns:
        pos_df["asset_class"] = "STK"

    # Per-position breakeven and weight (shares equivalent)
    # cost_basis from IBKR is already the TOTAL cost for the position
    # STK/ETF: breakeven = cost_basis / |quantity|  (per-share avg cost)
    # OPT: premium_per_share = cost_basis / (|quantity| * 100)
    # OPT call: breakeven = strike + premium_per_share
    # OPT put:  breakeven = strike - premium_per_share
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

    # Weighted-average breakeven across all legs
    safe_weight = unrealized_stats["weight"].replace(0, float("nan"))
    unrealized_stats["breakeven"] = (
        unrealized_stats["weighted_be"] / safe_weight
    ).abs()

    total_unrealized = unrealized_stats["unrealized_pnl"].sum()
    st.metric("Total Unrealized P&L", f"${total_unrealized:,.2f}",
              delta=f"{total_unrealized:,.2f}")

    disp_unreal = unrealized_stats[["symbol", "quantity", "breakeven", "market_value", "unrealized_pnl", "positions"]].copy()
    disp_unreal["quantity"] = disp_unreal["quantity"].map(lambda v: f"{v:,.2f}")
    disp_unreal["breakeven"] = disp_unreal["breakeven"].map(lambda v: f"${v:,.2f}")
    disp_unreal["market_value"] = disp_unreal["market_value"].map(lambda v: f"${v:,.2f}")
    disp_unreal["unrealized_pnl"] = disp_unreal["unrealized_pnl"].map(lambda v: f"${v:,.2f}")
    disp_unreal = disp_unreal.rename(columns={
        "symbol": "Symbol", "quantity": "Total Qty", "breakeven": "Avg Breakeven",
        "market_value": "Market Value", "unrealized_pnl": "Unrealized P&L", "positions": "Positions",
    })
    st.dataframe(disp_unreal, use_container_width=True, hide_index=True)

    # Bar chart of unrealized P&L by symbol
    top_u = min(10, len(unrealized_stats))
    st.bar_chart(
        unrealized_stats.head(top_u).set_index("symbol")["unrealized_pnl"]
        .rename("Unrealized P&L by Symbol")
    )

st.divider()

# ── Trade table ──────────────────────────────────────────────────────────────

st.subheader("Trade Log")

display_cols = [
    "trade_date", "symbol", "asset_class", "side",
    "quantity", "price", "proceeds", "commission", "realized_pnl",
]
if df["asset_class"].eq("OPT").any():
    display_cols += ["expiry", "strike", "right"]

show_cols = [c for c in display_cols if c in df.columns]

trade_log = df[show_cols].reset_index(drop=True).copy()
for col in ["price", "proceeds", "commission", "realized_pnl", "strike"]:
    if col in trade_log.columns:
        trade_log[col] = trade_log[col].map(lambda v: f"${v:,.2f}")
st.dataframe(
    trade_log,
    use_container_width=True,
    column_config={
        "trade_date": st.column_config.DatetimeColumn("Date/Time", format="YYYY-MM-DD HH:mm"),
        "realized_pnl": "Realized P&L",
    },
)
