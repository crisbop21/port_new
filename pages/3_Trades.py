"""Trade History page — filterable, sortable trade table with summary metrics."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from src.db import get_trades

st.set_page_config(page_title="Trade History", page_icon="📈", layout="wide")
st.title("Trade History")

# ── Filters ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    date_col1, date_col2 = st.columns(2)
    date_from = date_col1.date_input("From", value=date.today() - timedelta(days=90))
    date_to = date_col2.date_input("To", value=date.today())

    symbol = st.text_input("Symbol", placeholder="e.g. AAPL").strip().upper() or None

    asset_class = st.selectbox("Asset class", ["All", "STK", "OPT", "ETF"])
    asset_class_filter = None if asset_class == "All" else asset_class

    side = st.selectbox("Side", ["All", "BOT", "SLD"])
    side_filter = None if side == "All" else side

# ── Load trades ──────────────────────────────────────────────────────────────

rows = get_trades(
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

# ── Summary metrics ──────────────────────────────────────────────────────────

total_realized = df["realized_pnl"].sum()
total_commission = df["commission"].sum()
trade_count = len(df)

col1, col2, col3 = st.columns(3)
col1.metric("Total Realized P&L", f"${total_realized:,.2f}",
            delta=f"{total_realized:,.2f}")
col2.metric("Total Commissions", f"${total_commission:,.2f}")
col3.metric("Trades", trade_count)

st.divider()

# ── Trade table ──────────────────────────────────────────────────────────────

display_cols = [
    "trade_date", "symbol", "asset_class", "side",
    "quantity", "price", "proceeds", "commission", "realized_pnl",
]
if df["asset_class"].eq("OPT").any():
    display_cols += ["expiry", "strike", "right"]

show_cols = [c for c in display_cols if c in df.columns]

st.dataframe(
    df[show_cols].reset_index(drop=True),
    use_container_width=True,
    column_config={
        "trade_date": st.column_config.DatetimeColumn("Date/Time", format="YYYY-MM-DD HH:mm"),
        "price": st.column_config.NumberColumn(format="$%.2f"),
        "proceeds": st.column_config.NumberColumn(format="$%.2f"),
        "commission": st.column_config.NumberColumn(format="$%.4f"),
        "realized_pnl": st.column_config.NumberColumn("Realized P&L", format="$%.2f"),
        "strike": st.column_config.NumberColumn(format="$%.2f"),
    },
)
