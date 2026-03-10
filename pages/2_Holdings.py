"""Holdings page — view positions from a selected statement."""

import pandas as pd
import streamlit as st

from src.db import get_positions, get_statements

st.set_page_config(page_title="Holdings", page_icon="📂", layout="wide")
st.title("Holdings")

# ── Statement selector ───────────────────────────────────────────────────────

statements = get_statements()

if not statements:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

options = {
    s["id"]: f"{s['account_id']}  |  {s['period_start']} → {s['period_end']}"
    for s in statements
}

selected_id = st.selectbox(
    "Select statement",
    options.keys(),
    format_func=lambda k: options[k],
)

# ── Load positions ───────────────────────────────────────────────────────────

rows = get_positions(selected_id)

if not rows:
    st.warning("No positions found for this statement.")
    st.stop()

df = pd.DataFrame(rows)

# Convert numeric columns from strings
numeric_cols = ["quantity", "cost_basis", "market_price", "market_value", "unrealized_pnl"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Metric cards ─────────────────────────────────────────────────────────────

total_market_value = df["market_value"].sum()
total_unrealized = df["unrealized_pnl"].sum()
position_count = len(df)

col1, col2, col3 = st.columns(3)
col1.metric("Total Market Value", f"${total_market_value:,.2f}")
col2.metric("Unrealized P&L", f"${total_unrealized:,.2f}",
            delta=f"{total_unrealized:,.2f}")
col3.metric("Positions", position_count)

st.divider()

# ── Table grouped by asset class ─────────────────────────────────────────────

display_cols = [
    "symbol", "asset_class", "quantity", "cost_basis",
    "market_price", "market_value", "unrealized_pnl",
]
# Include option fields if any options exist
if df["asset_class"].eq("OPT").any():
    display_cols += ["expiry", "strike", "right"]

for asset_class, group in df.groupby("asset_class", sort=True):
    label = {"STK": "Stocks", "OPT": "Options", "ETF": "ETFs"}.get(asset_class, asset_class)
    st.subheader(f"{label} ({len(group)})")

    show_cols = [c for c in display_cols if c in group.columns]
    st.dataframe(
        group[show_cols].sort_values("symbol").reset_index(drop=True),
        use_container_width=True,
        column_config={
            "market_value": st.column_config.NumberColumn(format="$%.2f"),
            "cost_basis": st.column_config.NumberColumn(format="$%.2f"),
            "market_price": st.column_config.NumberColumn(format="$%.2f"),
            "unrealized_pnl": st.column_config.NumberColumn(format="$%.2f"),
            "strike": st.column_config.NumberColumn(format="$%.2f"),
        },
    )
