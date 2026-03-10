"""Holdings page — reconstruct positions as of any date in the statement range."""

from datetime import date

import pandas as pd
import streamlit as st

from src.db import get_statements, reconstruct_holdings

st.title("Holdings")

# ── Statement selector ───────────────────────────────────────────────────────

statements = get_statements()

if not statements:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

# Group statements by account; track widest date range and latest statement
accounts: dict[str, dict] = {}
for s in statements:
    acct = s["account_id"]
    p_start = date.fromisoformat(s["period_start"])
    p_end = date.fromisoformat(s["period_end"])
    if acct not in accounts:
        accounts[acct] = {
            "latest_id": s["id"],
            "period_start": p_start,
            "period_end": p_end,
        }
    else:
        info = accounts[acct]
        if p_end > info["period_end"]:
            info["latest_id"] = s["id"]
            info["period_end"] = p_end
        if p_start < info["period_start"]:
            info["period_start"] = p_start

account_list = list(accounts.keys())
selected_account = st.selectbox("Account", account_list)
acct_info = accounts[selected_account]

# ── Date picker ──────────────────────────────────────────────────────────────

as_of = st.date_input(
    "As-of date",
    value=acct_info["period_end"],
    min_value=acct_info["period_start"],
    max_value=acct_info["period_end"],
    help="Positions are reconstructed by reversing trades after this date.",
)

is_historical = as_of < acct_info["period_end"]

# ── Reconstruct holdings ────────────────────────────────────────────────────

with st.spinner("Reconstructing holdings..."):
    rows = reconstruct_holdings(acct_info["latest_id"], as_of)

if not rows:
    st.warning("No holdings found as of this date.")
    st.stop()

df = pd.DataFrame(rows)

# Convert numeric columns from strings
numeric_cols = [
    "quantity", "cost_basis", "cost_value",
    "market_price", "market_value", "unrealized_pnl", "strike",
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Metric cards ─────────────────────────────────────────────────────────────

if is_historical:
    # No market data for historical dates — show cost-based metrics
    total_cost_value = df["cost_value"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Cost Value", f"${total_cost_value:,.2f}")
    col2.metric("Positions", len(df))
    col3.metric("As-of", str(as_of))
    st.caption(
        "Market value and unrealized P&L are unavailable for historical dates "
        "(no price API). Cost value = quantity × cost basis × multiplier."
    )
else:
    total_market_value = df["market_value"].sum()
    total_unrealized = df["unrealized_pnl"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Market Value", f"${total_market_value:,.2f}")
    col2.metric("Unrealized P&L", f"${total_unrealized:,.2f}",
                delta=f"{total_unrealized:,.2f}")
    col3.metric("Positions", len(df))

st.divider()

# ── Table grouped by asset class ─────────────────────────────────────────────

if is_historical:
    display_cols = ["symbol", "asset_class", "quantity", "cost_basis", "multiplier", "cost_value"]
else:
    display_cols = [
        "symbol", "asset_class", "quantity", "cost_basis",
        "multiplier", "cost_value",
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
            "cost_value": st.column_config.NumberColumn("Cost Value", format="$%.2f"),
            "market_value": st.column_config.NumberColumn("Market Value", format="$%.2f"),
            "cost_basis": st.column_config.NumberColumn("Cost Basis", format="$%.2f"),
            "market_price": st.column_config.NumberColumn("Market Price", format="$%.2f"),
            "unrealized_pnl": st.column_config.NumberColumn("Unrealized P&L", format="$%.2f"),
            "strike": st.column_config.NumberColumn(format="$%.2f"),
            "multiplier": st.column_config.NumberColumn("Mult", format="%d"),
        },
    )
