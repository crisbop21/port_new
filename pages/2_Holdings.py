"""Holdings page — view positions as of any snapshot date and reconcile."""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.db import (
    get_account_ids,
    get_positions_as_of,
    get_snapshot_dates,
    reconcile_account,
    reconcile_pair,
)

st.title("Holdings")

# ── Account selector ────────────────────────────────────────────────────────

account_ids = get_account_ids()

if not account_ids:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

account_options = ["All Accounts"] + account_ids
selected_account = st.selectbox("Account", account_options)

# ── Collect snapshot dates per account ──────────────────────────────────────

if selected_account == "All Accounts":
    target_accounts = account_ids
else:
    target_accounts = [selected_account]

snapshot_dates_by_acct: dict[str, list[date]] = {}
for acct in target_accounts:
    snapshot_dates_by_acct[acct] = get_snapshot_dates(acct)

all_snapshot_dates = sorted(
    {d for dates in snapshot_dates_by_acct.values() for d in dates}
)

if not all_snapshot_dates:
    st.warning("No position snapshots found. Upload a statement with positions.")
    st.stop()

# ── Date picker ─────────────────────────────────────────────────────────────

as_of = st.date_input(
    "As-of date (snapshot)",
    value=all_snapshot_dates[-1],
    min_value=all_snapshot_dates[0],
    max_value=all_snapshot_dates[-1],
    help="Select a date that matches a position snapshot (statement_date).",
)

is_exact_snapshot = as_of in all_snapshot_dates

if not is_exact_snapshot:
    st.warning(
        f"**{as_of}** does not match any snapshot date. "
        f"Available snapshot dates: {', '.join(d.isoformat() for d in all_snapshot_dates)}"
    )
    st.stop()

# ── Load holdings ───────────────────────────────────────────────────────────

with st.spinner("Loading holdings..."):
    rows: list[dict] = []
    for acct in target_accounts:
        positions = get_positions_as_of(acct, as_of)
        for p in positions:
            multiplier = 100 if p.get("asset_class") == "OPT" else 1
            # cost_basis from IBKR is already total cost for the position
            cost_value = abs(float(p["cost_basis"]))
            rows.append({
                **p,
                "multiplier": multiplier,
                "cost_value": cost_value,
            })

if not rows:
    st.warning("No holdings found as of this date.")
    st.stop()

df = pd.DataFrame(rows)

# Convert numeric columns
numeric_cols = [
    "quantity", "cost_basis", "cost_value",
    "market_price", "market_value", "unrealized_pnl", "strike",
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Metric cards ────────────────────────────────────────────────────────────

has_market_data = "market_value" in df.columns and df["market_value"].notna().any()

if has_market_data:
    total_market_value = df["market_value"].sum()
    total_unrealized = df["unrealized_pnl"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Market Value", f"${total_market_value:,.2f}")
    col2.metric("Unrealized P&L", f"${total_unrealized:,.2f}",
                delta=f"{total_unrealized:,.2f}")
    col3.metric("Positions", len(df))
else:
    total_cost_value = df["cost_value"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Cost Value", f"${total_cost_value:,.2f}")
    col2.metric("Positions", len(df))
    col3.metric("As-of", str(as_of))
    st.caption(
        "Market value and unrealized P&L may be unavailable. "
        "Cost value is the total cost basis reported by IBKR."
    )

st.divider()

# ── Consolidated market value per symbol ───────────────────────────────────

st.subheader("Market Value by Symbol" if has_market_data else "Cost Basis by Symbol")

consol_rows = []
for _, row in df.iterrows():
    if has_market_data and pd.notna(row.get("market_value")):
        value = abs(float(row["market_value"]))
    elif row["asset_class"] == "OPT":
        value = float(row.get("strike", 0) or 0) * 100 * abs(float(row["quantity"]))
    else:
        value = abs(float(row["quantity"])) * float(row["cost_basis"])
    qty = abs(float(row["quantity"]))
    # cost_basis from IBKR is already total cost for the position
    cost = abs(float(row.get("cost_basis", 0) or 0))
    consol_rows.append({
        "symbol": row["symbol"],
        "market_value": value,
        "total_cost": cost,
        "quantity": qty,
        "asset_class": row["asset_class"],
        "strike": float(row.get("strike", 0) or 0),
        "right": row.get("right"),
    })

consol_df = (
    pd.DataFrame(consol_rows)
    .groupby("symbol", as_index=False)
    .agg(
        market_value=("market_value", "sum"),
        total_cost=("total_cost", "sum"),
        quantity=("quantity", "sum"),
        asset_class=("asset_class", "first"),
        strike=("strike", "first"),
        right=("right", "first"),
    )
)

# Breakeven calculation:
# STK/ETF: weighted average cost basis = total_cost / quantity
# OPT calls: strike + avg premium per share (total_cost / quantity / 100)
# OPT puts:  strike - avg premium per share (total_cost / quantity / 100)
safe_qty = consol_df["quantity"].replace(0, float("nan"))
is_opt = consol_df["asset_class"] == "OPT"
avg_premium = consol_df["total_cost"] / safe_qty / 100  # per-share premium for options
is_call = consol_df["right"] == "C"

consol_df["breakeven"] = float("nan")
consol_df.loc[~is_opt, "breakeven"] = consol_df.loc[~is_opt, "total_cost"] / safe_qty[~is_opt]
consol_df.loc[is_opt & is_call, "breakeven"] = (
    consol_df.loc[is_opt & is_call, "strike"] + avg_premium[is_opt & is_call]
)
consol_df.loc[is_opt & ~is_call, "breakeven"] = (
    consol_df.loc[is_opt & ~is_call, "strike"] - avg_premium[is_opt & ~is_call]
)
total_mv = consol_df["market_value"].sum()
consol_df["pct_of_account"] = (
    (consol_df["market_value"] / total_mv * 100) if total_mv else 0.0
)
consol_df = consol_df.sort_values("market_value", ascending=False).reset_index(drop=True)

col_table, col_chart = st.columns([1, 1])

with col_table:
    display_mv = consol_df[["symbol", "quantity", "breakeven", "market_value", "pct_of_account"]].copy()
    display_mv["quantity"] = display_mv["quantity"].map(lambda v: f"{v:,.2f}")
    display_mv["breakeven"] = display_mv["breakeven"].map(lambda v: f"${v:,.2f}")
    display_mv["market_value"] = display_mv["market_value"].map(lambda v: f"${v:,.2f}")
    display_mv["pct_of_account"] = display_mv["pct_of_account"].map(lambda v: f"{v:.1f}%")
    display_mv = display_mv.rename(columns={
        "symbol": "Symbol",
        "quantity": "Total Qty",
        "breakeven": "Avg Breakeven",
        "market_value": "Market Value ($)",
        "pct_of_account": "% of Account",
    })
    st.dataframe(display_mv, use_container_width=True, hide_index=True)

with col_chart:
    import altair as alt

    chart_data = consol_df.copy()
    pie = (
        alt.Chart(chart_data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("market_value:Q", title="Market Value"),
            color=alt.Color("symbol:N", title="Symbol"),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("market_value:Q", title="Market Value", format="$,.2f"),
                alt.Tooltip("pct_of_account:Q", title="% of Account", format=".1f"),
            ],
        )
        .properties(height=350)
    )
    st.altair_chart(pie, use_container_width=True)

st.divider()

# ── Table grouped by asset class ────────────────────────────────────────────

if has_market_data:
    display_cols = [
        "symbol", "asset_class", "quantity", "cost_basis",
        "multiplier", "cost_value",
        "market_price", "market_value", "unrealized_pnl",
    ]
else:
    display_cols = ["symbol", "asset_class", "quantity", "cost_basis", "multiplier", "cost_value"]

if df["asset_class"].eq("OPT").any():
    display_cols += ["expiry", "strike", "right"]

for asset_class, group in df.groupby("asset_class", sort=True):
    label = {"STK": "Stocks", "OPT": "Options", "ETF": "ETFs"}.get(asset_class, asset_class)
    st.subheader(f"{label} ({len(group)})")

    show_cols = [c for c in display_cols if c in group.columns]
    detail_df = group[show_cols].sort_values("symbol").reset_index(drop=True).copy()
    dollar_cols = ["cost_value", "market_value", "cost_basis", "market_price", "unrealized_pnl", "strike"]
    for col in dollar_cols:
        if col in detail_df.columns:
            detail_df[col] = detail_df[col].map(lambda v: f"${v:,.2f}")
    if "multiplier" in detail_df.columns:
        detail_df["multiplier"] = detail_df["multiplier"].astype(int)
    col_labels = {
        "cost_value": "Cost Value", "market_value": "Market Value",
        "cost_basis": "Cost Basis", "market_price": "Market Price",
        "unrealized_pnl": "Unrealized P&L", "multiplier": "Mult",
    }
    detail_df = detail_df.rename(columns={k: v for k, v in col_labels.items() if k in detail_df.columns})
    st.dataframe(detail_df, use_container_width=True)

# ── Reconciliation ──────────────────────────────────────────────────────────

st.divider()
st.subheader("Reconciliation")
st.caption(
    "For each consecutive pair of position snapshots (by statement_date), "
    "rolls forward from base holdings through trades and compares against "
    "the next snapshot. Mismatches indicate missing trades, corporate actions, "
    "or data gaps."
)

# Check if any account has enough snapshots
any_reconcilable = any(
    len(dates) >= 2 for dates in snapshot_dates_by_acct.values()
)

if not any_reconcilable:
    st.info(
        "Each account needs at least 2 position snapshots (different statement_dates) "
        "to reconcile. Upload more statements to enable this."
    )
    st.stop()

# Optional: pick a specific pair
all_recon_dates = sorted(
    {d for dates in snapshot_dates_by_acct.values() for d in dates}
)

use_custom_dates = False
custom_base: date | None = None
custom_target: date | None = None

if len(all_recon_dates) >= 2:
    use_custom_dates = st.checkbox(
        "Choose reconciliation dates",
        help="Select specific base and target snapshot dates instead of running all pairs.",
    )

if use_custom_dates and len(all_recon_dates) >= 2:
    col_b, col_t = st.columns(2)
    with col_b:
        custom_base = st.selectbox(
            "Base date (start from)",
            options=all_recon_dates[:-1],
            index=0,
            format_func=lambda d: d.isoformat(),
        )
    with col_t:
        valid_targets = [d for d in all_recon_dates if d > custom_base]
        custom_target = st.selectbox(
            "Target date (compare against)",
            options=valid_targets,
            index=len(valid_targets) - 1 if valid_targets else 0,
            format_func=lambda d: d.isoformat(),
        )

run_recon = st.button("Run Reconciliation", type="primary")

if not run_recon:
    st.stop()

for acct in target_accounts:
    acct_dates = snapshot_dates_by_acct.get(acct, [])
    if len(acct_dates) < 2:
        st.info(f"**{acct}**: Only {len(acct_dates)} snapshot(s) — need at least 2 to reconcile.")
        continue

    if use_custom_dates and custom_base and custom_target:
        # Validate the chosen dates exist for this account
        if custom_base not in acct_dates or custom_target not in acct_dates:
            st.info(
                f"**{acct}**: Selected dates not available for this account. "
                f"Available: {', '.join(d.isoformat() for d in acct_dates)}"
            )
            continue
        pair_results = [reconcile_pair(acct, custom_base, custom_target)]
    else:
        pair_results = reconcile_account(acct)

    st.markdown(f"### {acct}")

    for result in pair_results:
        header = f"**{result['base_date']} -> {result['target_date']}**"

        if result["ok"]:
            st.success(f"{header}: All holdings reconcile.")
        else:
            st.error(f"{header}: Reconciliation FAILED — differences found.")

        # Gaps summary
        gaps = result.get("gaps", {})
        if gaps.get("missing_from_target"):
            with st.expander(
                f"Reconstructed but not in target snapshot ({len(gaps['missing_from_target'])})",
                expanded=True,
            ):
                st.caption(
                    "These positions exist after rolling forward but are absent from "
                    "the target snapshot. Possible causes: position closed by a missing "
                    "trade, option expired/exercised, or corporate action."
                )
                st.dataframe(pd.DataFrame(gaps["missing_from_target"]), use_container_width=True)

        if gaps.get("missing_from_reconstruction"):
            with st.expander(
                f"In target but not reconstructed ({len(gaps['missing_from_reconstruction'])})",
                expanded=True,
            ):
                st.caption(
                    "These positions appear in the target snapshot but could not be "
                    "built from the base snapshot + trades. Possible causes: "
                    "missing statement, transfer-in, or corporate action."
                )
                st.dataframe(
                    pd.DataFrame(gaps["missing_from_reconstruction"]),
                    use_container_width=True,
                )

        # Per-holding detail
        if result["holdings"]:
            summary_rows = []
            for symbol, h in result["holdings"].items():
                summary_rows.append({
                    "Symbol": symbol,
                    "Base Qty": h["base_qty"],
                    "Trades": len(h["trades"]),
                    "Reconstructed Qty": h["reconstructed_qty"],
                    "Expected Qty": h["expected_qty"],
                    "Diff": h["diff"],
                    "Match": h["match"],
                })

            summary_df = pd.DataFrame(summary_rows)
            for col in ["Base Qty", "Reconstructed Qty", "Expected Qty", "Diff"]:
                summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce")

            mismatched = summary_df[~summary_df["Match"]].reset_index(drop=True)
            matched = summary_df[summary_df["Match"]].reset_index(drop=True)

            if not mismatched.empty:
                st.error(f"{len(mismatched)} holding(s) do not match:")
                st.dataframe(
                    mismatched,
                    use_container_width=True,
                    column_config={
                        "Base Qty": st.column_config.NumberColumn(format="%.4f"),
                        "Reconstructed Qty": st.column_config.NumberColumn(format="%.4f"),
                        "Expected Qty": st.column_config.NumberColumn(format="%.4f"),
                        "Diff": st.column_config.NumberColumn(format="%.4f"),
                    },
                )

            if not matched.empty:
                with st.expander(f"{len(matched)} holding(s) match", expanded=False):
                    st.dataframe(
                        matched,
                        use_container_width=True,
                        column_config={
                            "Base Qty": st.column_config.NumberColumn(format="%.4f"),
                            "Reconstructed Qty": st.column_config.NumberColumn(format="%.4f"),
                            "Expected Qty": st.column_config.NumberColumn(format="%.4f"),
                            "Diff": st.column_config.NumberColumn(format="%.4f"),
                        },
                    )

            # Per-holding trade ledger expanders
            for symbol, h in result["holdings"].items():
                status = "OK" if h["match"] else "MISMATCH"
                with st.expander(
                    f"{symbol}: {h['base_qty']} -> {h['reconstructed_qty']} "
                    f"(expected {h['expected_qty']}) [{status}]",
                    expanded=not h["match"],
                ):
                    if not h["trades"]:
                        st.caption("No trades in this period.")
                    else:
                        ledger_df = pd.DataFrame(h["trades"])
                        ledger_df.columns = ["Date", "Side", "Quantity", "Running Qty"]
                        for col in ["Quantity", "Running Qty"]:
                            ledger_df[col] = pd.to_numeric(ledger_df[col], errors="coerce")
                        st.dataframe(
                            ledger_df,
                            use_container_width=True,
                            column_config={
                                "Quantity": st.column_config.NumberColumn(format="%.4f"),
                                "Running Qty": st.column_config.NumberColumn(format="%.4f"),
                            },
                        )
