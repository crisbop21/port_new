"""Stock Metrics — fundamental data from SEC EDGAR for portfolio holdings."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.db import (
    clear_query_caches,
    get_account_ids,
    get_latest_stock_metrics,
    get_metrics_for_symbols,
    get_portfolio_symbols,
    get_stock_metrics,
    upsert_stock_metrics,
)
from src.fetcher import fetch_metrics_for_symbol

st.title("Stock Metrics")

# ── Account selector ─────────────────────────────────────────────────────────

account_ids = get_account_ids()
if not account_ids:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

account_options = ["All Accounts"] + account_ids
selected_account = st.selectbox("Account", account_options)
account_filter = None if selected_account == "All Accounts" else selected_account

# ── Portfolio symbols ────────────────────────────────────────────────────────

symbols = get_portfolio_symbols(account_id=account_filter)
if not symbols:
    st.info("No stock or ETF positions found. Upload a statement with stock holdings first.")
    st.stop()

st.caption(f"{len(symbols)} stock/ETF symbols in portfolio")

# ── Fetch controls ───────────────────────────────────────────────────────────

st.subheader("Fetch from SEC EDGAR")

fetch_symbols = st.multiselect(
    "Symbols to fetch",
    options=symbols,
    default=symbols,
    help="Select which symbols to fetch fundamental data for. ETFs typically have no SEC filings.",
)

col_fetch, col_status = st.columns([1, 3])

with col_fetch:
    fetch_clicked = st.button("Fetch Metrics", type="primary", disabled=not fetch_symbols)

if fetch_clicked:
    all_metrics = []
    all_errors = []
    progress = st.progress(0, text="Starting...")

    for i, sym in enumerate(fetch_symbols):
        progress.progress(
            (i + 1) / len(fetch_symbols),
            text=f"Fetching {sym} ({i + 1}/{len(fetch_symbols)})...",
        )
        metrics, errors = fetch_metrics_for_symbol(sym)
        all_metrics.extend(metrics)
        all_errors.extend(errors)

    progress.empty()

    # Save to database
    if all_metrics:
        with st.spinner("Saving to database..."):
            inserted, updated, db_errors = upsert_stock_metrics(all_metrics)
            all_errors.extend(db_errors)
            clear_query_caches()

        st.success(
            f"Done: {inserted} new metrics inserted, {updated} updated "
            f"across {len(fetch_symbols)} symbols."
        )
    else:
        st.warning("No metrics were fetched. Check the issues below.")

    # Show issues if any
    if all_errors:
        with st.expander(f"Issues ({len(all_errors)})", expanded=len(all_metrics) == 0):
            for err in all_errors:
                st.text(err)

st.divider()

# ── Portfolio-wide metrics table ─────────────────────────────────────────────

st.subheader("Portfolio Fundamentals")

metrics_data = get_metrics_for_symbols(symbols)

if not metrics_data:
    st.info(
        "No metrics in database yet. Use the **Fetch Metrics** button above "
        "to pull fundamental data from SEC EDGAR."
    )
    st.stop()

# Build a summary table: rows = symbols, columns = metrics
DISPLAY_METRICS = [
    ("revenue", "Revenue", "${:,.0f}"),
    ("net_income", "Net Income", "${:,.0f}"),
    ("eps_diluted", "EPS (Diluted)", "${:,.2f}"),
    ("total_assets", "Total Assets", "${:,.0f}"),
    ("total_liabilities", "Total Liabilities", "${:,.0f}"),
    ("stockholders_equity", "Equity", "${:,.0f}"),
    ("shares_outstanding", "Shares Out", "{:,.0f}"),
    ("operating_income", "Operating Income", "${:,.0f}"),
    ("cash_and_equivalents", "Cash", "${:,.0f}"),
]

rows = []
for sym in sorted(metrics_data.keys()):
    sym_metrics = metrics_data[sym]
    row: dict = {"Symbol": sym}
    for metric_key, display_name, fmt in DISPLAY_METRICS:
        if metric_key in sym_metrics:
            raw_val = sym_metrics[metric_key].get("metric_value")
            if raw_val is not None:
                try:
                    val = float(raw_val)
                    row[display_name] = fmt.format(val)
                except (ValueError, TypeError):
                    row[display_name] = str(raw_val)
            else:
                row[display_name] = "—"
        else:
            row[display_name] = "—"

    # Add the fiscal period for context
    any_metric = next(iter(sym_metrics.values()), {})
    row["Period End"] = any_metric.get("period_end", "—")
    row["Filing"] = any_metric.get("filing_type", "—")
    rows.append(row)

if rows:
    summary_df = pd.DataFrame(rows)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
else:
    st.info("No metrics to display.")

st.divider()

# ── Per-symbol detail view ───────────────────────────────────────────────────

st.subheader("Symbol Detail")

symbols_with_data = sorted(metrics_data.keys())
if not symbols_with_data:
    st.stop()

detail_symbol = st.selectbox(
    "Select symbol",
    options=symbols_with_data,
    key="detail_symbol",
)

if detail_symbol:
    latest = get_latest_stock_metrics(detail_symbol)

    if not latest:
        st.info(f"No metrics stored for {detail_symbol}.")
    else:
        # Metric cards
        card_metrics = [
            ("revenue", "Revenue"),
            ("net_income", "Net Income"),
            ("eps_diluted", "EPS (Diluted)"),
            ("operating_income", "Operating Income"),
        ]

        cols = st.columns(len(card_metrics))
        for col, (key, label) in zip(cols, card_metrics):
            if key in latest:
                raw = latest[key].get("metric_value")
                try:
                    val = float(raw)
                    if key.startswith("eps"):
                        display = f"${val:,.2f}"
                    else:
                        display = f"${val:,.0f}"
                except (ValueError, TypeError):
                    display = str(raw)
                col.metric(label, display)
            else:
                col.metric(label, "—")

        # Historical data for this symbol
        with st.expander("Historical metrics"):
            hist_metric = st.selectbox(
                "Metric",
                options=sorted(latest.keys()),
                key="hist_metric",
            )

            if hist_metric:
                history = get_stock_metrics(symbol=detail_symbol, metric_name=hist_metric)
                if history:
                    hist_df = pd.DataFrame(history)
                    hist_df["period_end"] = pd.to_datetime(hist_df["period_end"])
                    hist_df["metric_value"] = pd.to_numeric(hist_df["metric_value"], errors="coerce")
                    hist_df = hist_df.sort_values("period_end")

                    st.line_chart(hist_df, x="period_end", y="metric_value")

                    # Raw data table
                    display_cols = ["period_end", "metric_value", "filing_type", "source"]
                    available = [c for c in display_cols if c in hist_df.columns]
                    st.dataframe(
                        hist_df[available].reset_index(drop=True),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info(f"No historical data for {hist_metric}.")

        # Debug info
        with st.expander("Debug: raw metric data"):
            for name, row in sorted(latest.items()):
                st.text(
                    f"{name}: value={row.get('metric_value')}  "
                    f"period={row.get('period_end')}  "
                    f"filing={row.get('filing_type')}  "
                    f"cik={row.get('cik')}  "
                    f"fetched={row.get('fetched_at')}"
                )
