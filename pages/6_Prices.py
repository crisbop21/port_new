"""Prices — fetch and view daily stock prices from Yahoo Finance."""

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
    get_portfolio_symbols,
    upsert_daily_prices,
)
from src.price_fetcher import fetch_daily_prices, fetch_prices_for_symbols

st.title("Daily Prices")

# ── Account selector ─────────────────────────────────────────────────────────

account_ids = get_account_ids()
if not account_ids:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

account_options = ["All Accounts"] + account_ids
selected_account = st.selectbox("Account", account_options)
account_filter = None if selected_account == "All Accounts" else selected_account

symbols = get_portfolio_symbols(account_filter)
if not symbols:
    st.info("No stock/ETF positions found. Upload a statement with holdings first.")
    st.stop()

# Always include benchmark symbols for beta calculation
BENCHMARK_SYMBOLS = ["SPY", "QQQ"]
benchmarks_to_add = [b for b in BENCHMARK_SYMBOLS if b not in symbols]
symbols_with_benchmarks = symbols + benchmarks_to_add

# ── Fetch controls ───────────────────────────────────────────────────────────

st.subheader("Fetch Prices")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=365),
    )
with col2:
    end_date = st.date_input("End date", value=date.today())

st.write(f"**Portfolio symbols:** {', '.join(symbols)}")
if benchmarks_to_add:
    st.caption(f"Benchmarks (for beta calculation): {', '.join(benchmarks_to_add)}")

if st.button("Fetch All Portfolio Prices", type="primary"):
    fetch_list = symbols_with_benchmarks
    progress = st.progress(0, text="Fetching prices...")
    all_prices = []
    all_errors = []

    for i, sym in enumerate(fetch_list):
        progress.progress(
            (i + 1) / len(fetch_list),
            text=f"Fetching {sym} ({i + 1}/{len(fetch_list)})...",
        )
        prices, errs = fetch_daily_prices(sym, start=start_date, end=end_date)
        all_prices.extend(prices)
        all_errors.extend(errs)

    progress.empty()

    if all_errors:
        with st.expander(f"{len(all_errors)} warnings/errors"):
            for err in all_errors:
                st.warning(err)

    if all_prices:
        with st.spinner("Saving to database..."):
            inserted, updated, db_errors = upsert_daily_prices(all_prices)

        st.success(
            f"Saved {inserted + updated} price rows "
            f"({inserted} new, {updated} updated) "
            f"for {len(fetch_list)} symbols."
        )
        if db_errors:
            for err in db_errors:
                st.error(err)
    else:
        st.warning("No price data fetched.")

st.divider()

# ── Single symbol fetch ──────────────────────────────────────────────────────

with st.expander("Fetch a single symbol"):
    single_sym = st.text_input("Symbol", placeholder="e.g. AAPL").strip().upper()
    if single_sym and st.button("Fetch"):
        with st.spinner(f"Fetching {single_sym}..."):
            prices, errs = fetch_daily_prices(single_sym, start=start_date, end=end_date)
        if errs:
            for err in errs:
                st.warning(err)
        if prices:
            inserted, updated, db_errors = upsert_daily_prices(prices)
            st.success(f"{single_sym}: {inserted} new, {updated} updated rows.")
            if db_errors:
                for err in db_errors:
                    st.error(err)
        else:
            st.warning(f"No data returned for {single_sym}.")

st.divider()

# ── View stored prices ───────────────────────────────────────────────────────

st.subheader("Stored Prices")

# Latest price summary
latest_data = []
for sym in symbols_with_benchmarks:
    latest = get_latest_price(sym)
    if latest:
        latest_data.append({
            "Symbol": latest["symbol"],
            "Date": latest["price_date"],
            "Close": float(latest["close"]),
            "Adj Close": float(latest["adj_close"]),
            "Volume": latest["volume"],
        })

if latest_data:
    latest_df = pd.DataFrame(latest_data)
    st.dataframe(
        latest_df,
        use_container_width=True,
        column_config={
            "Close": st.column_config.NumberColumn("Close", format="$%.2f"),
            "Adj Close": st.column_config.NumberColumn("Adj Close", format="$%.2f"),
            "Volume": st.column_config.NumberColumn("Volume", format="%d"),
        },
    )
else:
    st.info("No prices stored yet. Use the fetch button above.")

# Per-symbol chart
selected_sym = st.selectbox("View price chart", [""] + symbols_with_benchmarks)
if selected_sym:
    price_rows = get_daily_prices(selected_sym, date_from=start_date, date_to=end_date)
    if price_rows:
        pdf = pd.DataFrame(price_rows)
        pdf["price_date"] = pd.to_datetime(pdf["price_date"])
        pdf["adj_close"] = pd.to_numeric(pdf["adj_close"], errors="coerce")

        st.line_chart(pdf, x="price_date", y="adj_close")

        with st.expander("Raw data"):
            st.dataframe(pdf, use_container_width=True)
    else:
        st.info(f"No stored prices for {selected_sym} in the selected date range.")
