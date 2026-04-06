"""Technical Analysis — signal-based ranking of portfolio holdings."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from src.db import get_account_ids, get_daily_prices, get_portfolio_symbols
from src.technical import (
    SIGNAL_CATEGORIES,
    SIGNAL_LABELS,
    WEIGHT_PRESETS,
    compute_all_rankings,
    compute_ma_flags,
    compute_signals,
    score_signals,
)

st.title("Technical Analysis")

# ── Account selector ─────────────────────────────────────────────────────────

account_ids = get_account_ids()
if not account_ids:
    st.info("No statements uploaded yet.")
    st.page_link("pages/1_Upload.py", label="Go to Upload", icon="📤")
    st.stop()

with st.sidebar:
    account_options = ["All Accounts"] + account_ids
    selected_account = st.selectbox("Account", account_options)
    account_filter = None if selected_account == "All Accounts" else selected_account

symbols = get_portfolio_symbols(account_id=account_filter)
if not symbols:
    st.info("No stock/ETF positions found.")
    st.page_link("pages/1_Upload.py", label="Upload a statement with holdings", icon="📤")
    st.stop()

# ── Controls ─────────────────────────────────────────────────────────────────

col_preset, col_lookback, col_info = st.columns([1, 1, 2])

with col_preset:
    preset = st.selectbox(
        "Strategy Preset",
        options=list(WEIGHT_PRESETS.keys()),
        index=1,  # default to Balanced
        help=(
            "**Momentum**: Overweights trend & momentum signals. "
            "**Balanced**: Equal category weighting. "
            "**Defensive**: Overweights volatility & mean-reversion signals."
        ),
    )

with col_lookback:
    lookback_days = st.number_input(
        "Lookback (days)",
        min_value=60,
        max_value=500,
        value=365,
        step=30,
        help="How many days of price history to use. 252+ needed for 12-1mo momentum.",
    )

with col_info:
    st.markdown("")  # spacer
    st.caption(f"{len(symbols)} symbols · Preset: **{preset}** · {lookback_days}d lookback")

# ── Weight display ───────────────────────────────────────────────────────────

with st.expander("View preset weights"):
    weights = WEIGHT_PRESETS[preset]
    weight_rows = []
    for key, w in weights.items():
        weight_rows.append({
            "Signal": SIGNAL_LABELS[key],
            "Category": SIGNAL_CATEGORIES[key],
            "Weight": f"{w:.0%}",
        })
    wdf = pd.DataFrame(weight_rows)
    # Group by category for summary
    cat_totals = {}
    for key, w in weights.items():
        cat = SIGNAL_CATEGORIES[key]
        cat_totals[cat] = cat_totals.get(cat, 0) + w
    cat_summary = " · ".join(f"**{cat}**: {pct:.0%}" for cat, pct in sorted(cat_totals.items()))
    st.caption(cat_summary)
    st.dataframe(wdf, use_container_width=True, hide_index=True)

# ── Load price data ──────────────────────────────────────────────────────────

start_date = date.today() - timedelta(days=lookback_days)

price_data: dict[str, pd.DataFrame] = {}
missing_symbols: list[str] = []

for sym in symbols:
    rows = get_daily_prices(sym, date_from=start_date)
    if rows:
        price_data[sym] = pd.DataFrame(rows)
    else:
        missing_symbols.append(sym)

if missing_symbols:
    st.warning(f"No price data for: {', '.join(missing_symbols)}.")
    st.page_link("pages/6_Prices.py", label="Fetch prices", icon="📈")

if not price_data:
    st.info("No price data available. Fetch daily prices first.")
    st.page_link("pages/6_Prices.py", label="Go to Prices", icon="📈")
    st.stop()

# ── Compute rankings ─────────────────────────────────────────────────────────

rankings_df = compute_all_rankings(price_data, preset=preset)

if rankings_df.empty:
    st.warning("Not enough price history to compute signals. Need at least 15 trading days.")
    st.stop()

# ── Composite ranking table ──────────────────────────────────────────────────

st.subheader("Composite Ranking")

# Build display table with scores and MA flags
display_cols = ["Rank", "Symbol", "Composite"]
ma_flag_cols = ["above_sma50", "above_sma100", "above_sma200"]
for col in ma_flag_cols:
    if col in rankings_df.columns:
        display_cols.append(col)
for key in SIGNAL_LABELS:
    display_cols.append(f"{key}_score")

display_df = rankings_df[display_cols].copy()
rename_map = {f"{k}_score": SIGNAL_LABELS[k] for k in SIGNAL_LABELS}
# Convert MA flags to visual indicators
ma_rename = {}
for col in ma_flag_cols:
    if col in display_df.columns:
        period = col.replace("above_sma", "")
        label = f"SMA {period}"
        display_df[col] = display_df[col].map(
            {True: "\u2705", False: "\u274c", None: "\u2014"}
        )
        ma_rename[col] = label
rename_map.update(ma_rename)
display_df = display_df.rename(columns=rename_map)

from src.ui_helpers import color_score

score_columns = ["Composite"] + list(SIGNAL_LABELS.values())
styled = display_df.style.map(color_score, subset=score_columns)

st.dataframe(
    styled,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Rank": st.column_config.NumberColumn("Rank", width="small"),
        "Composite": st.column_config.NumberColumn("Composite", format="%.1f"),
    },
)

st.caption(
    "Scores: 0–100 (higher = more favorable). "
    "RSI and Bollinger %B use contrarian scoring — oversold = opportunity."
)

# ── Per-symbol detail ────────────────────────────────────────────────────────

st.divider()
st.subheader("Signal Detail")

detail_symbol = st.selectbox(
    "Select symbol",
    options=rankings_df["Symbol"].tolist(),
    key="tech_detail",
)

if detail_symbol and detail_symbol in price_data:
    df = price_data[detail_symbol]
    raw = compute_signals(df)
    scores = score_signals(raw)
    ma_flags = compute_ma_flags(df)

    # Moving average flags
    ma_cols = st.columns(3)
    for col, (period, key) in zip(ma_cols, [("50", "above_sma50"), ("100", "above_sma100"), ("200", "above_sma200")]):
        flag = ma_flags.get(key)
        if flag is True:
            col.metric(f"SMA {period}", "\u2705 Above")
        elif flag is False:
            col.metric(f"SMA {period}", "\u274c Below")
        else:
            col.metric(f"SMA {period}", "\u2014 N/A")

    # Signal cards in two rows
    signal_keys = list(SIGNAL_LABELS.keys())
    row1_keys = signal_keys[:5]
    row2_keys = signal_keys[5:]

    cols1 = st.columns(len(row1_keys))
    for col, key in zip(cols1, row1_keys):
        raw_val = raw.get(key)
        score_val = scores.get(key)
        if raw_val is not None:
            # Format raw value
            if key == "rsi_14":
                raw_display = f"{raw_val:.1f}"
            elif key in ("momentum_12_1", "roc_20", "realized_vol_20", "atr_pct", "sma_trend"):
                raw_display = f"{raw_val:.1%}"
            elif key == "volume_trend":
                raw_display = f"{raw_val:.2f}x"
            elif key == "bollinger_pctb":
                raw_display = f"{raw_val:.2f}"
            else:
                raw_display = f"{raw_val:.4f}"
            col.metric(
                SIGNAL_LABELS[key],
                raw_display,
                delta=f"Score: {score_val:.0f}" if score_val is not None else "N/A",
            )
        else:
            col.metric(SIGNAL_LABELS[key], "—", delta="Insufficient data")

    cols2 = st.columns(len(row2_keys))
    for col, key in zip(cols2, row2_keys):
        raw_val = raw.get(key)
        score_val = scores.get(key)
        if raw_val is not None:
            if key == "bollinger_pctb":
                raw_display = f"{raw_val:.2f}"
            elif key in ("macd", "atr_pct"):
                raw_display = f"{raw_val:.1%}" if key == "atr_pct" else f"{raw_val:.4f}"
            elif key == "roc_20":
                raw_display = f"{raw_val:.1%}"
            elif key == "obv_trend":
                raw_display = f"{raw_val:.3f}"
            else:
                raw_display = f"{raw_val:.4f}"
            col.metric(
                SIGNAL_LABELS[key],
                raw_display,
                delta=f"Score: {score_val:.0f}" if score_val is not None else "N/A",
            )
        else:
            col.metric(SIGNAL_LABELS[key], "—", delta="Insufficient data")

    # Price chart
    st.divider()
    chart_df = df.copy()
    chart_df["price_date"] = pd.to_datetime(chart_df["price_date"])
    chart_df["adj_close"] = pd.to_numeric(chart_df["adj_close"], errors="coerce")
    chart_df = chart_df.sort_values("price_date")

    st.line_chart(chart_df, x="price_date", y="adj_close")
