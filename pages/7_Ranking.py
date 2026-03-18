"""Asset Ranking Monitor — score portfolio holdings 1–10 on technicals + fundamentals."""

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
    get_latest_stock_metrics,
    get_portfolio_symbols,
    get_stock_metrics,
)
from src.ranking import (
    AssetRanking,
    compute_overall,
    rank_assets,
    score_fundamentals,
    score_technicals,
)

st.title("Asset Ranking Monitor")

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


# ── Build rankings ───────────────────────────────────────────────────────────


def _get_closes(symbol: str, days: int = 300) -> list[float]:
    """Fetch daily closes from DB, oldest first."""
    start = date.today() - timedelta(days=days)
    rows = get_daily_prices(symbol, date_from=start)
    closes = []
    for r in rows:
        try:
            closes.append(float(r["adj_close"]))
        except (ValueError, TypeError, KeyError):
            continue
    return closes


def _get_prior_revenue(symbol: str) -> float | None:
    """Get revenue from ~1 year ago for YoY growth calculation."""
    rows = get_stock_metrics(symbol=symbol, metric_name="revenue")
    if not rows:
        return None
    # rows are ordered by period_end desc — find annual (FY) entries
    fy_rows = [r for r in rows if r.get("fiscal_period") == "FY"]
    if len(fy_rows) >= 2:
        try:
            return float(fy_rows[1]["metric_value"])
        except (ValueError, TypeError, KeyError):
            return None
    # Fallback: any row that's at least 9 months older than the latest
    if len(rows) >= 2:
        try:
            return float(rows[1]["metric_value"])
        except (ValueError, TypeError, KeyError):
            return None
    return None


st.caption(f"Scoring {len(symbols)} stock/ETF holdings on technical + fundamental factors")

rankings: list[AssetRanking] = []
missing_prices: list[str] = []
missing_fundamentals: list[str] = []

progress = st.progress(0, text="Computing rankings...")

for i, sym in enumerate(symbols):
    progress.progress((i + 1) / len(symbols), text=f"Scoring {sym}...")

    ar = AssetRanking(symbol=sym)

    # Technical scoring
    closes = _get_closes(sym)
    if closes:
        ar.technical = score_technicals(closes)
    else:
        missing_prices.append(sym)

    # Fundamental scoring
    latest_metrics = get_latest_stock_metrics(sym)
    latest_price_row = get_latest_price(sym)
    current_price = None
    if latest_price_row:
        try:
            current_price = float(latest_price_row["adj_close"])
        except (ValueError, TypeError, KeyError):
            pass

    prior_rev = _get_prior_revenue(sym)

    if latest_metrics:
        ar.fundamental = score_fundamentals(latest_metrics, current_price, prior_rev)
    else:
        missing_fundamentals.append(sym)

    ar.overall_score = compute_overall(ar.technical, ar.fundamental)
    rankings.append(ar)

progress.empty()

# Assign ranks
rankings = rank_assets(rankings)

# ── Warnings ─────────────────────────────────────────────────────────────────

if missing_prices:
    st.warning(
        f"No price data for: {', '.join(missing_prices)}. "
        "Fetch prices on the **Prices** page first."
    )
if missing_fundamentals:
    st.info(
        f"No fundamental data for: {', '.join(missing_fundamentals)}. "
        "ETFs typically lack SEC filings. Fetch on the **Metrics** page."
    )

# ── Summary table ────────────────────────────────────────────────────────────

st.subheader("Rankings")

rows = []
for r in rankings:
    row = {
        "Rank": r.overall_rank if r.overall_rank else "—",
        "Symbol": r.symbol,
        "Overall": r.overall_rounded if r.overall_rounded else "—",
        "Technical": r.technical.composite if r.technical.composite else "—",
        "Fundamental": r.fundamental.composite if r.fundamental.composite else "—",
        "RSI": f"{r.technical.rsi:.0f}" if r.technical.rsi is not None else "—",
        "vs SMA50": f"{r.technical.sma50_pct:+.1f}%" if r.technical.sma50_pct is not None else "—",
        "vs SMA200": f"{r.technical.sma200_pct:+.1f}%" if r.technical.sma200_pct is not None else "—",
        "30d Chg": f"{r.technical.momentum_30d:+.1f}%" if r.technical.momentum_30d is not None else "—",
        "Vol (ann)": f"{r.technical.volatility:.0f}%" if r.technical.volatility is not None else "—",
        "P/E": f"{r.fundamental.pe_ratio:.1f}" if r.fundamental.pe_ratio is not None else "—",
        "Margin": f"{r.fundamental.profit_margin:.1f}%" if r.fundamental.profit_margin is not None else "—",
        "Rev Grw": f"{r.fundamental.revenue_growth:+.1f}%" if r.fundamental.revenue_growth is not None else "—",
        "D/E": f"{r.fundamental.debt_to_equity:.2f}" if r.fundamental.debt_to_equity is not None else "—",
        "Op Margin": f"{r.fundamental.operating_margin:.1f}%" if r.fundamental.operating_margin is not None else "—",
    }
    rows.append(row)

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Score distribution chart ─────────────────────────────────────────
    scored = [r for r in rankings if r.overall_score is not None]
    if scored:
        chart_df = pd.DataFrame({
            "Symbol": [r.symbol for r in scored],
            "Technical": [r.technical.composite or 0 for r in scored],
            "Fundamental": [r.fundamental.composite or 0 for r in scored],
        }).set_index("Symbol")

        st.subheader("Score Comparison")
        st.bar_chart(chart_df)

# ── Detail view ──────────────────────────────────────────────────────────────

st.divider()
st.subheader("Score Breakdown")

detail_sym = st.selectbox("Select symbol", [r.symbol for r in rankings], key="rank_detail")

if detail_sym:
    ar = next((r for r in rankings if r.symbol == detail_sym), None)
    if ar is None:
        st.stop()

    col1, col2, col3 = st.columns(3)
    col1.metric("Overall Score", f"{ar.overall_rounded}/10" if ar.overall_rounded else "N/A")
    col2.metric("Technical", f"{ar.technical.composite}/10" if ar.technical.composite else "N/A")
    col3.metric("Fundamental", f"{ar.fundamental.composite}/10" if ar.fundamental.composite else "N/A")

    # Technical breakdown
    st.markdown("**Technical Indicators**")
    tech_rows = []
    indicators = [
        ("RSI (14)", ar.technical.rsi, ar.technical.rsi_score, "50 is neutral; <30 oversold, >70 overbought"),
        ("Price vs SMA-50", ar.technical.sma50_pct, ar.technical.sma50_score, "% above/below 50-day moving avg"),
        ("Price vs SMA-200", ar.technical.sma200_pct, ar.technical.sma200_score, "% above/below 200-day moving avg"),
        ("30-day Momentum", ar.technical.momentum_30d, ar.technical.momentum_score, "Price change over 30 trading days"),
        ("Volatility (ann.)", ar.technical.volatility, ar.technical.volatility_score, "Lower is better for stability"),
    ]
    for name, val, score, desc in indicators:
        tech_rows.append({
            "Indicator": name,
            "Value": f"{val:.1f}" if val is not None else "—",
            "Score (1-10)": f"{score:.1f}" if score is not None else "—",
            "Description": desc,
        })
    st.dataframe(pd.DataFrame(tech_rows), use_container_width=True, hide_index=True)

    # Fundamental breakdown
    st.markdown("**Fundamental Indicators**")
    fund_rows = []
    indicators_f = [
        ("P/E Ratio", ar.fundamental.pe_ratio, ar.fundamental.pe_score, "Lower is better (value)"),
        ("Profit Margin", ar.fundamental.profit_margin, ar.fundamental.margin_score, "Net income / revenue"),
        ("Revenue Growth", ar.fundamental.revenue_growth, ar.fundamental.growth_score, "Year-over-year"),
        ("Debt/Equity", ar.fundamental.debt_to_equity, ar.fundamental.de_score, "Lower is better"),
        ("Operating Margin", ar.fundamental.operating_margin, ar.fundamental.op_margin_score, "Operating income / revenue"),
    ]
    for name, val, score, desc in indicators_f:
        fund_rows.append({
            "Indicator": name,
            "Value": f"{val:.2f}" if val is not None else "—",
            "Score (1-10)": f"{score:.1f}" if score is not None else "—",
            "Description": desc,
        })
    st.dataframe(pd.DataFrame(fund_rows), use_container_width=True, hide_index=True)

    # Weight explanation
    with st.expander("Scoring methodology"):
        st.markdown("""
**Composite Score = 40% Technical + 60% Fundamental**

*Technical sub-scores (equal weight except volatility at half):*
- **RSI**: Peaks at 50 (neutral momentum), penalises extremes
- **SMA-50/200**: Rewards price above moving averages (uptrend)
- **30d Momentum**: Rewards positive recent performance
- **Volatility**: Rewards lower annualised volatility (stability)

*Fundamental sub-scores:*
- **P/E Ratio** (weight 2.5): Lower P/E = better value
- **Profit Margin** (weight 2.0): Higher net margin = better
- **Revenue Growth** (weight 2.0): Higher YoY growth = better
- **Debt/Equity** (weight 1.5): Lower leverage = better
- **Operating Margin** (weight 2.0): Higher op margin = better

If only one side (technical or fundamental) has data, the composite
uses that side alone. Symbols without any data get no score.
        """)
else:
    st.info("Select a symbol above to see the full breakdown.")
