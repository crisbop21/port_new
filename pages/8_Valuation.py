"""Company Valuation & Fundamentals — ratio analysis, historical percentiles,
and composite scoring.  All data from SEC EDGAR metrics + daily prices already
in the database.  No new API calls required.
"""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import math

import pandas as pd
import streamlit as st

from decimal import Decimal

from src.db import (
    clear_query_caches,
    get_account_ids,
    get_daily_prices,
    get_latest_price,
    get_latest_stock_metrics,
    get_latest_valuation_snapshots,
    get_metrics_for_symbols,
    get_portfolio_symbols,
    get_positions,
    get_statements,
    get_stock_metrics,
    get_valuation_snapshots,
    upsert_valuation_snapshots,
)
from src.models import ValuationSnapshot
from src.splits import detect_splits, normalize_metrics
from src.ttm import compute_ttm_latest, is_flow_metric
from src.valuation import (
    SCORE_PRESETS,
    compute_fundamental_score,
    compute_growth,
    compute_historical_ratios,
    compute_peg,
    compute_percentile,
    compute_portfolio_stats,
    compute_ratios,
)

st.title("Company Valuation & Fundamentals")

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

# ── Check for required data ──────────────────────────────────────────────────

metrics_data = get_metrics_for_symbols(symbols)
if not metrics_data:
    st.warning("No fundamental metrics in database. Fetch SEC EDGAR data first.")
    st.page_link("pages/5_Metrics.py", label="Go to Metrics", icon="📊")
    st.stop()

symbols_with_data = sorted(metrics_data.keys())

# ── Controls ─────────────────────────────────────────────────────────────────

col_preset, col_lookback, col_growth = st.columns([1, 1, 1])

with col_preset:
    preset = st.selectbox(
        "Scoring Preset",
        options=list(SCORE_PRESETS.keys()),
        index=0,
        help=(
            "**Balanced**: Equal-ish weighting across categories. "
            "**Value**: Emphasizes cheap valuation. "
            "**Growth**: Emphasizes revenue/EPS growth. "
            "**Quality**: Emphasizes profitability and financial health."
        ),
    )

with col_lookback:
    pct_lookback = st.selectbox(
        "Percentile Lookback",
        options=["3 Years", "5 Years", "All History"],
        index=1,
        help="How much history to use when computing valuation percentiles.",
    )

with col_growth:
    growth_period = st.selectbox(
        "Growth Period",
        options=[1, 3, 5],
        index=0,
        format_func=lambda x: f"{x} Year{'s' if x > 1 else ''} ({'YoY' if x == 1 else 'CAGR'})",
        help="Lookback period for revenue/EPS growth calculation.",
    )

# ── Preset weights display ───────────────────────────────────────────────────

with st.expander("View scoring weights"):
    weights = SCORE_PRESETS[preset]
    weight_df = pd.DataFrame([
        {"Category": cat.title(), "Weight": f"{w:.0%}"}
        for cat, w in weights.items()
    ])
    st.dataframe(weight_df, use_container_width=True, hide_index=True)
    st.caption(
        "Valuation category uses **inverted historical percentile** "
        "(cheaper vs own history = higher score). "
        "Other categories use absolute ratio levels."
    )

# ── Compute all ratios ───────────────────────────────────────────────────────

# Cache for split detection
_splits_cache: dict[str, list] = {}


def _get_splits(sym: str) -> list:
    if sym not in _splits_cache:
        shares = get_stock_metrics(symbol=sym, metric_name="shares_outstanding")
        eps = get_stock_metrics(symbol=sym, metric_name="eps_diluted")
        _splits_cache[sym] = detect_splits(shares, eps)
    return _splits_cache[sym]


def _get_ttm_value(sym: str, metric_name: str, splits: list) -> float | None:
    """Get TTM-adjusted value for a flow metric."""
    history = get_stock_metrics(symbol=sym, metric_name=metric_name)
    if not history:
        return None
    history = normalize_metrics(history, splits, metric_name)
    vk = "normalized_value" if splits else "metric_value"
    try:
        ttm_val, _ = compute_ttm_latest(history, value_key=vk)
    except TypeError:
        ttm_val = None
    return ttm_val


all_ratios: dict[str, dict] = {}
all_growth: dict[str, dict] = {}
all_percentiles: dict[str, dict] = {}
all_scores: dict[str, tuple] = {}
missing_prices: list[str] = []
_valuation_diag: dict[str, dict] = {}  # per-symbol diagnostics

for sym in symbols_with_data:
    # Latest price
    price_row = get_latest_price(sym)
    if not price_row:
        missing_prices.append(sym)
        continue

    try:
        latest_price = float(price_row["adj_close"])
    except (TypeError, ValueError, KeyError):
        missing_prices.append(sym)
        continue

    sym_metrics = metrics_data[sym]
    splits = _get_splits(sym)

    # Build TTM values for flow metrics
    ttm_values: dict[str, float | None] = {}
    for m_name in ("revenue", "net_income", "operating_income", "gross_profit",
                    "eps_basic", "eps_diluted", "capital_expenditures",
                    "dividends_paid", "interest_expense"):
        if m_name in sym_metrics:
            fp = sym_metrics[m_name].get("fiscal_period", "")
            if is_flow_metric(m_name) and fp and fp != "FY":
                ttm_values[m_name] = _get_ttm_value(sym, m_name, splits)
            else:
                try:
                    ttm_values[m_name] = float(sym_metrics[m_name]["metric_value"])
                except (TypeError, ValueError):
                    pass

    # Compute ratios
    ratios = compute_ratios(sym_metrics, latest_price, ttm_metrics=ttm_values)
    all_ratios[sym] = ratios

    # Compute growth
    growth: dict[str, float | None] = {}
    for m_name, g_key in [("revenue", "revenue_growth"), ("eps_diluted", "eps_growth"),
                           ("net_income", "net_income_growth")]:
        history = get_stock_metrics(symbol=sym, metric_name=m_name)
        growth[g_key] = compute_growth(history, m_name, lookback_years=growth_period)
    all_growth[sym] = growth

    # PEG from historical growth
    ratios["peg"] = compute_peg(ratios.get("pe_ttm"), growth.get("eps_growth"))

    # Historical percentiles for valuation ratios
    percentiles: dict[str, float | None] = {}
    # Determine lookback date range for percentiles
    if pct_lookback == "3 Years":
        pct_start = date.today() - timedelta(days=3 * 365)
    elif pct_lookback == "5 Years":
        pct_start = date.today() - timedelta(days=5 * 365)
    else:
        pct_start = None

    # Get price history and metric history for percentile computation
    prices = get_daily_prices(sym, date_from=pct_start)
    _sym_diag: dict = {"prices": len(prices) if prices else 0}
    if prices:
        metric_hist: dict[str, list[dict]] = {}
        for m_name in ("shares_outstanding", "stockholders_equity", "total_assets",
                        "total_liabilities", "cash_and_equivalents", "revenue",
                        "net_income", "operating_income", "eps_diluted", "eps_basic"):
            rows = get_stock_metrics(symbol=sym, metric_name=m_name)
            if rows:
                metric_hist[m_name] = sorted(rows, key=lambda r: str(r.get("period_end", "")))
        _sym_diag["metrics_present"] = list(metric_hist.keys())
        _sym_diag["metrics_counts"] = {k: len(v) for k, v in metric_hist.items()}

        hist_ratios = compute_historical_ratios(metric_hist, prices)
        _sym_diag["hist_ratios"] = len(hist_ratios)

        for ratio_key in ("pe_ttm", "pb", "ps", "ev_ebitda"):
            hist_values = [r[ratio_key] for r in hist_ratios if r.get(ratio_key) is not None]
            _sym_diag[f"hist_{ratio_key}"] = len(hist_values)
            # Use the last historical observation as the canonical "current"
            # value so that ratios, percentiles, scores, and chart all agree.
            if hist_values:
                ratios[ratio_key] = hist_values[-1]
            percentiles[ratio_key] = compute_percentile(ratios.get(ratio_key), hist_values)

    # Track diagnostic if valuation percentiles are all None
    if all(percentiles.get(k) is None for k in ("pe_ttm", "pb", "ps", "ev_ebitda")):
        _valuation_diag[sym] = _sym_diag

    all_percentiles[sym] = percentiles

    # Recompute PEG with the (possibly overridden) pe_ttm
    ratios["peg"] = compute_peg(ratios.get("pe_ttm"), growth.get("eps_growth"))

    # Fundamental score
    score, cat_scores = compute_fundamental_score(ratios, percentiles, growth, preset=preset)
    all_scores[sym] = (score, cat_scores)

if missing_prices:
    st.warning(f"No price data for: {', '.join(missing_prices)}.")
    st.page_link("pages/6_Prices.py", label="Fetch prices", icon="📈")

# Show diagnostic for symbols with missing valuation percentiles
if _valuation_diag:
    with st.expander(f"⚠️ Valuation score missing for {', '.join(_valuation_diag.keys())}", expanded=True):
        for _sym, _diag in _valuation_diag.items():
            st.markdown(f"**{_sym}**:")
            st.json(_diag)

scored_symbols = [s for s in symbols_with_data if s in all_ratios]

if not scored_symbols:
    st.info("No symbols with both metrics and price data. Fetch prices and metrics first.")
    st.stop()


# ── Build snapshot objects for persistence ───────────────────────────────────

def _dec(val: float | None) -> Decimal | None:
    """Convert float to Decimal for the snapshot model, skipping inf/nan."""
    if val is None:
        return None
    if math.isnan(val) or math.isinf(val):
        return None
    return Decimal(str(round(val, 6)))


def _build_snapshots() -> list[ValuationSnapshot]:
    """Build ValuationSnapshot objects from the current computation."""
    snaps = []
    for sym in scored_symbols:
        ratios = all_ratios[sym]
        growth = all_growth.get(sym, {})
        pcts = all_percentiles.get(sym, {})
        score, cat_scores = all_scores.get(sym, (None, {}))
        price_row = get_latest_price(sym)
        if not price_row:
            continue
        try:
            price = Decimal(str(price_row["adj_close"]))
        except (TypeError, ValueError, KeyError):
            continue

        snap = ValuationSnapshot(
            symbol=sym,
            snapshot_date=date.today(),
            preset=preset,
            price_used=price,
            pe_ttm=_dec(ratios.get("pe_ttm")),
            pb=_dec(ratios.get("pb")),
            ps=_dec(ratios.get("ps")),
            ev_ebitda=_dec(ratios.get("ev_ebitda")),
            ev_revenue=_dec(ratios.get("ev_revenue")),
            peg=_dec(ratios.get("peg")),
            earnings_yield=_dec(ratios.get("earnings_yield")),
            fcf_yield=_dec(ratios.get("fcf_yield")),
            market_cap=_dec(ratios.get("market_cap")),
            enterprise_value=_dec(ratios.get("enterprise_value")),
            gross_margin=_dec(ratios.get("gross_margin")),
            operating_margin=_dec(ratios.get("operating_margin")),
            net_margin=_dec(ratios.get("net_margin")),
            roe=_dec(ratios.get("roe")),
            roa=_dec(ratios.get("roa")),
            debt_to_equity=_dec(ratios.get("debt_to_equity")),
            current_ratio=_dec(ratios.get("current_ratio")),
            interest_coverage=_dec(ratios.get("interest_coverage")),
            cash_to_assets=_dec(ratios.get("cash_to_assets")),
            dividend_yield=_dec(ratios.get("dividend_yield")),
            payout_ratio=_dec(ratios.get("payout_ratio")),
            revenue_growth=_dec(growth.get("revenue_growth")),
            eps_growth=_dec(growth.get("eps_growth")),
            net_income_growth=_dec(growth.get("net_income_growth")),
            pe_percentile=_dec(pcts.get("pe_ttm")),
            pb_percentile=_dec(pcts.get("pb")),
            ps_percentile=_dec(pcts.get("ps")),
            ev_ebitda_percentile=_dec(pcts.get("ev_ebitda")),
            score_composite=_dec(score),
            score_valuation=_dec(cat_scores.get("valuation")),
            score_profitability=_dec(cat_scores.get("profitability")),
            score_health=_dec(cat_scores.get("health")),
            score_growth=_dec(cat_scores.get("growth")),
        )
        snaps.append(snap)
    return snaps


# ── Save snapshot controls ───────────────────────────────────────────────────

col_save, col_save_info = st.columns([1, 3])

with col_save:
    save_clicked = st.button("Save Snapshot", type="primary",
                              help="Persist today's valuation ratios and scores to the database for historical tracking.")

with col_save_info:
    # Check when last snapshot was saved
    cached = get_latest_valuation_snapshots(scored_symbols[:1], preset=preset)
    if cached:
        last_row = next(iter(cached.values()), {})
        last_date = last_row.get("snapshot_date", "never")
        st.caption(f"Last saved: **{last_date}** · Saves {len(scored_symbols)} symbols for preset **{preset}**")
    else:
        st.caption(f"No snapshots saved yet · Will save {len(scored_symbols)} symbols for preset **{preset}**")

if save_clicked:
    snapshots = _build_snapshots()
    if snapshots:
        with st.spinner(f"Saving {len(snapshots)} snapshots..."):
            inserted, updated, errors = upsert_valuation_snapshots(snapshots)
            clear_query_caches()
        if errors:
            for err in errors:
                st.error(err)
        else:
            st.success(f"Saved: {inserted} new, {updated} updated snapshots.")
    else:
        st.warning("No snapshots to save.")

# ── Tabs ────────────────────────────────────────────────────────────────────

val_tab_comp, val_tab_scores, val_tab_pct, val_tab_deep, val_tab_port = st.tabs(
    ["Comparison", "Scores", "Percentiles", "Deep Dive", "Portfolio"]
)

# ── Tab: Comparison Table ───────────────────────────────────────────────────

RATIO_DISPLAY = [
    # (key, label, format, higher_is_better)
    ("pe_ttm", "P/E", "{:.1f}", False),
    ("pb", "P/B", "{:.2f}", False),
    ("ps", "P/S", "{:.2f}", False),
    ("ev_ebitda", "EV/EBITDA", "{:.1f}", False),
    ("peg", "PEG", "{:.2f}", False),
    ("earnings_yield", "Earn Yield", "{:.1%}", True),
    ("fcf_yield", "FCF Yield", "{:.1%}", True),
    ("gross_margin", "Gross Mgn", "{:.1%}", True),
    ("operating_margin", "Op Mgn", "{:.1%}", True),
    ("net_margin", "Net Mgn", "{:.1%}", True),
    ("roe", "ROE", "{:.1%}", True),
    ("roa", "ROA", "{:.1%}", True),
    ("debt_to_equity", "D/E", "{:.2f}", False),
    ("current_ratio", "Curr Ratio", "{:.2f}", True),
    ("interest_coverage", "Int Cov", "{:.1f}", True),
    ("dividend_yield", "Div Yield", "{:.1%}", True),
]

comp_rows = []
for sym in scored_symbols:
    ratios = all_ratios[sym]
    growth = all_growth.get(sym, {})
    row: dict = {"Symbol": sym}

    for key, label, fmt, _ in RATIO_DISPLAY:
        val = ratios.get(key)
        if val is not None and not math.isnan(val) and not math.isinf(val):
            try:
                row[label] = fmt.format(val)
            except (ValueError, TypeError):
                row[label] = "—"
        else:
            row[label] = "—"

    # Growth columns
    for g_key, g_label in [("revenue_growth", "Rev Grw"), ("eps_growth", "EPS Grw"),
                            ("net_income_growth", "NI Grw")]:
        val = growth.get(g_key)
        if val is not None:
            row[g_label] = f"{val:.1%}"
        else:
            row[g_label] = "—"

    comp_rows.append(row)

comp_df = pd.DataFrame(comp_rows)

with val_tab_comp:
    st.subheader("Comparison Table")
    st.dataframe(comp_df, use_container_width=True, hide_index=True)
    growth_label = f"{'YoY' if growth_period == 1 else f'{growth_period}yr CAGR'}"
    st.caption(f"Growth period: {growth_label} · Valuation ratios use TTM earnings where available")

# ── Fundamental Score Ranking ────────────────────────────────────────────────


from src.ui_helpers import color_score

score_rows = []
for sym in scored_symbols:
    score, cat_scores = all_scores.get(sym, (None, {}))
    row = {
        "Symbol": sym,
        "Composite": round(score, 1) if score is not None else None,
        "Valuation": round(cat_scores.get("valuation", 0), 1) if cat_scores.get("valuation") is not None else None,
        "Profitability": round(cat_scores.get("profitability", 0), 1) if cat_scores.get("profitability") is not None else None,
        "Health": round(cat_scores.get("health", 0), 1) if cat_scores.get("health") is not None else None,
        "Growth": round(cat_scores.get("growth", 0), 1) if cat_scores.get("growth") is not None else None,
    }
    score_rows.append(row)

score_df = pd.DataFrame(score_rows)
score_df = score_df.sort_values("Composite", ascending=False, na_position="last")
score_df.insert(0, "Rank", range(1, len(score_df) + 1))

score_cols = ["Composite", "Valuation", "Profitability", "Health", "Growth"]
styled = score_df.style.map(color_score, subset=score_cols)

with val_tab_scores:
    st.subheader("Fundamental Score")
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
        f"Preset: **{preset}** · "
        f"Valuation scored by inverted percentile vs {pct_lookback.lower()} history "
        f"(cheaper = higher score) · "
        f"Scores: 0-100 (higher = more favorable)"
    )

# ── Valuation Percentiles ────────────────────────────────────────────────────

pct_rows = []
for sym in scored_symbols:
    pcts = all_percentiles.get(sym, {})
    ratios = all_ratios[sym]
    row = {"Symbol": sym}
    for ratio_key, label in [("pe_ttm", "P/E"), ("pb", "P/B"),
                              ("ps", "P/S"), ("ev_ebitda", "EV/EBITDA")]:
        val = ratios.get(ratio_key)
        pct = pcts.get(ratio_key)
        if val is not None and not math.isnan(val) and not math.isinf(val):
            row[f"{label}"] = f"{val:.1f}"
        else:
            row[f"{label}"] = "—"
        if pct is not None:
            row[f"{label} %ile"] = f"{pct:.0f}%"
        else:
            row[f"{label} %ile"] = "—"
    pct_rows.append(row)

with val_tab_pct:
    st.subheader("Valuation Percentiles")
    st.caption(
        "Where each stock's current valuation sits within its own history. "
        "Low percentile = cheap relative to its past. High = expensive."
    )
    if pct_rows:
        pct_df = pd.DataFrame(pct_rows)
        st.dataframe(pct_df, use_container_width=True, hide_index=True)

# ── Single-Symbol Deep Dive ──────────────────────────────────────────────────

with val_tab_deep:
    st.subheader("Symbol Deep Dive")

    detail_symbol = st.selectbox(
        "Select symbol",
        options=scored_symbols,
        key="val_detail",
    )

    if detail_symbol and detail_symbol in all_ratios:
        ratios = all_ratios[detail_symbol]
        growth = all_growth.get(detail_symbol, {})
        percentiles = all_percentiles.get(detail_symbol, {})
        score, cat_scores = all_scores.get(detail_symbol, (None, {}))

        if pct_lookback == "3 Years":
            _dd_hist_start = date.today() - timedelta(days=3 * 365)
        elif pct_lookback == "5 Years":
            _dd_hist_start = date.today() - timedelta(days=5 * 365)
        else:
            _dd_hist_start = None

        _dd_prices = get_daily_prices(detail_symbol, date_from=_dd_hist_start)
        _dd_metric_hist: dict[str, list[dict]] = {}
        if _dd_prices:
            for _m_name in ("shares_outstanding", "stockholders_equity", "total_assets",
                            "total_liabilities", "cash_and_equivalents", "revenue",
                            "net_income", "operating_income", "eps_diluted", "eps_basic"):
                _m_rows = get_stock_metrics(symbol=detail_symbol, metric_name=_m_name)
                if _m_rows:
                    _dd_metric_hist[_m_name] = sorted(_m_rows, key=lambda r: str(r.get("period_end", "")))

        _dd_hist_data = compute_historical_ratios(_dd_metric_hist, _dd_prices) if _dd_prices else []

        # Score overview
        cols = st.columns(5)
        score_items = [
            ("Composite", score),
            ("Valuation", cat_scores.get("valuation")),
            ("Profitability", cat_scores.get("profitability")),
            ("Health", cat_scores.get("health")),
            ("Growth", cat_scores.get("growth")),
        ]
        for col, (label, val) in zip(cols, score_items):
            col.metric(label, f"{val:.1f}" if val is not None else "—")

        # Key ratios in cards
        st.markdown("**Valuation**")
        val_cols = st.columns(5)
        val_items = [
            ("P/E", ratios.get("pe_ttm"), "{:.1f}", percentiles.get("pe_ttm")),
            ("P/B", ratios.get("pb"), "{:.2f}", percentiles.get("pb")),
            ("P/S", ratios.get("ps"), "{:.2f}", percentiles.get("ps")),
            ("EV/EBITDA", ratios.get("ev_ebitda"), "{:.1f}", percentiles.get("ev_ebitda")),
            ("PEG", ratios.get("peg"), "{:.2f}", None),
        ]
        for col, (label, val, fmt, pct) in zip(val_cols, val_items):
            if val is not None and not math.isnan(val) and not math.isinf(val):
                display = fmt.format(val)
                delta = f"{pct:.0f}th %ile" if pct is not None else None
            else:
                display = "—"
                delta = None
            col.metric(label, display, delta=delta, delta_color="off")

        st.markdown("**Profitability**")
        prof_cols = st.columns(5)
        prof_items = [
            ("Gross Margin", ratios.get("gross_margin"), "{:.1%}"),
            ("Op Margin", ratios.get("operating_margin"), "{:.1%}"),
            ("Net Margin", ratios.get("net_margin"), "{:.1%}"),
            ("ROE", ratios.get("roe"), "{:.1%}"),
            ("ROA", ratios.get("roa"), "{:.1%}"),
        ]
        for col, (label, val, fmt) in zip(prof_cols, prof_items):
            if val is not None and not math.isnan(val) and not math.isinf(val):
                col.metric(label, fmt.format(val))
            else:
                col.metric(label, "—")

        st.markdown("**Financial Health**")
        health_cols = st.columns(5)
        health_items = [
            ("D/E", ratios.get("debt_to_equity"), "{:.2f}"),
            ("Current Ratio", ratios.get("current_ratio"), "{:.2f}"),
            ("Interest Cov", ratios.get("interest_coverage"), "{:.1f}"),
            ("Cash/Assets", ratios.get("cash_to_assets"), "{:.1%}"),
            ("Div Yield", ratios.get("dividend_yield"), "{:.2%}"),
        ]
        for col, (label, val, fmt) in zip(health_cols, health_items):
            if val is not None and not math.isnan(val) and not math.isinf(val):
                col.metric(label, fmt.format(val))
            else:
                col.metric(label, "—")

        st.markdown("**Growth**")
        growth_cols = st.columns(3)
        growth_items = [
            ("Revenue Growth", growth.get("revenue_growth"), "{:.1%}"),
            ("EPS Growth", growth.get("eps_growth"), "{:.1%}"),
            ("Net Income Growth", growth.get("net_income_growth"), "{:.1%}"),
        ]
        for col, (label, val, fmt) in zip(growth_cols, growth_items):
            if val is not None:
                col.metric(label, fmt.format(val))
            else:
                col.metric(label, "—")

        # Historical valuation chart
        st.divider()
        st.markdown("**Historical Valuation Ratios**")

        hist_ratio_choice = st.selectbox(
            "Ratio to chart",
            options=["pe_ttm", "pb", "ps", "ev_ebitda"],
            format_func=lambda x: {"pe_ttm": "P/E", "pb": "P/B", "ps": "P/S", "ev_ebitda": "EV/EBITDA"}[x],
            key="hist_ratio",
        )

        if _dd_hist_data:
            chart_df = pd.DataFrame(_dd_hist_data)
            chart_df["period_end"] = pd.to_datetime(chart_df["period_end"])
            chart_df = chart_df.dropna(subset=[hist_ratio_choice])
            chart_df = chart_df.sort_values("period_end")

            if not chart_df.empty:
                st.line_chart(chart_df, x="period_end", y=hist_ratio_choice)

                values = chart_df[hist_ratio_choice].tolist()
                current = values[-1] if values else None
                if current is not None and len(values) >= 4:
                    mn, mx, avg = min(values), max(values), sum(values) / len(values)
                    pct = compute_percentile(current, values)
                    range_cols = st.columns(4)
                    range_cols[0].metric("Current", f"{current:.2f}")
                    range_cols[1].metric("Avg", f"{avg:.2f}")
                    range_cols[2].metric("Range", f"{mn:.1f} — {mx:.1f}")
                    range_cols[3].metric("Percentile", f"{pct:.0f}%" if pct is not None else "—")

                with st.expander("Percentile diagnostics"):
                    st.markdown(
                        f"**{hist_ratio_choice}** — {len(values)} ratio data points "
                        f"from {len(_dd_prices)} daily prices"
                    )
                    diag_df = chart_df[["period_end", "price", hist_ratio_choice]].copy()
                    diag_df = diag_df.rename(columns={hist_ratio_choice: "ratio_value"})
                    st.dataframe(diag_df, use_container_width=True, hide_index=True)
                    if current is not None:
                        count_below = sum(1 for v in values if v < current)
                        st.markdown(
                            f"Current value: **{current:.2f}** · "
                            f"Values below current: **{count_below}/{len(values)}** · "
                            f"Percentile: **{(count_below / len(values)) * 100:.1f}%**"
                        )
            else:
                st.info("Not enough historical data points to chart this ratio.")
        else:
            st.info("No price history available. Fetch prices first.")

        # Score History
        st.divider()
        st.markdown("**Score History**")

        hist_snapshots = get_valuation_snapshots(detail_symbol, preset=preset)
        if hist_snapshots and len(hist_snapshots) >= 2:
            snap_df = pd.DataFrame(hist_snapshots)
            snap_df["snapshot_date"] = pd.to_datetime(snap_df["snapshot_date"])
            for col_name in ("score_composite", "score_valuation", "score_profitability",
                             "score_health", "score_growth", "price_used"):
                snap_df[col_name] = pd.to_numeric(snap_df[col_name], errors="coerce")
            snap_df = snap_df.sort_values("snapshot_date")

            score_chart_cols = ["score_composite", "score_valuation", "score_profitability",
                                "score_health", "score_growth"]
            chart_data = snap_df[["snapshot_date"] + score_chart_cols].set_index("snapshot_date")
            chart_data.columns = ["Composite", "Valuation", "Profitability", "Health", "Growth"]
            st.line_chart(chart_data)
            st.caption(f"{len(hist_snapshots)} snapshots saved for {detail_symbol} ({preset} preset)")
        elif hist_snapshots and len(hist_snapshots) == 1:
            st.info("Only 1 snapshot saved. Save snapshots on multiple days to see score trends.")
        else:
            st.info("No saved snapshots yet. Click **Save Snapshot** to start tracking score history.")

# ── Tab: Portfolio Stats ──────────────────────────────────────────────────

with val_tab_port:
    st.subheader("Portfolio-Level Stats")

    statements = get_statements()
    if statements:
        if account_filter:
            statements = [s for s in statements if s["account_id"] == account_filter]

        latest_stmt = statements[0] if statements else None
        positions = get_positions(latest_stmt["id"]) if latest_stmt else []

        port_stats = compute_portfolio_stats(positions, all_ratios)

        if port_stats:
            st.markdown("**Weighted Averages** (by market value)")
            wa_cols = st.columns(5)
            wa_items = [
                ("Wtd P/E", port_stats.get("weighted_pe_ttm"), "{:.1f}"),
                ("Wtd P/B", port_stats.get("weighted_pb"), "{:.2f}"),
                ("Wtd P/S", port_stats.get("weighted_ps"), "{:.2f}"),
                ("Wtd Earn Yield", port_stats.get("weighted_earnings_yield"), "{:.2%}"),
                ("Wtd Div Yield", port_stats.get("weighted_dividend_yield"), "{:.2%}"),
            ]
            for col, (label, val, fmt) in zip(wa_cols, wa_items):
                if val is not None and not math.isnan(val) and not math.isinf(val):
                    col.metric(label, fmt.format(val))
                else:
                    col.metric(label, "—")

            st.markdown("**Concentration**")
            conc_cols = st.columns(3)
            hhi = port_stats.get("herfindahl")
            top_pct = port_stats.get("top_holding_pct")
            n_hold = port_stats.get("num_holdings")

            conc_cols[0].metric(
                "Holdings",
                f"{n_hold:.0f}" if n_hold else "—",
            )
            conc_cols[1].metric(
                "Top Holding",
                f"{top_pct:.1f}%" if top_pct is not None else "—",
            )
            if hhi is not None:
                if hhi < 1500:
                    hhi_label = "Diversified"
                elif hhi < 2500:
                    hhi_label = "Moderate"
                else:
                    hhi_label = "Concentrated"
                conc_cols[2].metric("HHI", f"{hhi:.0f}", delta=hhi_label, delta_color="off")
            else:
                conc_cols[2].metric("HHI", "—")

            ey_cost = port_stats.get("earnings_yield_on_cost")
            if ey_cost is not None:
                st.metric("Earnings Yield on Cost", f"{ey_cost:.2%}")
        else:
            st.info("Could not compute portfolio stats — no matching positions with ratio data.")
    else:
        st.info("No statements found.")
