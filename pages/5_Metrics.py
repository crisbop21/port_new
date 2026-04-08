"""Stock Metrics — fundamental data from SEC EDGAR for portfolio holdings."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

from src.db import (
    clear_query_caches,
    delete_stock_metrics,
    get_account_ids,
    get_latest_stock_metrics,
    get_metrics_for_symbols,
    get_portfolio_symbols,
    get_stock_metrics,
    upsert_stock_metrics,
)
from src.fetcher import fetch_metrics_for_symbol
from src.splits import (
    SPLIT_AFFECTED_PER_SHARE,
    SPLIT_AFFECTED_SHARE_COUNT,
    detect_splits,
    normalize_latest_value,
    normalize_metrics,
)
from src.ttm import compute_quarterly_latest, compute_ttm, compute_ttm_latest, is_flow_metric
from src.ui_helpers import COLORS, inject_metric_card_css

st.title("Stock Metrics")
inject_metric_card_css()

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

# ── Portfolio symbols ────────────────────────────────────────────────────────

symbols = get_portfolio_symbols(account_id=account_filter)
if not symbols:
    st.info("No stock or ETF positions found.")
    st.page_link("pages/1_Upload.py", label="Upload a statement with stock holdings", icon="📤")
    st.stop()

st.caption(f"{len(symbols)} stock/ETF symbols in portfolio")

# ── Fetch controls (collapsed when data exists) ─────────────────────────────

metrics_data = get_metrics_for_symbols(symbols)
has_data = bool(metrics_data)

with st.expander("Fetch from SEC EDGAR", expanded=not has_data):
    fetch_symbols = st.multiselect(
        "Symbols to fetch",
        options=symbols,
        default=symbols,
        help="Select which symbols to fetch fundamental data for. ETFs typically have no SEC filings.",
    )

    overwrite = st.checkbox(
        "Overwrite existing data",
        value=False,
        help=(
            "Delete existing metrics for selected symbols before fetching. "
            "Use this to re-fetch with updated reporting-style detection "
            "(cumulative YTD vs standalone quarterly)."
        ),
    )

    col_fetch, col_status = st.columns([1, 3])

    with col_fetch:
        fetch_clicked = st.button("Fetch Metrics", type="primary", disabled=not fetch_symbols)

    if fetch_clicked:
        all_metrics = []
        all_errors = []

        if overwrite and fetch_symbols:
            with st.spinner("Deleting existing metrics..."):
                deleted, del_errors = delete_stock_metrics(fetch_symbols)
                all_errors.extend(del_errors)
                if deleted:
                    st.info(f"Deleted {deleted} existing metric rows for clean re-fetch.")
                clear_query_caches()

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

        if all_errors:
            with st.expander(f"Issues ({len(all_errors)})", expanded=len(all_metrics) == 0):
                for err in all_errors:
                    st.text(err)

# ── Guard: need data to continue ────────────────────────────────────────────

if not metrics_data:
    st.info(
        "No metrics in database yet. Use the **Fetch Metrics** section above "
        "to pull fundamental data from SEC EDGAR."
    )
    st.stop()

# Reload after potential fetch
metrics_data = get_metrics_for_symbols(symbols)
if not metrics_data:
    st.stop()

# ── Pre-compute split detection per symbol ──────────────────────────────────

_splits_cache: dict[str, list] = {}


def _get_splits_for_symbol(sym: str) -> list:
    """Detect splits for a symbol, cached for the page render."""
    if sym not in _splits_cache:
        shares = get_stock_metrics(symbol=sym, metric_name="shares_outstanding")
        eps = get_stock_metrics(symbol=sym, metric_name="eps_diluted")
        _splits_cache[sym] = detect_splits(shares, eps)
    return _splits_cache[sym]


# ── Build summary data ──────────────────────────────────────────────────────

DISPLAY_METRICS = [
    ("revenue", "Revenue", "$%.0f"),
    ("net_income", "Net Income", "$%.0f"),
    ("eps_diluted", "EPS (Diluted)", "$%.2f"),
    ("total_assets", "Total Assets", "$%.0f"),
    ("total_liabilities", "Total Liabilities", "$%.0f"),
    ("stockholders_equity", "Equity", "$%.0f"),
    ("shares_outstanding", "Shares Out", "%.0f"),
    ("operating_income", "Operating Income", "$%.0f"),
    ("cash_and_equivalents", "Cash", "$%.0f"),
]

rows = []
split_notes: list[str] = []
ttm_notes: list[str] = []
for sym in sorted(metrics_data.keys()):
    sym_metrics = metrics_data[sym]
    sym_splits = _get_splits_for_symbol(sym)
    row: dict = {"Symbol": sym}

    for metric_key, display_name, fmt in DISPLAY_METRICS:
        if metric_key in sym_metrics:
            raw_val = sym_metrics[metric_key].get("metric_value")
            period = sym_metrics[metric_key].get("period_end", "")
            filing = sym_metrics[metric_key].get("filing_type", "")
            fp = sym_metrics[metric_key].get("fiscal_period", "")
            if raw_val is not None:
                try:
                    val = float(raw_val)

                    if is_flow_metric(metric_key) and fp:
                        history = get_stock_metrics(symbol=sym, metric_name=metric_key)
                        history = normalize_metrics(history, sym_splits, metric_key)
                        vk = "normalized_value" if sym_splits else "metric_value"

                        try:
                            q_val, q_label, q_method = compute_quarterly_latest(history, value_key=vk)
                        except TypeError:
                            q_val, q_label, q_method = None, None, None

                        try:
                            ttm_val, ttm_method = compute_ttm_latest(history, value_key=vk)
                        except TypeError:
                            ttm_val, ttm_method = None, None

                        if q_val is not None:
                            val = q_val
                            if sym not in [n.split(":")[0] for n in ttm_notes]:
                                ttm_notes.append(f"{sym}: {q_label} quarterly from {fp} data")
                        elif ttm_val is not None:
                            val = ttm_val
                            if sym not in [n.split(":")[0] for n in ttm_notes]:
                                ttm_notes.append(f"{sym}: TTM (annual) from {fp} data")

                        if sym_splits and sym not in [n.split(":")[0] for n in split_notes]:
                            split_notes.append(f"{sym}: {len(sym_splits)} split(s) detected — per-share metrics adjusted")
                    elif sym_splits and metric_key in (SPLIT_AFFECTED_PER_SHARE | SPLIT_AFFECTED_SHARE_COUNT):
                        val, was_adjusted = normalize_latest_value(
                            metric_key, val, str(period), sym_splits,
                        )
                        if was_adjusted:
                            if sym not in [n.split(":")[0] for n in split_notes]:
                                split_notes.append(f"{sym}: {len(sym_splits)} split(s) detected — per-share metrics adjusted")

                    row[display_name] = val
                except (ValueError, TypeError):
                    row[display_name] = None
            else:
                row[display_name] = None
        else:
            row[display_name] = None

    any_metric = next(iter(sym_metrics.values()), {})
    row["Period End"] = any_metric.get("period_end", "—")
    row["Filing"] = any_metric.get("filing_type", "—")
    row["FP"] = any_metric.get("fiscal_period", "—")
    rows.append(row)


# ── Main content: three tabs ─────────────────────────────────────────────────

tab_overview, tab_heatmap, tab_detail = st.tabs(
    ["Portfolio Fundamentals", "Comparison Heatmap", "Symbol Detail"]
)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Portfolio Fundamentals
# ══════════════════════════════════════════════════════════════════════════════

with tab_overview:
    if rows:
        summary_df = pd.DataFrame(rows)

        # ── Top KPI cards: aggregated portfolio-level numbers ────────────
        total_revenue = summary_df["Revenue"].sum()
        total_net_income = summary_df["Net Income"].sum()
        total_equity = summary_df["Equity"].sum()
        total_cash = summary_df["Cash"].sum()
        symbols_count = len(summary_df)

        c1, c2, c3, c4, c5 = st.columns(5, gap="medium")
        c1.metric("Symbols", symbols_count)
        c2.metric("Total Revenue", f"${total_revenue:,.0f}")
        c3.metric(
            "Total Net Income",
            f"${total_net_income:,.0f}",
            delta=f"{'Positive' if total_net_income >= 0 else 'Negative'}",
            delta_color="normal" if total_net_income >= 0 else "inverse",
        )
        c4.metric("Total Equity", f"${total_equity:,.0f}")
        c5.metric("Total Cash", f"${total_cash:,.0f}")

        st.markdown("---")

        # ── Color-coded summary table ────────────────────────────────────

        def _color_pnl(val):
            """Color positive values green, negative red."""
            if pd.isna(val):
                return ""
            try:
                v = float(val)
            except (ValueError, TypeError):
                return ""
            if v > 0:
                return "color: #22c55e; font-weight: 600"
            elif v < 0:
                return "color: #ef4444; font-weight: 600"
            return ""

        colored_cols = ["Net Income", "EPS (Diluted)", "Operating Income"]
        metrics_col_config = {
            display_name: st.column_config.NumberColumn(display_name, format=fmt)
            for _, display_name, fmt in DISPLAY_METRICS
        }

        _style_subset = [c for c in colored_cols if c in summary_df.columns]
        try:
            styled = summary_df.style.map(
                _color_pnl, subset=_style_subset
            )
        except AttributeError:
            # pandas < 2.1 fallback
            styled = summary_df.style.applymap(
                _color_pnl, subset=_style_subset
            )
        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            column_config=metrics_col_config,
            height=min(400, 38 + len(summary_df) * 35),
        )

        notes = []
        if ttm_notes:
            notes.append("TTM-adjusted: " + " | ".join(ttm_notes))
        if split_notes:
            notes.append("Split-adjusted: " + " | ".join(split_notes))
        if notes:
            st.caption(" · ".join(notes))
    else:
        st.info("No metrics to display.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: Comparison Heatmap
# ══════════════════════════════════════════════════════════════════════════════

with tab_heatmap:
    if rows:
        heatmap_df = pd.DataFrame(rows).set_index("Symbol")

        # Select numeric columns that have meaningful variance for comparison
        heatmap_metrics = ["Revenue", "Net Income", "EPS (Diluted)", "Equity",
                           "Operating Income", "Cash"]
        available_hm = [c for c in heatmap_metrics if c in heatmap_df.columns]

        if len(available_hm) >= 2 and len(heatmap_df) >= 2:
            st.subheader("Fundamentals Comparison")
            st.caption(
                "Each cell shows a z-score: how many standard deviations above/below "
                "the portfolio average. Green = above average, red = below."
            )

            hm_data = heatmap_df[available_hm].apply(pd.to_numeric, errors="coerce")

            # Z-score normalization (per column)
            means = hm_data.mean()
            stds = hm_data.std().replace(0, 1)
            z_scores = (hm_data - means) / stds

            fig = go.Figure(
                data=go.Heatmap(
                    z=z_scores.values,
                    x=z_scores.columns.tolist(),
                    y=z_scores.index.tolist(),
                    colorscale=[
                        [0, "#ef4444"],
                        [0.5, "#f8fafc"],
                        [1, "#22c55e"],
                    ],
                    zmid=0,
                    text=hm_data.map(
                        lambda v: f"${v:,.0f}" if pd.notna(v) else "—"
                    ).values,
                    texttemplate="%{text}",
                    textfont=dict(size=11),
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "%{x}: %{text}<br>"
                        "Z-score: %{z:.2f}"
                        "<extra></extra>"
                    ),
                    showscale=True,
                    colorbar=dict(
                        title="Z-Score",
                        thickness=12,
                        len=0.6,
                    ),
                )
            )
            fig.update_layout(
                height=max(300, len(heatmap_df) * 45 + 80),
                xaxis=dict(side="top", tickangle=0),
                yaxis=dict(autorange="reversed"),
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

            # ── Bar chart: compare a single metric across symbols ────────
            st.markdown("---")
            st.subheader("Compare Metric Across Symbols")
            compare_metric = st.selectbox(
                "Select metric",
                options=available_hm,
                index=0,
                key="compare_metric",
            )

            if compare_metric:
                bar_data = hm_data[[compare_metric]].dropna().sort_values(
                    compare_metric, ascending=True
                )
                colors = [
                    COLORS["profit"] if v >= 0 else COLORS["loss"]
                    for v in bar_data[compare_metric]
                ]
                fig_bar = go.Figure(
                    go.Bar(
                        x=bar_data[compare_metric],
                        y=bar_data.index,
                        orientation="h",
                        marker_color=colors,
                        hovertemplate="<b>%{y}</b><br>%{x:$,.0f}<extra></extra>",
                    )
                )
                fig_bar.update_layout(
                    height=max(250, len(bar_data) * 35 + 60),
                    xaxis_tickformat="$,.0f",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Need at least 2 symbols with data to show comparison heatmap.")
    else:
        st.info("No metrics data available for comparison.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: Symbol Detail
# ══════════════════════════════════════════════════════════════════════════════

with tab_detail:
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
            # ── Split detection ──────────────────────────────────────────
            detected_splits = _get_splits_for_symbol(detail_symbol)

            # ── Styled KPI cards ─────────────────────────────────────────
            card_metrics = [
                ("revenue", "Revenue"),
                ("net_income", "Net Income"),
                ("eps_diluted", "EPS (Diluted)"),
                ("operating_income", "Operating Income"),
            ]

            cols = st.columns(len(card_metrics), gap="medium")
            for col, (key, label) in zip(cols, card_metrics):
                if key in latest:
                    raw = latest[key].get("metric_value")
                    period = latest[key].get("period_end", "")
                    fp = latest[key].get("fiscal_period", "")
                    try:
                        val = float(raw)
                        suffixes = []

                        if is_flow_metric(key) and fp:
                            history = get_stock_metrics(symbol=detail_symbol, metric_name=key)
                            history = normalize_metrics(history, detected_splits, key)
                            vk = "normalized_value" if detected_splits else "metric_value"

                            try:
                                q_val, q_label, q_method = compute_quarterly_latest(history, value_key=vk)
                            except TypeError:
                                q_val, q_label, q_method = None, None, None

                            try:
                                ttm_val, ttm_method = compute_ttm_latest(history, value_key=vk)
                            except TypeError:
                                ttm_val, ttm_method = None, None

                            if q_val is not None:
                                val = q_val
                                suffixes.append(f"{q_label}")
                            elif ttm_val is not None:
                                val = ttm_val
                                suffixes.append("TTM")

                            if detected_splits:
                                suffixes.append("adj")
                        elif detected_splits and key in (SPLIT_AFFECTED_PER_SHARE | SPLIT_AFFECTED_SHARE_COUNT):
                            val, was_adj = normalize_latest_value(key, val, str(period), detected_splits)
                            if was_adj:
                                suffixes.append("adj")

                        adjusted_label = label
                        if suffixes:
                            adjusted_label = f"{label} ({', '.join(suffixes)})"

                        if key.startswith("eps"):
                            display = f"${val:,.2f}"
                        else:
                            display = f"${val:,.0f}"

                        # Show delta color based on positive/negative
                        delta_color = "normal" if val >= 0 else "inverse"
                        col.metric(
                            adjusted_label,
                            display,
                            delta="Positive" if val >= 0 else "Negative",
                            delta_color=delta_color,
                        )
                    except (ValueError, TypeError):
                        col.metric(label, str(raw))
                else:
                    col.metric(label, "—")

            if detected_splits:
                with st.expander(f"Detected splits ({len(detected_splits)})", expanded=True):
                    for sp in detected_splits:
                        icon = "🔴" if sp.confidence == "high" else "🟡"
                        st.markdown(
                            f"{icon} **{sp.period_end}** — "
                            f"ratio {sp.shares_ratio:.2f}x "
                            f"({sp.confidence} confidence)"
                        )
                        st.caption(sp.reason)

            # ── Historical data: tabbed view ─────────────────────────────
            st.markdown("---")
            st.subheader("Historical Metrics")

            hist_metric = st.selectbox(
                "Metric",
                options=sorted(latest.keys()),
                key="hist_metric",
            )

            show_raw = False
            if detected_splits:
                show_raw = st.checkbox(
                    "Show raw (unadjusted) values",
                    value=False,
                    help="Uncheck to see split-adjusted values (default)",
                )

            if hist_metric:
                history = get_stock_metrics(symbol=detail_symbol, metric_name=hist_metric)
                if history:
                    history.sort(key=lambda r: str(r.get("period_end", "")))
                    history = normalize_metrics(history, detected_splits, hist_metric)

                    has_ttm = False
                    ttm_value_key = "normalized_value" if detected_splits else "metric_value"
                    if is_flow_metric(hist_metric):
                        try:
                            ttm_history = compute_ttm(history, value_key=ttm_value_key)
                        except TypeError:
                            st.error(
                                f"TTM computation failed for {hist_metric}. "
                                "This may indicate a version mismatch — try restarting the app."
                            )
                            ttm_history = []
                        ttm_by_pe = {
                            str(r.get("period_end", "")): r for r in ttm_history
                        }
                        for orig in history:
                            pe = str(orig.get("period_end", ""))
                            ttm_row = ttm_by_pe.get(pe, {})
                            orig["quarterly_value"] = ttm_row.get("quarterly_value")
                            orig["ttm_value"] = ttm_row.get("ttm_value")
                            orig["ttm_method"] = ttm_row.get("ttm_method")
                            orig["is_ytd"] = ttm_row.get("is_ytd", False)
                            if not orig.get("fiscal_period"):
                                orig["fiscal_period"] = ttm_row.get("fiscal_period", "")
                        has_ttm = any(r.get("ttm_value") is not None for r in history)

                    if has_ttm:
                        value_col = "ttm_value"
                    elif detected_splits and not show_raw:
                        value_col = "normalized_value"
                    else:
                        value_col = "metric_value"

                    hist_df = pd.DataFrame(history)
                    hist_df["period_end"] = pd.to_datetime(hist_df["period_end"])
                    hist_df[value_col] = pd.to_numeric(hist_df[value_col], errors="coerce")
                    hist_df = hist_df.sort_values("period_end")

                    # ── Sub-tabs: Chart / Data Table ─────────────────────
                    sub_chart, sub_table = st.tabs(["Trend Chart", "Data Table"])

                    with sub_chart:
                        if has_ttm:
                            st.caption(
                                "Showing TTM (trailing twelve months) values for "
                                "apples-to-apples comparison across 10-K and 10-Q filings"
                            )

                        chart_df = hist_df.dropna(subset=[value_col])
                        if not chart_df.empty:
                            # Color by positive/negative values
                            y_vals = chart_df[value_col].tolist()
                            bar_colors = [
                                COLORS["profit"] if v >= 0 else COLORS["loss"]
                                for v in y_vals
                            ]

                            # Use bars for quarterly data, line for longer series
                            if len(chart_df) <= 20:
                                fig = go.Figure(
                                    go.Bar(
                                        x=chart_df["period_end"],
                                        y=chart_df[value_col],
                                        marker_color=bar_colors,
                                        hovertemplate=(
                                            "<b>%{x|%Y-%m-%d}</b><br>"
                                            "Value: %{y:$,.0f}"
                                            "<extra></extra>"
                                        ),
                                    )
                                )
                            else:
                                fig = go.Figure(
                                    go.Scatter(
                                        x=chart_df["period_end"],
                                        y=chart_df[value_col],
                                        mode="lines+markers",
                                        line=dict(color=COLORS["primary"], width=2),
                                        marker=dict(size=5),
                                        hovertemplate=(
                                            "<b>%{x|%Y-%m-%d}</b><br>"
                                            "Value: %{y:$,.0f}"
                                            "<extra></extra>"
                                        ),
                                    )
                                )

                            # Add zero reference line
                            fig.add_hline(
                                y=0,
                                line_dash="dot",
                                line_color="#94a3b8",
                                line_width=1,
                            )

                            fig.update_layout(
                                height=400,
                                yaxis_tickformat="$,.0f",
                                xaxis_title=None,
                                yaxis_title=hist_metric.replace("_", " ").title(),
                                showlegend=False,
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No numeric data to chart.")

                    with sub_table:
                        display_cols = [
                            "period_end", "fiscal_period", "fiscal_year",
                            "filing_type", "metric_value", "duration_days",
                            "reporting_style",
                        ]
                        if has_ttm:
                            display_cols.extend([
                                "quarterly_value", "ttm_value",
                                "ttm_method", "is_ytd",
                            ])
                        if detected_splits:
                            display_cols.extend(["normalized_value", "split_adjusted"])
                        if not has_ttm and not detected_splits:
                            display_cols.append("source")

                        available = [c for c in display_cols if c in hist_df.columns]
                        st.dataframe(
                            hist_df[available].reset_index(drop=True),
                            use_container_width=True,
                            hide_index=True,
                        )
                else:
                    st.info(f"No historical data for {hist_metric}.")

        # Debug info (fully collapsed)
        with st.expander("Debug: raw metric data"):
            for name, row in sorted(latest.items()):
                st.text(
                    f"{name}: value={row.get('metric_value')}  "
                    f"period={row.get('period_end')}  "
                    f"filing={row.get('filing_type')}  "
                    f"cik={row.get('cik')}  "
                    f"fetched={row.get('fetched_at')}"
                )
