"""Shared UI helpers used across Streamlit pages."""

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st


# ── Financial color palette ──────────────────────────────────────────────────

COLORS = {
    "primary": "#1f77b4",
    "profit": "#22c55e",
    "loss": "#ef4444",
    "warning": "#f59e0b",
    "neutral": "#64748b",
    "series": [
        "#1f77b4",
        "#22c55e",
        "#f59e0b",
        "#a855f7",
        "#ef4444",
        "#3b82f6",
        "#ec4899",
        "#06b6d4",
    ],
}

# ── Plotly chart template ────────────────────────────────────────────────────

_ibkr_template = go.layout.Template(
    layout=go.Layout(
        font=dict(family="Source Sans Pro, sans-serif", color="#262730"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=COLORS["series"],
        hovermode="x unified",
        hoverlabel=dict(bgcolor="white", font_size=13),
        xaxis=dict(gridcolor="#e2e8f0", showgrid=True),
        yaxis=dict(gridcolor="#e2e8f0", showgrid=True),
        margin=dict(l=0, r=0, t=40, b=0),
    )
)
pio.templates["ibkr"] = _ibkr_template
pio.templates.default = "ibkr"


# ── Metric card CSS ─────────────────────────────────────────────────────────

METRIC_CARD_CSS = """
<style>
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
    border: 1px solid #e2e8f0;
    border-radius: 0.6rem;
    padding: 0.8rem 1rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
[data-testid="stMetric"] label {
    font-size: 0.78rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
[data-testid="stMetricValue"] {
    font-size: 1.5rem;
    font-weight: 700;
}
</style>
"""


def inject_metric_card_css():
    """Inject CSS to style st.metric widgets as polished cards."""
    st.markdown(METRIC_CARD_CSS, unsafe_allow_html=True)


# ── Score color coding ─────────────────────────────────────────────────────


def color_score(val):
    """Return CSS background-color for a score value (0-100 scale).

    Green >= 70, Yellow >= 40, Red < 40.
    Used by Technical and Valuation pages for styled dataframes.
    """
    if isinstance(val, str):
        try:
            val = float(val)
        except ValueError:
            return ""
    if pd.isna(val):
        return ""
    if val >= 70:
        return "background-color: #c6efce"
    elif val >= 40:
        return "background-color: #ffeb9c"
    else:
        return "background-color: #ffc7ce"


# ── Currency / percentage formatting ───────────────────────────────────────


def fmt_currency(val: float | None) -> str:
    """Format a number as $X,XXX.XX."""
    if val is None:
        return "—"
    return f"${val:,.2f}"


def fmt_pct(val: float | None, decimals: int = 1) -> str:
    """Format a number as a percentage string."""
    if val is None:
        return "—"
    return f"{val:.{decimals}%}"


# ── Data freshness badge ──────────────────────────────────────────────────


def freshness_badge(last_date: date | str | None) -> str:
    """Return a colored text badge indicating data age.

    Returns a markdown string with:
      - green circle + date if < 1 day old
      - yellow circle + date if 1-7 days old
      - red circle + date if > 7 days old
      - gray "No data" if None
    """
    if last_date is None:
        return ":gray[No data]"

    if isinstance(last_date, str):
        try:
            last_date = date.fromisoformat(last_date)
        except (ValueError, TypeError):
            return ":gray[Unknown]"

    age_days = (date.today() - last_date).days

    date_str = last_date.isoformat()
    if age_days <= 1:
        return f":green[{date_str}]"
    elif age_days <= 7:
        return f":orange[{date_str}]"
    else:
        return f":red[{date_str}]"


def render_freshness(label: str, last_date: date | str | None):
    """Render a freshness indicator inline: 'Label · last updated: badge'."""
    badge = freshness_badge(last_date)
    st.caption(f"{label} · Last updated: {badge}")
