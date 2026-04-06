"""Shared UI helpers used across Streamlit pages."""

from datetime import date, timedelta

import pandas as pd
import streamlit as st


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
