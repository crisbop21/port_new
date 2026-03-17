"""Trailing Twelve Months (TTM) computation for SEC EDGAR metrics.

Problem:
    10-K filings report 12-month cumulative income-statement figures.
    10-Q filings report year-to-date (YTD) cumulative figures:
        Q1 = 3-month, Q2 = 6-month, Q3 = 9-month.
    Comparing a 10-K revenue to a Q1 10-Q revenue is meaningless.

Solution:
    TTM = latest YTD + prior FY − prior same-quarter YTD

    For FY periods, TTM = the reported value (already 12 months).
    For Q-n periods:
        TTM = Q-n YTD + prior_FY − prior_Q-n YTD

Balance-sheet items (assets, liabilities, equity, shares, cash) are
point-in-time snapshots and do NOT need TTM adjustment.
"""

import logging
from datetime import date

logger = logging.getLogger(__name__)

# ── Classification ──────────────────────────────────────────────────────────

# Income-statement / flow metrics — these are cumulative over a period
# and NEED TTM adjustment when mixing 10-K and 10-Q.
FLOW_METRICS = {
    "revenue",
    "net_income",
    "eps_basic",
    "eps_diluted",
    "operating_income",
}

# Balance-sheet / point-in-time metrics — no TTM needed.
POINT_IN_TIME_METRICS = {
    "total_assets",
    "total_liabilities",
    "stockholders_equity",
    "shares_outstanding",
    "cash_and_equivalents",
}


def is_flow_metric(metric_name: str) -> bool:
    """True if this metric is an income-statement flow that needs TTM."""
    return metric_name in FLOW_METRICS


def _fiscal_year_for(period_end: str) -> int:
    """Derive the fiscal year from a period_end date string."""
    try:
        return date.fromisoformat(period_end).year
    except (ValueError, TypeError):
        return 0


def compute_ttm(
    metric_rows: list[dict],
) -> list[dict]:
    """Compute TTM values for a single flow metric's history.

    Each row must have: metric_value, period_end, fiscal_period, filing_type.

    Returns a new list of dicts with added keys:
        - ttm_value: the trailing-twelve-month figure (or None if not computable)
        - ttm_method: 'annual' | 'computed' | None
        - is_ytd: bool — True if the raw value is a YTD cumulative (not 12-month)

    Rows are returned in chronological order.
    """
    if not metric_rows:
        return []

    # Sort chronologically
    sorted_rows = sorted(metric_rows, key=lambda r: str(r.get("period_end", "")))

    # Build lookup: (fiscal_year, fiscal_period) → float value
    # This lets us find "prior FY" and "prior same-quarter YTD" efficiently.
    by_fy_fp: dict[tuple[int, str], dict] = {}
    for row in sorted_rows:
        fp = row.get("fiscal_period", "")
        pe = str(row.get("period_end", ""))
        fy = _fiscal_year_for(pe)
        if fy and fp:
            # If multiple entries for same (fy, fp), keep the latest-filed
            key = (fy, fp)
            existing = by_fy_fp.get(key)
            if existing is None or str(row.get("period_end", "")) >= str(existing.get("period_end", "")):
                by_fy_fp[key] = row

    result = []
    for row in sorted_rows:
        r = dict(row)
        fp = r.get("fiscal_period", "")
        pe = str(r.get("period_end", ""))
        fy = _fiscal_year_for(pe)

        try:
            val = float(r.get("metric_value", 0))
        except (ValueError, TypeError):
            r["ttm_value"] = None
            r["ttm_method"] = None
            r["is_ytd"] = False
            result.append(r)
            continue

        # Annual filings: the value IS the TTM
        if fp == "FY":
            r["ttm_value"] = val
            r["ttm_method"] = "annual"
            r["is_ytd"] = False
            result.append(r)
            continue

        # Quarterly filings: compute TTM = current YTD + prior FY - prior same-quarter YTD
        r["is_ytd"] = True

        if fp == "Q1":
            # Q1 YTD is just 3 months. TTM = Q1 + prior_FY - prior_Q1
            prior_fy_row = by_fy_fp.get((fy - 1, "FY"))
            prior_q1_row = by_fy_fp.get((fy - 1, "Q1"))

            if prior_fy_row is not None and prior_q1_row is not None:
                try:
                    prior_fy_val = float(prior_fy_row.get("metric_value", 0))
                    prior_q1_val = float(prior_q1_row.get("metric_value", 0))
                    r["ttm_value"] = round(val + prior_fy_val - prior_q1_val, 4)
                    r["ttm_method"] = "computed"
                except (ValueError, TypeError):
                    r["ttm_value"] = None
                    r["ttm_method"] = None
            elif prior_fy_row is not None:
                # No prior Q1 — can't compute exact TTM, but we can approximate
                # TTM ≈ Q1 * 4 is too rough. Better: just use prior FY as fallback.
                r["ttm_value"] = None
                r["ttm_method"] = None
            else:
                r["ttm_value"] = None
                r["ttm_method"] = None

        elif fp in ("Q2", "Q3"):
            # Q2 YTD = 6 months, Q3 YTD = 9 months
            # TTM = current YTD + prior_FY - prior same-quarter YTD
            prior_fy_row = by_fy_fp.get((fy - 1, "FY"))
            prior_same_q_row = by_fy_fp.get((fy - 1, fp))

            if prior_fy_row is not None and prior_same_q_row is not None:
                try:
                    prior_fy_val = float(prior_fy_row.get("metric_value", 0))
                    prior_same_q_val = float(prior_same_q_row.get("metric_value", 0))
                    r["ttm_value"] = round(val + prior_fy_val - prior_same_q_val, 4)
                    r["ttm_method"] = "computed"
                except (ValueError, TypeError):
                    r["ttm_value"] = None
                    r["ttm_method"] = None
            else:
                r["ttm_value"] = None
                r["ttm_method"] = None

        elif fp == "Q4":
            # Q4 in a 10-Q context is unusual (most companies file 10-K for Q4).
            # If present, the YTD value should equal the full year.
            r["ttm_value"] = val
            r["ttm_method"] = "annual"
            r["is_ytd"] = False

        else:
            # Unknown fiscal period
            r["ttm_value"] = None
            r["ttm_method"] = None

        result.append(r)

    return result


def compute_ttm_latest(metric_rows: list[dict]) -> tuple[float | None, str | None]:
    """Compute just the latest TTM value for a flow metric.

    Returns (ttm_value, ttm_method) or (None, None) if not computable.
    """
    ttm_rows = compute_ttm(metric_rows)
    if not ttm_rows:
        return None, None

    # Return the most recent row that has a TTM value
    for row in reversed(ttm_rows):
        if row.get("ttm_value") is not None:
            return row["ttm_value"], row["ttm_method"]

    return None, None
