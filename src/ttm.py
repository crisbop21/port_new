"""Trailing Twelve Months (TTM) computation for SEC EDGAR metrics.

Problem:
    10-K filings report 12-month cumulative income-statement figures.
    10-Q filings report year-to-date (YTD) cumulative figures:
        Q1 = 3-month, Q2 = 6-month, Q3 = 9-month.
    Comparing a 10-K revenue to a Q1 10-Q revenue is meaningless.

Solution — isolate each quarter, then sum the last 4:
    Q1 isolated = Q1 YTD                  (already 3 months)
    Q2 isolated = Q2 YTD − Q1 YTD
    Q3 isolated = Q3 YTD − Q2 YTD
    Q4 isolated = FY     − Q3 YTD
    TTM         = sum of last 4 isolated quarters

Balance-sheet items (assets, liabilities, equity, shares, cash) are
point-in-time snapshots and do NOT need TTM adjustment.
"""

import logging
from dataclasses import dataclass
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
    "gross_profit",
    "capital_expenditures",
    "dividends_paid",
    "interest_expense",
}

# Balance-sheet / point-in-time metrics — no TTM needed.
POINT_IN_TIME_METRICS = {
    "total_assets",
    "total_liabilities",
    "stockholders_equity",
    "shares_outstanding",
    "cash_and_equivalents",
    "current_assets",
    "current_liabilities",
    "long_term_debt",
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


# ── Quarter isolation ───────────────────────────────────────────────────────

# Ordered so we can look up the prior quarter's YTD
_PRIOR_YTD = {"Q1": None, "Q2": "Q1", "Q3": "Q2", "Q4": "Q3"}


@dataclass
class IsolatedQuarter:
    """A single quarter's isolated (non-cumulative) value."""

    fiscal_year: int
    quarter: str          # Q1, Q2, Q3, Q4
    period_end: str
    ytd_value: float      # raw YTD value from the filing
    isolated_value: float  # just this quarter's contribution
    method: str           # 'direct' (Q1) | 'subtracted' (Q2-Q4)


def isolate_quarters(
    metric_rows: list[dict],
    value_key: str = "metric_value",
) -> list[IsolatedQuarter]:
    """Derive isolated quarterly values from YTD-cumulative SEC data.

    Input rows must have: period_end, fiscal_period, and the field
    named by *value_key* (default "metric_value").

    Args:
        metric_rows: historical metric rows from DB
        value_key:   which dict key holds the value to isolate.
                     Use "normalized_value" when split-adjusted data is available.

    Derivation:
        Q1 isolated = Q1 YTD  (already a single quarter)
        Q2 isolated = Q2 YTD − Q1 YTD
        Q3 isolated = Q3 YTD − Q2 YTD
        Q4 isolated = FY      − Q3 YTD   (FY is the full-year 10-K value)

    Returns isolated quarters sorted chronologically.
    """
    if not metric_rows:
        return []

    # Build lookup: (fiscal_year, fiscal_period) → float YTD value
    by_fy_fp: dict[tuple[int, str], tuple[float, str]] = {}
    for row in metric_rows:
        fp = row.get("fiscal_period", "")
        pe = str(row.get("period_end", ""))
        fy = _fiscal_year_for(pe)
        if not fy or not fp:
            continue
        try:
            val = float(row.get(value_key) or row.get("metric_value", 0))
        except (ValueError, TypeError):
            continue
        key = (fy, fp)
        # Keep latest entry per (fy, fp)
        if key not in by_fy_fp or pe >= by_fy_fp[key][1]:
            by_fy_fp[key] = (val, pe)

    quarters: list[IsolatedQuarter] = []

    for (fy, fp), (ytd_val, pe) in sorted(by_fy_fp.items()):
        if fp == "FY":
            # FY contributes Q4 = FY − Q3 YTD
            q3_entry = by_fy_fp.get((fy, "Q3"))
            if q3_entry is not None:
                q4_val = round(ytd_val - q3_entry[0], 4)
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter="Q4", period_end=pe,
                    ytd_value=ytd_val, isolated_value=q4_val,
                    method="subtracted",
                ))
            else:
                # No Q3 data — can't isolate Q4, but we know the annual total.
                # Store as Q4 = FY (best we can do; mark method accordingly).
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter="Q4", period_end=pe,
                    ytd_value=ytd_val, isolated_value=ytd_val,
                    method="annual_only",
                ))
            continue

        if fp == "Q4":
            # Rare: Q4 in a 10-Q. Treat YTD as full year → same as FY logic.
            q3_entry = by_fy_fp.get((fy, "Q3"))
            if q3_entry is not None:
                q4_val = round(ytd_val - q3_entry[0], 4)
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter="Q4", period_end=pe,
                    ytd_value=ytd_val, isolated_value=q4_val,
                    method="subtracted",
                ))
            else:
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter="Q4", period_end=pe,
                    ytd_value=ytd_val, isolated_value=ytd_val,
                    method="annual_only",
                ))
            continue

        prior_fp = _PRIOR_YTD.get(fp)

        if prior_fp is None:
            # Q1: already isolated
            quarters.append(IsolatedQuarter(
                fiscal_year=fy, quarter=fp, period_end=pe,
                ytd_value=ytd_val, isolated_value=ytd_val,
                method="direct",
            ))
        else:
            # Q2, Q3: subtract prior quarter's YTD
            prior_entry = by_fy_fp.get((fy, prior_fp))
            if prior_entry is not None:
                isolated = round(ytd_val - prior_entry[0], 4)
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter=fp, period_end=pe,
                    ytd_value=ytd_val, isolated_value=isolated,
                    method="subtracted",
                ))
            else:
                # Can't isolate without prior YTD
                logger.debug(
                    "Cannot isolate %s FY%d: missing %s YTD",
                    fp, fy, prior_fp,
                )

    # Sort chronologically by (fiscal_year, quarter order)
    q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    quarters.sort(key=lambda q: (q.fiscal_year, q_order.get(q.quarter, 9)))

    return quarters


# ── TTM from isolated quarters ──────────────────────────────────────────────


def compute_ttm(
    metric_rows: list[dict],
    value_key: str = "metric_value",
) -> list[dict]:
    """Compute TTM values for a single flow metric's history.

    Strategy:
        1. Isolate each quarter (Q1, Q2, Q3, Q4) from YTD data
        2. For each period, TTM = sum of the 4 most recent isolated quarters
           up to and including that period

    Args:
        metric_rows: historical metric rows
        value_key:   which dict key holds the value to use.
                     Use "normalized_value" after split normalization.

    Returns a new list of dicts (chronological) with added keys:
        - quarterly_value: the isolated single-quarter figure (or None)
        - ttm_value: trailing-twelve-month figure (or None if < 4 quarters)
        - ttm_method: 'annual' | 'sum_4q' | None
        - is_ytd: bool — True if the raw value is a YTD cumulative
    """
    if not metric_rows:
        return []

    sorted_rows = sorted(metric_rows, key=lambda r: str(r.get("period_end", "")))

    # Isolate quarters
    isolated = isolate_quarters(metric_rows, value_key=value_key)

    # Build a lookup from period_end → IsolatedQuarter for enriching rows
    iso_by_pe: dict[str, IsolatedQuarter] = {}
    for q in isolated:
        iso_by_pe[q.period_end] = q

    # Build running list for TTM: at each quarter, sum the last 4
    ttm_at_pe: dict[str, tuple[float, str]] = {}
    for i, q in enumerate(isolated):
        if q.method == "annual_only" and q.quarter == "Q4":
            # FY with no quarterly breakdown → the value IS 12 months
            ttm_at_pe[q.period_end] = (q.ytd_value, "annual")
        elif i >= 3:
            window = isolated[i - 3 : i + 1]
            # Verify we have exactly 4 consecutive quarters
            expected_quarters = {"Q1", "Q2", "Q3", "Q4"}
            actual_quarters = {wq.quarter for wq in window}
            if actual_quarters == expected_quarters:
                ttm_val = round(sum(wq.isolated_value for wq in window), 4)
                ttm_at_pe[q.period_end] = (ttm_val, "sum_4q")

    # Enrich each row
    result = []
    for row in sorted_rows:
        r = dict(row)
        pe = str(r.get("period_end", ""))
        fp = r.get("fiscal_period", "")

        iso_q = iso_by_pe.get(pe)
        r["quarterly_value"] = iso_q.isolated_value if iso_q else None

        r["is_ytd"] = fp in ("Q2", "Q3")

        ttm_entry = ttm_at_pe.get(pe)
        if fp == "FY" and ttm_entry is None:
            # FY value is already 12 months
            try:
                r["ttm_value"] = float(r.get(value_key) or r.get("metric_value", 0))
                r["ttm_method"] = "annual"
            except (ValueError, TypeError):
                r["ttm_value"] = None
                r["ttm_method"] = None
        elif ttm_entry is not None:
            r["ttm_value"] = ttm_entry[0]
            r["ttm_method"] = ttm_entry[1]
        else:
            r["ttm_value"] = None
            r["ttm_method"] = None

        result.append(r)

    return result


def compute_ttm_latest(
    metric_rows: list[dict],
    value_key: str = "metric_value",
) -> tuple[float | None, str | None]:
    """Compute just the latest TTM value for a flow metric.

    Returns (ttm_value, ttm_method) or (None, None) if not computable.
    """
    ttm_rows = compute_ttm(metric_rows, value_key=value_key)
    if not ttm_rows:
        return None, None

    # Return the most recent row that has a TTM value
    for row in reversed(ttm_rows):
        if row.get("ttm_value") is not None:
            return row["ttm_value"], row["ttm_method"]

    return None, None
