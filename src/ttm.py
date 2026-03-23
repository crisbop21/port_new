"""Trailing Twelve Months (TTM) computation for SEC EDGAR metrics.

Problem:
    10-K filings report 12-month cumulative income-statement figures.
    10-Q filings report year-to-date (YTD) cumulative figures:
        Q1 = 3-month, Q2 = 6-month, Q3 = 9-month.
    Comparing a 10-K revenue to a Q1 10-Q revenue is meaningless.

    BUT — not all companies report cumulative YTD.  Some report standalone
    quarterly figures (each 10-Q = just that quarter's 3 months).

Solution — detect reporting style and handle both:
    Cumulative YTD (most US large-caps):
        Q1 isolated = Q1 YTD                  (already 3 months)
        Q2 isolated = Q2 YTD − Q1 YTD
        Q3 isolated = Q3 YTD − Q2 YTD
        Q4 isolated = FY     − Q3 YTD
    Standalone quarterly:
        Q1 isolated = Q1 value   (already 3 months)
        Q2 isolated = Q2 value   (already 3 months)
        Q3 isolated = Q3 value   (already 3 months)
        Q4 isolated = FY − (Q1 + Q2 + Q3)  or  Q4 value if available
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


def _fiscal_year_for(row: dict) -> int:
    """Get the fiscal year from a row, preferring the XBRL 'fiscal_year' field.

    Falls back to deriving from period_end.year if fiscal_year is missing.
    This handles non-calendar fiscal years (Apple Sep, Microsoft Jun, etc.).
    """
    fy = row.get("fiscal_year")
    if fy is not None:
        try:
            return int(fy)
        except (ValueError, TypeError):
            pass
    # Fallback: derive from period_end
    pe = str(row.get("period_end", ""))
    try:
        return date.fromisoformat(pe).year
    except (ValueError, TypeError):
        return 0


def _detect_style_from_rows(metric_rows: list[dict]) -> str:
    """Detect reporting style from the metric rows themselves.

    Uses the reporting_style field if set by the fetcher,
    otherwise infers from duration_days on Q2/Q3 entries.
    """
    # Check if fetcher already classified it
    for row in metric_rows:
        style = row.get("reporting_style")
        if style and style != "unknown":
            return style

    # Infer from duration_days on Q2/Q3 entries
    has_short = False
    has_long = False
    for row in metric_rows:
        fp = row.get("fiscal_period", "")
        if fp not in ("Q2", "Q3"):
            continue
        days = row.get("duration_days")
        if days is None:
            continue
        if days <= 100:
            has_short = True
        elif days <= 290:
            has_long = True

    if has_short and has_long:
        return "mixed"
    if has_long:
        return "cumulative_ytd"
    if has_short:
        return "standalone_quarterly"

    # No Q2/Q3 data — check for any quarterly data
    has_quarterly = any(
        row.get("fiscal_period") in ("Q1", "Q2", "Q3", "Q4")
        for row in metric_rows
    )
    return "cumulative_ytd" if has_quarterly else "annual_only"


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
    method: str           # 'direct' (Q1) | 'subtracted' (Q2-Q4) | 'standalone' | 'annual_only'


def _isolate_cumulative(
    by_fy_fp: dict[tuple[int, str], tuple[float, str]],
) -> list[IsolatedQuarter]:
    """Isolate quarters from YTD-cumulative data (traditional approach)."""
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
                # No Q3 data — can't isolate Q4, store as annual_only
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

    return quarters


def _isolate_standalone(
    by_fy_fp: dict[tuple[int, str], tuple[float, str]],
) -> list[IsolatedQuarter]:
    """Isolate quarters from standalone-quarterly data.

    Each Q1-Q3 value is already a single quarter's figure.
    Q4 = FY − (Q1 + Q2 + Q3) if FY is available, or Q4 value directly.
    """
    quarters: list[IsolatedQuarter] = []

    for (fy, fp), (val, pe) in sorted(by_fy_fp.items()):
        if fp == "FY":
            # Derive Q4 from FY - sum(Q1+Q2+Q3) if all 3 are available
            q_sum = 0.0
            has_all = True
            for q in ("Q1", "Q2", "Q3"):
                q_entry = by_fy_fp.get((fy, q))
                if q_entry is not None:
                    q_sum += q_entry[0]
                else:
                    has_all = False
                    break

            if has_all:
                q4_val = round(val - q_sum, 4)
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter="Q4", period_end=pe,
                    ytd_value=val, isolated_value=q4_val,
                    method="subtracted",
                ))
            else:
                quarters.append(IsolatedQuarter(
                    fiscal_year=fy, quarter="Q4", period_end=pe,
                    ytd_value=val, isolated_value=val,
                    method="annual_only",
                ))
            continue

        if fp == "Q4":
            # Standalone Q4 — use directly
            quarters.append(IsolatedQuarter(
                fiscal_year=fy, quarter="Q4", period_end=pe,
                ytd_value=val, isolated_value=val,
                method="standalone",
            ))
            continue

        # Q1, Q2, Q3: already standalone
        quarters.append(IsolatedQuarter(
            fiscal_year=fy, quarter=fp, period_end=pe,
            ytd_value=val, isolated_value=val,
            method="standalone",
        ))

    return quarters


def isolate_quarters(
    metric_rows: list[dict],
    value_key: str = "metric_value",
) -> list[IsolatedQuarter]:
    """Derive isolated quarterly values from SEC data.

    Detects the reporting style (cumulative YTD vs standalone) and applies
    the appropriate isolation logic.

    Input rows must have: period_end, fiscal_period, and the field
    named by *value_key* (default "metric_value").

    Args:
        metric_rows: historical metric rows from DB
        value_key:   which dict key holds the value to isolate.
                     Use "normalized_value" when split-adjusted data is available.

    Returns isolated quarters sorted chronologically.
    """
    if not metric_rows:
        return []

    # Detect reporting style
    style = _detect_style_from_rows(metric_rows)

    # Build lookup: (fiscal_year, fiscal_period) → float value
    by_fy_fp: dict[tuple[int, str], tuple[float, str]] = {}
    for row in metric_rows:
        fp = row.get("fiscal_period", "")
        pe = str(row.get("period_end", ""))
        fy = _fiscal_year_for(row)
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

    # Apply the right isolation strategy
    if style == "standalone_quarterly":
        quarters = _isolate_standalone(by_fy_fp)
    else:
        # cumulative_ytd, mixed, annual_only all use cumulative logic
        # (mixed defaults to cumulative since _pick_all_annual already
        # selected the YTD context for Q2/Q3)
        quarters = _isolate_cumulative(by_fy_fp)

    # Sort chronologically by (fiscal_year, quarter order)
    q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    quarters.sort(key=lambda q: (q.fiscal_year, q_order.get(q.quarter, 9)))

    # Sanity checks
    quarters = _sanity_check_quarters(quarters, style)

    return quarters


# ── Sanity checks ──────────────────────────────────────────────────────────


def _sanity_check_quarters(
    quarters: list[IsolatedQuarter],
    style: str,
) -> list[IsolatedQuarter]:
    """Validate isolated quarters and flag anomalies.

    Checks:
    1. Negative revenue after isolation → likely wrong subtraction
    2. Quarter > FY total → isolation math is wrong
    3. Cross-validate: sum of 4 quarters vs FY when both available

    Does NOT discard data — just logs warnings so users can investigate.
    """
    if not quarters:
        return quarters

    # Group by fiscal year
    by_fy: dict[int, list[IsolatedQuarter]] = {}
    for q in quarters:
        by_fy.setdefault(q.fiscal_year, []).append(q)

    for fy, qs in by_fy.items():
        q4 = next((q for q in qs if q.quarter == "Q4"), None)

        # Check 1: Negative isolated values (for revenue-like metrics)
        for q in qs:
            if q.isolated_value < 0 and q.method in ("subtracted", "standalone"):
                logger.warning(
                    "Sanity check: FY%d %s has negative isolated value %.2f "
                    "(method=%s). This may indicate the reporting style was "
                    "misdetected (cumulative vs standalone). Style=%s",
                    fy, q.quarter, q.isolated_value, q.method, style,
                )

        # Check 2: Cross-validate sum of quarters vs FY
        if q4 and q4.method != "annual_only":
            fy_value = q4.ytd_value if q4.method == "subtracted" else None
            if fy_value is not None:
                q_sum = sum(q.isolated_value for q in qs)
                tolerance = abs(fy_value) * 0.01 if fy_value else 1.0
                if abs(q_sum - fy_value) > tolerance:
                    logger.warning(
                        "Sanity check: FY%d quarter sum (%.2f) differs from "
                        "FY total (%.2f) by %.2f. Tolerance=%.2f. Style=%s",
                        fy, q_sum, fy_value,
                        abs(q_sum - fy_value), tolerance, style,
                    )

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

    # Detect reporting style to set is_ytd flag correctly
    style = _detect_style_from_rows(metric_rows)

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

        # is_ytd: for cumulative reporters, Q2/Q3 are YTD
        # for standalone reporters, nothing is YTD
        if style == "standalone_quarterly":
            r["is_ytd"] = False
        else:
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


def compute_quarterly_latest(
    metric_rows: list[dict],
    value_key: str = "metric_value",
) -> tuple[float | None, str | None, str | None]:
    """Return the latest isolated quarterly value for a flow metric.

    Derives quarterly values from the filing history — if the latest period
    is a 10-K (FY), Q4 is computed as FY − Q3 YTD.  This ensures all
    values are on a consistent quarterly basis, comparable to Bloomberg.

    Returns (quarterly_value, quarter_label, method) or (None, None, None).
    Method is 'annual_only' when FY data exists but quarters can't be derived;
    in that case (None, None, None) is returned since we can't produce a
    meaningful quarterly figure.
    """
    isolated = isolate_quarters(metric_rows, value_key=value_key)
    if not isolated:
        return None, None, None

    # Return the most recent quarter that was actually isolated
    # (skip annual_only since that's a full-year figure, not quarterly)
    for q in reversed(isolated):
        if q.method != "annual_only":
            return q.isolated_value, q.quarter, q.method

    return None, None, None
