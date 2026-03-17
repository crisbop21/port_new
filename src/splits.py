"""Stock-split detection and normalization for SEC EDGAR metrics.

Detects likely splits by analyzing period-over-period changes in
shares_outstanding and cross-checking against EPS movements.

Decision tree:
    Shares outstanding change > 15% in one period?
        YES → Did EPS move inversely by approximately the same ratio?
            YES → likely split → normalize
        YES → Is the shares ratio close to a common split ratio (2:1, 3:1, etc.)?
            YES → likely split → normalize
        NO  → organic change (buyback / issuance) → leave as-is
"""

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_CHANGE_THRESHOLD = 0.15  # 15% change triggers investigation
_INVERSE_TOLERANCE = 0.10  # EPS inverse ratio must be within 10%

# Common split ratios (and their inverses for reverse splits)
_COMMON_RATIOS = [
    2, 3, 4, 5, 7, 8, 10, 15, 20,  # forward splits
    1 / 2, 1 / 3, 1 / 4, 1 / 5, 1 / 8, 1 / 10, 1 / 15, 1 / 20,  # reverse
    3 / 2, 4 / 3, 5 / 4,  # less common ratios
]
_RATIO_TOLERANCE = 0.08  # how close to a common ratio to count as a match


@dataclass
class DetectedSplit:
    """A detected stock split event."""

    symbol: str
    period_end: date  # period where the jump was observed
    prior_period_end: date
    shares_ratio: float  # new_shares / old_shares (e.g. 2.0 for 2:1 split)
    confidence: str  # "high" (EPS confirmed) or "medium" (ratio match only)
    reason: str  # human-readable explanation


def _is_close_to_common_ratio(ratio: float) -> bool:
    """Check if a shares ratio is close to a known split ratio."""
    for common in _COMMON_RATIOS:
        if abs(ratio - common) / common <= _RATIO_TOLERANCE:
            return True
    return False


def _format_ratio(ratio: float) -> str:
    """Format a split ratio as 'N:1' or '1:N'."""
    if ratio >= 1:
        # Forward split
        rounded = round(ratio)
        if abs(ratio - rounded) < 0.15:
            return f"{rounded}:1"
        return f"{ratio:.2f}:1"
    else:
        inv = round(1 / ratio)
        return f"1:{inv}"


def detect_splits(
    shares_data: list[dict],
    eps_data: list[dict] | None = None,
) -> list[DetectedSplit]:
    """Detect likely stock splits from shares_outstanding history.

    Args:
        shares_data: rows from stock_metrics for shares_outstanding,
                     each with keys: symbol, metric_value, period_end
        eps_data:    optional rows for eps_diluted (or eps_basic),
                     used to confirm splits via inverse movement

    Returns:
        List of DetectedSplit events, sorted by period_end ascending.
    """
    if len(shares_data) < 2:
        return []

    symbol = shares_data[0].get("symbol", "???")

    # Sort chronologically
    sorted_shares = sorted(shares_data, key=lambda r: str(r.get("period_end", "")))

    # Build EPS lookup by period_end if available
    eps_by_period: dict[str, float] = {}
    if eps_data:
        for row in eps_data:
            pe = str(row.get("period_end", ""))
            try:
                eps_by_period[pe] = float(row.get("metric_value", 0))
            except (ValueError, TypeError):
                pass

    splits: list[DetectedSplit] = []

    for i in range(1, len(sorted_shares)):
        prev = sorted_shares[i - 1]
        curr = sorted_shares[i]

        try:
            prev_shares = float(prev.get("metric_value", 0))
            curr_shares = float(curr.get("metric_value", 0))
        except (ValueError, TypeError):
            continue

        if prev_shares <= 0 or curr_shares <= 0:
            continue

        ratio = curr_shares / prev_shares
        pct_change = abs(ratio - 1.0)

        # Step 1: Does shares outstanding change > threshold?
        if pct_change <= _CHANGE_THRESHOLD:
            continue

        prev_end = str(prev.get("period_end", ""))
        curr_end = str(curr.get("period_end", ""))
        confidence = "low"
        reason_parts: list[str] = []
        reason_parts.append(
            f"Shares changed {pct_change:.0%}: "
            f"{prev_shares:,.0f} → {curr_shares:,.0f} "
            f"(ratio {_format_ratio(ratio)})"
        )

        # Step 2: Did EPS move inversely?  (only meaningful if ratio is
        # close to a common split ratio — otherwise a buyback + earnings
        # growth can look like an inverse)
        eps_confirmed = False
        if prev_end in eps_by_period and curr_end in eps_by_period:
            prev_eps = eps_by_period[prev_end]
            curr_eps = eps_by_period[curr_end]
            if prev_eps != 0 and curr_eps != 0:
                eps_ratio = curr_eps / prev_eps
                expected_eps_ratio = 1.0 / ratio
                if expected_eps_ratio != 0:
                    deviation = abs(eps_ratio - expected_eps_ratio) / abs(expected_eps_ratio)
                    if deviation <= _INVERSE_TOLERANCE and _is_close_to_common_ratio(ratio):
                        eps_confirmed = True
                        confidence = "high"
                        reason_parts.append(
                            f"EPS moved inversely: {prev_eps:.2f} → {curr_eps:.2f} "
                            f"(ratio {eps_ratio:.2f}, expected {expected_eps_ratio:.2f})"
                        )

        # Step 3: Is it a common split ratio?
        ratio_match = _is_close_to_common_ratio(ratio)
        if ratio_match:
            if confidence != "high":
                confidence = "medium"
            reason_parts.append(f"Ratio {_format_ratio(ratio)} matches common split pattern")

        # Only flag as split if EPS confirmed OR ratio matches
        if not eps_confirmed and not ratio_match:
            logger.info(
                "%s: shares changed %.0f%% at %s but no split signals — "
                "treating as organic (buyback/issuance)",
                symbol, pct_change * 100, curr_end,
            )
            continue

        try:
            period_end = date.fromisoformat(curr_end)
            prior_period_end = date.fromisoformat(prev_end)
        except ValueError:
            continue

        split = DetectedSplit(
            symbol=symbol,
            period_end=period_end,
            prior_period_end=prior_period_end,
            shares_ratio=ratio,
            confidence=confidence,
            reason="; ".join(reason_parts),
        )
        splits.append(split)
        logger.info(
            "%s: detected likely %s split at %s — %s (confidence=%s)",
            symbol, _format_ratio(ratio), curr_end, split.reason, confidence,
        )

    return splits


# Metrics that are per-share (divide by ratio to normalize old values)
SPLIT_AFFECTED_PER_SHARE = {"eps_basic", "eps_diluted"}
# Metrics that are share-count (multiply by ratio to normalize old values)
SPLIT_AFFECTED_SHARE_COUNT = {"shares_outstanding"}
# All other metrics (revenue, net_income, etc.) are unaffected by splits


def _compute_split_factor(
    period: str,
    sorted_splits: list[DetectedSplit],
) -> float:
    """Cumulative split factor for a period — product of all ratios after it."""
    factor = 1.0
    for s in sorted_splits:
        if period < str(s.period_end):
            factor *= s.shares_ratio
    return factor


def normalize_metrics(
    metrics: list[dict],
    splits: list[DetectedSplit],
    metric_name: str,
) -> list[dict]:
    """Normalize historical metric values to account for detected splits.

    For per-share metrics (EPS), pre-split values are divided by the
    cumulative split ratio.  For share-count metrics, pre-split values
    are multiplied by the cumulative ratio.

    Args:
        metrics:     rows from stock_metrics, each with period_end + metric_value
        splits:      detected splits (from detect_splits)
        metric_name: which metric this is (determines multiply vs divide)

    Returns:
        New list of dicts with an added 'normalized_value' key and
        'split_adjusted' boolean flag.
    """
    if not splits or not metrics:
        result = []
        for row in metrics:
            r = dict(row)
            r["normalized_value"] = r.get("metric_value")
            r["split_adjusted"] = False
            result.append(r)
        return result

    sorted_splits = sorted(splits, key=lambda s: s.period_end)

    result = []
    for row in metrics:
        r = dict(row)
        period = str(r.get("period_end", ""))
        factor = _compute_split_factor(period, sorted_splits)

        adjusted = False
        raw_val = r.get("metric_value")

        if factor != 1.0 and raw_val is not None:
            try:
                val = float(raw_val)
                if metric_name in SPLIT_AFFECTED_PER_SHARE:
                    r["normalized_value"] = round(val / factor, 4)
                    adjusted = True
                elif metric_name in SPLIT_AFFECTED_SHARE_COUNT:
                    r["normalized_value"] = round(val * factor, 0)
                    adjusted = True
                else:
                    r["normalized_value"] = raw_val
            except (ValueError, TypeError):
                r["normalized_value"] = raw_val
        else:
            r["normalized_value"] = raw_val

        r["split_adjusted"] = adjusted
        result.append(r)

    return result


def normalize_latest_value(
    metric_name: str,
    metric_value: float,
    period_end: str,
    splits: list[DetectedSplit],
) -> tuple[float, bool]:
    """Normalize a single latest metric value for splits.

    Returns (adjusted_value, was_adjusted).
    """
    if not splits:
        return metric_value, False

    sorted_splits = sorted(splits, key=lambda s: s.period_end)
    factor = _compute_split_factor(period_end, sorted_splits)

    if factor == 1.0:
        return metric_value, False

    if metric_name in SPLIT_AFFECTED_PER_SHARE:
        return round(metric_value / factor, 4), True
    elif metric_name in SPLIT_AFFECTED_SHARE_COUNT:
        return round(metric_value * factor, 0), True

    return metric_value, False


def normalize_symbol_data(
    all_metric_rows: dict[str, list[dict]],
    splits: list[DetectedSplit],
) -> dict[str, list[dict]]:
    """Normalize ALL metric histories for a symbol in one pass.

    Args:
        all_metric_rows: dict mapping metric_name → list of DB rows
        splits:          detected splits for this symbol

    Returns:
        Same structure with normalized_value and split_adjusted added.
    """
    result: dict[str, list[dict]] = {}
    for metric_name, rows in all_metric_rows.items():
        result[metric_name] = normalize_metrics(rows, splits, metric_name)
    return result
