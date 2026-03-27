"""Valuation engine — computes fundamental ratios, historical percentiles,
and composite scores from SEC EDGAR metrics + daily prices.

No external API calls — everything derived from data already in Supabase.
"""

import logging
import math
from datetime import date
from typing import Any

import numpy as np

from src.ttm import compute_ttm, is_flow_metric

logger = logging.getLogger(__name__)


# ── Ratio computation ────────────────────────────────────────────────────────

def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    """Divide, returning None on missing data or zero/negative denominator."""
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def _get_metric_value(
    metrics: dict[str, dict],
    name: str,
) -> float | None:
    """Extract a float value from the latest-metrics dict."""
    row = metrics.get(name)
    if row is None:
        return None
    try:
        return float(row["metric_value"])
    except (KeyError, TypeError, ValueError):
        return None


def compute_ratios(
    metrics: dict[str, dict],
    latest_price: float,
    ttm_metrics: dict[str, float | None] | None = None,
) -> dict[str, float | None]:
    """Compute all fundamental ratios for a single symbol.

    Args:
        metrics: latest stock_metrics keyed by metric_name
        latest_price: most recent close price
        ttm_metrics: optional TTM-adjusted values for flow metrics.
                     Keys are metric names, values are TTM floats.
                     Falls back to raw metric_value if not provided.

    Returns dict of ratio_name → value (None if not computable).
    """
    ttm = ttm_metrics or {}

    def _flow(name: str) -> float | None:
        """Get TTM value if available, else raw."""
        if name in ttm and ttm[name] is not None:
            return ttm[name]
        return _get_metric_value(metrics, name)

    def _point(name: str) -> float | None:
        return _get_metric_value(metrics, name)

    # Core inputs
    shares = _point("shares_outstanding")
    equity = _point("stockholders_equity")
    total_assets = _point("total_assets")
    total_liabilities = _point("total_liabilities")
    cash = _point("cash_and_equivalents")
    current_assets = _point("current_assets")
    current_liabilities = _point("current_liabilities")
    long_term_debt = _point("long_term_debt")

    revenue = _flow("revenue")
    net_income = _flow("net_income")
    operating_income = _flow("operating_income")
    gross_profit = _flow("gross_profit")
    capex = _flow("capital_expenditures")
    dividends = _flow("dividends_paid")
    interest_expense = _flow("interest_expense")
    eps = _flow("eps_diluted") or _flow("eps_basic")

    # Derived
    market_cap = latest_price * shares if shares else None
    enterprise_value = None
    if market_cap is not None and total_liabilities is not None and cash is not None:
        enterprise_value = market_cap + total_liabilities - cash

    fcf = None
    if operating_income is not None and capex is not None:
        # capex is reported as positive (payments), so subtract
        fcf = operating_income - abs(capex)

    ratios: dict[str, float | None] = {}

    # ── Valuation ────────────────────────────────────────────────────────
    ratios["market_cap"] = market_cap
    ratios["enterprise_value"] = enterprise_value
    ratios["pe_ttm"] = _safe_div(latest_price, eps) if eps and eps > 0 else None
    ratios["pb"] = _safe_div(market_cap, equity) if equity and equity > 0 else None
    ratios["ps"] = _safe_div(market_cap, revenue) if revenue and revenue > 0 else None
    ratios["ev_ebitda"] = _safe_div(enterprise_value, operating_income) if operating_income and operating_income > 0 else None
    ratios["ev_revenue"] = _safe_div(enterprise_value, revenue) if revenue and revenue > 0 else None
    ratios["earnings_yield"] = _safe_div(net_income, market_cap)
    ratios["fcf_yield"] = _safe_div(fcf, market_cap)

    # ── Profitability ────────────────────────────────────────────────────
    ratios["gross_margin"] = _safe_div(gross_profit, revenue)
    ratios["operating_margin"] = _safe_div(operating_income, revenue)
    ratios["net_margin"] = _safe_div(net_income, revenue)
    ratios["roe"] = _safe_div(net_income, equity)
    ratios["roa"] = _safe_div(net_income, total_assets)

    # ── Financial health ─────────────────────────────────────────────────
    ratios["debt_to_equity"] = _safe_div(total_liabilities, equity)
    ratios["current_ratio"] = _safe_div(current_assets, current_liabilities)
    ratios["interest_coverage"] = _safe_div(operating_income, abs(interest_expense)) if interest_expense else None
    ratios["cash_to_assets"] = _safe_div(cash, total_assets)

    # ── Dividend ─────────────────────────────────────────────────────────
    div_per_share = _safe_div(abs(dividends) if dividends else None, shares)
    ratios["dividend_yield"] = _safe_div(div_per_share, latest_price)
    ratios["payout_ratio"] = _safe_div(abs(dividends) if dividends else None, net_income) if net_income and net_income > 0 else None

    return ratios


# ── Growth computation ───────────────────────────────────────────────────────


def compute_growth(
    metric_history: list[dict],
    metric_name: str,
    lookback_years: int = 1,
) -> float | None:
    """Compute growth rate for a metric over a lookback period.

    For lookback_years=1: simple YoY growth.
    For lookback_years>1: CAGR.

    metric_history: rows from stock_metrics for a single symbol+metric,
                    sorted by period_end descending.
    """
    if not metric_history or len(metric_history) < 2:
        return None

    # Filter to annual (FY) or use TTM periods
    annual = [r for r in metric_history if r.get("fiscal_period") == "FY"]
    if len(annual) < 2:
        return None

    # Sort by period_end descending
    annual.sort(key=lambda r: str(r.get("period_end", "")), reverse=True)

    latest = annual[0]
    # Find the row ~lookback_years ago
    target_year = date.fromisoformat(str(latest["period_end"])).year - lookback_years
    older = None
    for row in annual[1:]:
        row_year = date.fromisoformat(str(row["period_end"])).year
        if row_year <= target_year:
            older = row
            break

    if older is None:
        return None

    try:
        v_new = float(latest["metric_value"])
        v_old = float(older["metric_value"])
    except (TypeError, ValueError):
        return None

    if v_old == 0 or v_old < 0:
        return None

    if lookback_years == 1:
        return (v_new - v_old) / abs(v_old)
    else:
        # CAGR
        actual_years = (
            date.fromisoformat(str(latest["period_end"]))
            - date.fromisoformat(str(older["period_end"]))
        ).days / 365.25
        if actual_years <= 0:
            return None
        if v_new <= 0:
            return None
        return (v_new / v_old) ** (1 / actual_years) - 1


def compute_peg(
    pe: float | None,
    eps_growth: float | None,
) -> float | None:
    """PEG ratio using historical EPS growth."""
    if pe is None or eps_growth is None:
        return None
    # Convert growth to percentage (PEG convention: growth as whole number)
    growth_pct = eps_growth * 100
    if growth_pct <= 0:
        return None
    return pe / growth_pct


# ── Historical percentile engine ─────────────────────────────────────────────


def compute_historical_ratios(
    metric_history: dict[str, list[dict]],
    price_history: list[dict],
) -> list[dict]:
    """Build a *daily* time series of valuation ratios from metrics + prices.

    For each trading day, pairs the day's price with the most recent
    fundamental data (TTM for flow metrics, point-in-time for balance sheet).
    This produces one observation per trading day, not just per filing date.

    Args:
        metric_history: {metric_name: [rows sorted by period_end asc]}
        price_history: daily_prices rows sorted by price_date asc

    Returns list of dicts with period_end (= price_date), price, and ratio
    values for each day where we have both fundamentals and a price.
    """
    if not price_history or not metric_history:
        return []

    # Build sorted daily price list: [(date_str, price), ...]
    daily_prices: list[tuple[str, float]] = []
    for row in price_history:
        try:
            daily_prices.append((str(row["price_date"]), float(row["adj_close"])))
        except (KeyError, TypeError, ValueError):
            continue
    daily_prices.sort(key=lambda x: x[0])
    if not daily_prices:
        return []

    # Pre-compute TTM time series for flow metrics so historical ratios
    # use annualized values (not raw quarterly figures).
    _FLOW_RATIO_METRICS = ("eps_diluted", "eps_basic", "revenue",
                           "net_income", "operating_income")
    ttm_by_metric: dict[str, list[tuple[str, float]]] = {}
    for name in _FLOW_RATIO_METRICS:
        rows = metric_history.get(name, [])
        if not rows:
            continue
        ttm_rows = compute_ttm(rows)
        entries: list[tuple[str, float]] = []
        for r in ttm_rows:
            pe = str(r.get("period_end", ""))
            ttm_val = r.get("ttm_value")
            if pe and ttm_val is not None:
                entries.append((pe, ttm_val))
        if entries:
            entries.sort(key=lambda x: x[0])
            ttm_by_metric[name] = entries

    # Build sorted time series for point-in-time metrics: [(date, value), ...]
    _POINT_METRICS = ("shares_outstanding", "stockholders_equity", "total_assets",
                      "total_liabilities", "cash_and_equivalents")
    point_by_metric: dict[str, list[tuple[str, float]]] = {}
    for name in _POINT_METRICS:
        rows = metric_history.get(name, [])
        entries = []
        for row in rows:
            pe = str(row.get("period_end", ""))
            try:
                val = float(row["metric_value"])
            except (KeyError, TypeError, ValueError):
                continue
            if pe:
                entries.append((pe, val))
        if entries:
            entries.sort(key=lambda x: x[0])
            point_by_metric[name] = entries

    # Fallback: derive shares_outstanding from net_income / eps_diluted
    # when shares_outstanding data is missing (common for multi-class stocks
    # like META where XBRL may not report a single aggregate figure).
    if "shares_outstanding" not in point_by_metric:
        ni_rows = metric_history.get("net_income", [])
        eps_rows = metric_history.get("eps_diluted", [])
        if ni_rows and eps_rows:
            # Build lookup: period_end → eps_diluted (FY entries only)
            eps_by_pe: dict[str, float] = {}
            for row in eps_rows:
                if row.get("fiscal_period") != "FY":
                    continue
                pe = str(row.get("period_end", ""))
                try:
                    val = float(row["metric_value"])
                except (KeyError, TypeError, ValueError):
                    continue
                if pe and val and val != 0:
                    eps_by_pe[pe] = val

            derived_shares: list[tuple[str, float]] = []
            for row in ni_rows:
                if row.get("fiscal_period") != "FY":
                    continue
                pe = str(row.get("period_end", ""))
                try:
                    ni_val = float(row["metric_value"])
                except (KeyError, TypeError, ValueError):
                    continue
                eps_val = eps_by_pe.get(pe)
                if pe and eps_val and ni_val:
                    shares = ni_val / eps_val
                    if shares > 0:
                        derived_shares.append((pe, shares))

            if derived_shares:
                derived_shares.sort(key=lambda x: x[0])
                point_by_metric["shares_outstanding"] = derived_shares
                logger.info(
                    "Derived shares_outstanding from net_income/eps_diluted "
                    "(%d data points)", len(derived_shares),
                )

    def _latest_on_or_before(
        series: list[tuple[str, float]], target: str,
    ) -> float | None:
        """Binary search for the most recent value on or before target date."""
        if not series:
            return None
        idx = _bisect_right_tuples(series, target) - 1
        if idx < 0:
            return None
        return series[idx][1]

    # Iterate over every trading day
    results = []
    for price_date, price in daily_prices:
        shares = _latest_on_or_before(point_by_metric.get("shares_outstanding", []), price_date)
        if shares is None or shares <= 0:
            continue

        market_cap = price * shares
        equity = _latest_on_or_before(point_by_metric.get("stockholders_equity", []), price_date)
        total_liabilities = _latest_on_or_before(point_by_metric.get("total_liabilities", []), price_date)
        cash = _latest_on_or_before(point_by_metric.get("cash_and_equivalents", []), price_date)

        # TTM flow metrics
        eps = (_latest_on_or_before(ttm_by_metric.get("eps_diluted", []), price_date)
               or _latest_on_or_before(ttm_by_metric.get("eps_basic", []), price_date))
        revenue = _latest_on_or_before(ttm_by_metric.get("revenue", []), price_date)
        operating_income = _latest_on_or_before(ttm_by_metric.get("operating_income", []), price_date)

        ev = None
        if total_liabilities is not None and cash is not None:
            ev = market_cap + total_liabilities - cash

        result_row: dict[str, Any] = {"period_end": price_date, "price": price}
        result_row["pe_ttm"] = price / eps if eps and eps > 0 else None
        result_row["pb"] = market_cap / equity if equity and equity > 0 else None
        result_row["ps"] = market_cap / revenue if revenue and revenue > 0 else None
        result_row["ev_ebitda"] = ev / operating_income if ev and operating_income and operating_income > 0 else None
        results.append(result_row)

    return results


def _bisect_right(sorted_list: list[str], target: str) -> int:
    """Binary search for insertion point (right)."""
    lo, hi = 0, len(sorted_list)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_list[mid] <= target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _bisect_right_tuples(sorted_tuples: list[tuple[str, float]], target: str) -> int:
    """Binary search for insertion point (right) on a list of (date, value) tuples."""
    lo, hi = 0, len(sorted_tuples)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_tuples[mid][0] <= target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def compute_percentile(
    current_value: float | None,
    historical_values: list[float],
) -> float | None:
    """Compute percentile rank of current_value within historical values.

    Returns 0-100 (0 = lowest ever, 100 = highest ever).
    """
    if current_value is None or not historical_values:
        return None
    values = [v for v in historical_values if v is not None and not math.isnan(v)]
    if len(values) < 4:
        return None
    count_below = sum(1 for v in values if v < current_value)
    return (count_below / len(values)) * 100


# ── Fundamental score ────────────────────────────────────────────────────────

SCORE_PRESETS: dict[str, dict[str, float]] = {
    "Balanced": {"valuation": 0.30, "profitability": 0.25, "health": 0.20, "growth": 0.25},
    "Value":    {"valuation": 0.45, "profitability": 0.20, "health": 0.20, "growth": 0.15},
    "Growth":   {"valuation": 0.15, "profitability": 0.20, "health": 0.15, "growth": 0.50},
    "Quality":  {"valuation": 0.15, "profitability": 0.40, "health": 0.30, "growth": 0.15},
}

# Ratio scoring config: (higher_is_better, min_good, max_good)
# For "higher is better" metrics, higher raw value = higher score.
# For "lower is better" (like P/E), lower raw value = higher score.
_RATIO_SCORING: dict[str, tuple[str, bool]] = {
    # Valuation — scored via percentile (inverted: low percentile = high score)
    "pe_ttm": ("valuation", False),
    "pb": ("valuation", False),
    "ps": ("valuation", False),
    "ev_ebitda": ("valuation", False),
    # Profitability — absolute, higher is better
    "gross_margin": ("profitability", True),
    "operating_margin": ("profitability", True),
    "net_margin": ("profitability", True),
    "roe": ("profitability", True),
    "roa": ("profitability", True),
    # Health
    "current_ratio": ("health", True),
    "interest_coverage": ("health", True),
    "cash_to_assets": ("health", True),
    "debt_to_equity": ("health", False),
    # Growth — higher is better
    "revenue_growth": ("growth", True),
    "eps_growth": ("growth", True),
    "net_income_growth": ("growth", True),
}


def _score_absolute(value: float | None, higher_is_better: bool) -> float | None:
    """Convert a raw ratio to a 0-100 score using a sigmoid-like mapping.

    This produces a smooth score that avoids extreme clipping.
    """
    if value is None:
        return None

    if higher_is_better:
        # Map: 0% → 50, 10% → 65, 25% → 80, 50%+ → 95
        # For ratios like margins, ROE expressed as decimals (0.10 = 10%)
        score = 50 + 50 * (2 / (1 + math.exp(-8 * value)) - 1)
    else:
        # Lower is better (e.g., debt_to_equity)
        # D/E of 0 → 90, 1 → 50, 3+ → 10
        score = 100 - 50 * (2 / (1 + math.exp(-1.5 * value)) - 1)

    return max(0.0, min(100.0, score))


def _score_percentile_inverted(percentile: float | None) -> float | None:
    """For valuation: low percentile (cheap vs history) = high score."""
    if percentile is None:
        return None
    return max(0.0, min(100.0, 100.0 - percentile))


def compute_fundamental_score(
    ratios: dict[str, float | None],
    percentiles: dict[str, float | None],
    growth: dict[str, float | None],
    preset: str = "Balanced",
) -> tuple[float | None, dict[str, float | None]]:
    """Compute composite fundamental score (0-100).

    Args:
        ratios: from compute_ratios()
        percentiles: historical percentile for valuation ratios
        growth: {"revenue_growth": 0.15, "eps_growth": 0.10, ...}
        preset: weight preset name

    Returns:
        (composite_score, category_scores) where category_scores has
        keys: valuation, profitability, health, growth
    """
    weights = SCORE_PRESETS.get(preset, SCORE_PRESETS["Balanced"])

    # Merge ratios and growth into one dict for scoring
    all_values = {**ratios, **growth}

    # Score each ratio
    category_scores_raw: dict[str, list[float]] = {
        "valuation": [],
        "profitability": [],
        "health": [],
        "growth": [],
    }

    for ratio_name, (category, higher_is_better) in _RATIO_SCORING.items():
        if category == "valuation":
            # Use percentile-based scoring for valuation
            score = _score_percentile_inverted(percentiles.get(ratio_name))
        else:
            score = _score_absolute(all_values.get(ratio_name), higher_is_better)

        if score is not None:
            category_scores_raw[category].append(score)

    # Average within each category
    category_scores: dict[str, float | None] = {}
    for cat in category_scores_raw:
        values = category_scores_raw[cat]
        if values:
            category_scores[cat] = sum(values) / len(values)
        else:
            category_scores[cat] = None

    # Weighted composite
    total_weight = 0.0
    weighted_sum = 0.0
    for cat, weight in weights.items():
        if category_scores.get(cat) is not None:
            weighted_sum += category_scores[cat] * weight
            total_weight += weight

    composite = weighted_sum / total_weight if total_weight > 0 else None

    return composite, category_scores


# ── Portfolio-level stats ────────────────────────────────────────────────────


def compute_portfolio_stats(
    holdings: list[dict],
    ratios_by_symbol: dict[str, dict[str, float | None]],
) -> dict[str, float | None]:
    """Compute portfolio-level aggregated statistics.

    Args:
        holdings: position rows with symbol, market_value, cost_basis
        ratios_by_symbol: {symbol: ratios_dict}

    Returns dict with portfolio-level metrics.
    """
    stats: dict[str, float | None] = {}

    # Filter to symbols that have ratios
    valid = [
        h for h in holdings
        if h.get("symbol") in ratios_by_symbol
        and h.get("asset_class") in ("STK", "ETF")
    ]

    if not valid:
        return stats

    total_value = sum(abs(float(h.get("market_value", 0))) for h in valid)
    if total_value == 0:
        return stats

    # Weighted averages
    for ratio_name in ("pe_ttm", "pb", "ps", "earnings_yield", "dividend_yield"):
        weighted = 0.0
        weight_sum = 0.0
        for h in valid:
            sym = h["symbol"]
            val = ratios_by_symbol.get(sym, {}).get(ratio_name)
            mv = abs(float(h.get("market_value", 0)))
            if val is not None and not math.isnan(val) and not math.isinf(val):
                weighted += val * mv
                weight_sum += mv
        stats[f"weighted_{ratio_name}"] = weighted / weight_sum if weight_sum > 0 else None

    # Earnings yield on cost
    total_cost = sum(abs(float(h.get("cost_basis", 0))) for h in valid)
    if total_cost > 0:
        total_earnings = 0.0
        for h in valid:
            sym = h["symbol"]
            ey = ratios_by_symbol.get(sym, {}).get("earnings_yield")
            mv = abs(float(h.get("market_value", 0)))
            if ey is not None:
                total_earnings += ey * mv
        stats["earnings_yield_on_cost"] = total_earnings / total_cost if total_cost > 0 else None

    # Herfindahl concentration index (0-10000, lower = more diversified)
    shares_pct = [(abs(float(h.get("market_value", 0))) / total_value * 100) for h in valid]
    stats["herfindahl"] = sum(p ** 2 for p in shares_pct)
    stats["top_holding_pct"] = max(shares_pct) if shares_pct else None
    stats["num_holdings"] = len(valid)

    return stats
