"""Asset ranking engine — scores portfolio holdings 1–10 on technicals + fundamentals.

Technical indicators (from daily_prices):
  - RSI-14
  - Price vs 50-day SMA (trend)
  - Price vs 200-day SMA (trend)
  - 30-day price change %
  - 30-day volatility (annualised std dev)

Fundamental indicators (from stock_metrics via SEC EDGAR):
  - P/E ratio (price / EPS diluted)
  - Profit margin (net income / revenue)
  - Revenue growth (YoY)
  - Debt-to-equity (total liabilities / stockholders equity)
  - Operating margin (operating income / revenue)

Each sub-score maps to 1–10 via percentile-style buckets.
Final composite = weighted average of all available sub-scores, rounded to nearest int.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal

logger = logging.getLogger(__name__)


# ── Sub-score helpers ────────────────────────────────────────────────────────


def _clamp(val: float, lo: float = 1.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, val))


def _linear_score(val: float, worst: float, best: float) -> float:
    """Map val linearly from [worst, best] → [1, 10]. Values outside are clamped."""
    if best == worst:
        return 5.5
    score = 1.0 + 9.0 * (val - worst) / (best - worst)
    return _clamp(score)


def _inverse_score(val: float, best: float, worst: float) -> float:
    """Higher val is worse (e.g. debt-to-equity). Maps [best, worst] → [10, 1]."""
    return _linear_score(val, worst, best)


# ── Technical indicators ─────────────────────────────────────────────────────


def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    """Compute RSI from a list of closing prices (oldest first)."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    # Use exponential moving average (Wilder's smoothing)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_sma(closes: list[float], period: int) -> float | None:
    """Simple moving average of the last `period` closes."""
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def compute_volatility(closes: list[float], period: int = 30) -> float | None:
    """Annualised volatility from daily log returns over `period` days."""
    if len(closes) < period + 1:
        return None
    import math
    returns = []
    recent = closes[-(period + 1):]
    for i in range(1, len(recent)):
        if recent[i - 1] > 0:
            returns.append(math.log(recent[i] / recent[i - 1]))
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(var) * math.sqrt(252) * 100  # annualised %


def compute_price_change_pct(closes: list[float], period: int = 30) -> float | None:
    """Percentage change over the last `period` trading days."""
    if len(closes) < period + 1:
        return None
    old = closes[-(period + 1)]
    if old == 0:
        return None
    return ((closes[-1] - old) / old) * 100


# ── Score builders ───────────────────────────────────────────────────────────


@dataclass
class TechnicalScores:
    rsi: float | None = None
    rsi_score: float | None = None
    sma50_pct: float | None = None       # price vs SMA50 (% above/below)
    sma50_score: float | None = None
    sma200_pct: float | None = None
    sma200_score: float | None = None
    momentum_30d: float | None = None    # 30-day price change %
    momentum_score: float | None = None
    volatility: float | None = None      # annualised %
    volatility_score: float | None = None
    composite: float | None = None       # weighted average 1-10


def score_technicals(closes: list[float]) -> TechnicalScores:
    """Compute technical sub-scores from a list of daily closes (oldest first)."""
    t = TechnicalScores()
    if len(closes) < 15:
        return t

    latest = closes[-1]

    # RSI: 30-70 is neutral. < 30 oversold (contrarian bullish=7), > 70 overbought (bearish=3)
    # Sweet spot around 50-60 scores highest.
    t.rsi = compute_rsi(closes)
    if t.rsi is not None:
        # Score: 50 → 10, 30 → 6, 70 → 6, 0 → 3, 100 → 1
        if t.rsi <= 50:
            t.rsi_score = _linear_score(t.rsi, 0, 50)
        else:
            t.rsi_score = _linear_score(t.rsi, 100, 50)

    # Price vs SMA50: positive = above trend (bullish)
    sma50 = compute_sma(closes, 50)
    if sma50 is not None and sma50 > 0:
        t.sma50_pct = ((latest - sma50) / sma50) * 100
        t.sma50_score = _linear_score(t.sma50_pct, -15, 15)

    # Price vs SMA200: positive = above long-term trend
    sma200 = compute_sma(closes, 200)
    if sma200 is not None and sma200 > 0:
        t.sma200_pct = ((latest - sma200) / sma200) * 100
        t.sma200_score = _linear_score(t.sma200_pct, -25, 25)

    # 30-day momentum
    t.momentum_30d = compute_price_change_pct(closes, 30)
    if t.momentum_30d is not None:
        t.momentum_score = _linear_score(t.momentum_30d, -20, 20)

    # Volatility: lower is better for stability
    t.volatility = compute_volatility(closes, 30)
    if t.volatility is not None:
        t.volatility_score = _inverse_score(t.volatility, 10, 60)

    # Composite: weighted average of available scores
    weights = {
        "rsi": (t.rsi_score, 2.0),
        "sma50": (t.sma50_score, 2.0),
        "sma200": (t.sma200_score, 2.0),
        "momentum": (t.momentum_score, 2.0),
        "volatility": (t.volatility_score, 1.0),
    }
    total_w, total_s = 0.0, 0.0
    for _, (score, w) in weights.items():
        if score is not None:
            total_w += w
            total_s += score * w
    if total_w > 0:
        t.composite = round(total_s / total_w, 1)
    return t


@dataclass
class FundamentalScores:
    pe_ratio: float | None = None
    pe_score: float | None = None
    profit_margin: float | None = None    # %
    margin_score: float | None = None
    revenue_growth: float | None = None   # YoY %
    growth_score: float | None = None
    debt_to_equity: float | None = None
    de_score: float | None = None
    operating_margin: float | None = None  # %
    op_margin_score: float | None = None
    composite: float | None = None


def score_fundamentals(
    latest_metrics: dict[str, dict],
    current_price: float | None = None,
    prior_revenue: float | None = None,
) -> FundamentalScores:
    """Compute fundamental sub-scores from latest SEC EDGAR metrics.

    Args:
        latest_metrics: dict keyed by metric_name → row dict with 'metric_value'.
        current_price: latest stock price (for P/E calculation).
        prior_revenue: revenue from one year ago (for YoY growth).
    """
    f = FundamentalScores()

    def _val(metric_name: str) -> float | None:
        row = latest_metrics.get(metric_name)
        if row is None:
            return None
        v = row.get("metric_value")
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    eps = _val("eps_diluted")
    revenue = _val("revenue")
    net_income = _val("net_income")
    operating_income = _val("operating_income")
    total_liabilities = _val("total_liabilities")
    stockholders_equity = _val("stockholders_equity")

    # P/E ratio: lower is better (value), but negative means losses
    if current_price is not None and eps is not None and eps > 0:
        f.pe_ratio = current_price / eps
        # Score: P/E 5→10 (deep value), P/E 15→7 (fair), P/E 35→3, P/E 60+→1
        f.pe_score = _inverse_score(f.pe_ratio, 5, 60)

    # Profit margin: higher is better
    if net_income is not None and revenue is not None and revenue != 0:
        f.profit_margin = (net_income / revenue) * 100
        f.margin_score = _linear_score(f.profit_margin, -10, 30)

    # Revenue growth YoY: higher is better
    if prior_revenue is not None and revenue is not None and prior_revenue != 0:
        f.revenue_growth = ((revenue - prior_revenue) / abs(prior_revenue)) * 100
        f.growth_score = _linear_score(f.revenue_growth, -20, 40)

    # Debt-to-equity: lower is better
    if total_liabilities is not None and stockholders_equity is not None and stockholders_equity > 0:
        f.debt_to_equity = total_liabilities / stockholders_equity
        f.de_score = _inverse_score(f.debt_to_equity, 0.3, 4.0)

    # Operating margin: higher is better
    if operating_income is not None and revenue is not None and revenue != 0:
        f.operating_margin = (operating_income / revenue) * 100
        f.op_margin_score = _linear_score(f.operating_margin, -5, 35)

    # Composite
    weights = {
        "pe": (f.pe_score, 2.5),
        "margin": (f.margin_score, 2.0),
        "growth": (f.growth_score, 2.0),
        "de": (f.de_score, 1.5),
        "op_margin": (f.op_margin_score, 2.0),
    }
    total_w, total_s = 0.0, 0.0
    for _, (score, w) in weights.items():
        if score is not None:
            total_w += w
            total_s += score * w
    if total_w > 0:
        f.composite = round(total_s / total_w, 1)
    return f


@dataclass
class AssetRanking:
    symbol: str
    technical: TechnicalScores = field(default_factory=TechnicalScores)
    fundamental: FundamentalScores = field(default_factory=FundamentalScores)
    overall_score: float | None = None   # 1-10
    overall_rank: int | None = None      # 1 = best in portfolio

    @property
    def overall_rounded(self) -> int | None:
        return round(self.overall_score) if self.overall_score is not None else None


def compute_overall(tech: TechnicalScores, fund: FundamentalScores) -> float | None:
    """Weighted blend of technical (40%) and fundamental (60%) composites."""
    if tech.composite is not None and fund.composite is not None:
        return round(0.4 * tech.composite + 0.6 * fund.composite, 1)
    if tech.composite is not None:
        return tech.composite
    if fund.composite is not None:
        return fund.composite
    return None


def rank_assets(rankings: list[AssetRanking]) -> list[AssetRanking]:
    """Sort by overall_score descending and assign rank numbers."""
    scored = [r for r in rankings if r.overall_score is not None]
    unscored = [r for r in rankings if r.overall_score is None]
    scored.sort(key=lambda r: r.overall_score, reverse=True)
    for i, r in enumerate(scored, 1):
        r.overall_rank = i
    return scored + unscored
