"""Portfolio beta calculation vs market benchmarks (SPY, QQQ).

Computes per-symbol beta, portfolio beta (weighted average by market value),
and dollar beta (beta * market_value) for risk analysis.

Beta = Cov(stock_returns, benchmark_returns) / Var(benchmark_returns)
using daily log returns over a configurable lookback window.

Option beta uses Black-Scholes delta for leverage adjustment:
  Option Beta = Underlying Beta × Delta × (Underlying Price / Option Price)
"""

import logging
import math

import numpy as np
import pandas as pd
from scipy.stats import norm

logger = logging.getLogger(__name__)

BENCHMARKS = {"SPY", "QQQ"}

DEFAULT_LOOKBACK_DAYS = 252
DEFAULT_MIN_PERIODS = 60
DEFAULT_RISK_FREE_RATE = 0.045  # ~4.5% US Treasury


def compute_beta(
    stock_prices: list[dict],
    benchmark_prices: list[dict],
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_periods: int = DEFAULT_MIN_PERIODS,
) -> float | None:
    """Compute the beta of a stock vs a benchmark from daily price rows.

    Args:
        stock_prices: List of daily price dicts with 'price_date' and 'adj_close'.
        benchmark_prices: List of daily price dicts with 'price_date' and 'adj_close'.
        lookback_days: Number of trading days to use (default 252 ~ 1 year).
        min_periods: Minimum overlapping data points required.

    Returns:
        Beta coefficient, or None if insufficient data.
    """
    if not stock_prices or not benchmark_prices:
        return None

    stock_df = pd.DataFrame(stock_prices)[["price_date", "adj_close"]].copy()
    bench_df = pd.DataFrame(benchmark_prices)[["price_date", "adj_close"]].copy()

    stock_df["adj_close"] = pd.to_numeric(stock_df["adj_close"], errors="coerce")
    bench_df["adj_close"] = pd.to_numeric(bench_df["adj_close"], errors="coerce")

    stock_df = stock_df.rename(columns={"adj_close": "stock"})
    bench_df = bench_df.rename(columns={"adj_close": "bench"})

    merged = pd.merge(stock_df, bench_df, on="price_date", how="inner")
    merged = merged.sort_values("price_date").reset_index(drop=True)

    # Use only the most recent lookback_days
    if len(merged) > lookback_days:
        merged = merged.tail(lookback_days).reset_index(drop=True)

    if len(merged) < min_periods:
        return None

    # Daily log returns
    stock_ret = np.log(merged["stock"] / merged["stock"].shift(1)).dropna()
    bench_ret = np.log(merged["bench"] / merged["bench"].shift(1)).dropna()

    # Align after dropping NaN
    common_idx = stock_ret.index.intersection(bench_ret.index)
    stock_ret = stock_ret.loc[common_idx]
    bench_ret = bench_ret.loc[common_idx]

    if len(stock_ret) < min_periods - 1:
        return None

    bench_var = bench_ret.var()
    if bench_var == 0 or np.isnan(bench_var):
        return None

    cov = stock_ret.cov(bench_ret)
    beta = cov / bench_var

    return float(beta)


def compute_portfolio_beta(
    holdings: dict[str, dict],
    benchmark_prices: list[dict],
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_periods: int = DEFAULT_MIN_PERIODS,
) -> dict:
    """Compute portfolio-level beta metrics.

    Args:
        holdings: {symbol: {"market_value": float, "prices": list[dict]}}
        benchmark_prices: Benchmark daily price rows.
        lookback_days: Lookback window in trading days.
        min_periods: Minimum data points required per symbol.

    Returns:
        Dict with keys:
            betas: {symbol: beta or None}
            dollar_betas: {symbol: dollar_beta or None}
            portfolio_beta: weighted average beta (or None)
            portfolio_dollar_beta: sum of dollar betas
    """
    betas: dict[str, float | None] = {}
    dollar_betas: dict[str, float | None] = {}

    for symbol, data in holdings.items():
        beta = compute_beta(
            data["prices"],
            benchmark_prices,
            lookback_days=lookback_days,
            min_periods=min_periods,
        )
        betas[symbol] = beta
        if beta is not None:
            dollar_betas[symbol] = beta * data["market_value"]
        else:
            dollar_betas[symbol] = None

    # Portfolio beta = market-value-weighted average of individual betas
    total_value = 0.0
    weighted_sum = 0.0
    for symbol, beta in betas.items():
        if beta is not None:
            mv = holdings[symbol]["market_value"]
            weighted_sum += beta * mv
            total_value += mv

    portfolio_beta = weighted_sum / total_value if total_value > 0 else None
    portfolio_dollar_beta = sum(v for v in dollar_betas.values() if v is not None)

    return {
        "betas": betas,
        "dollar_betas": dollar_betas,
        "portfolio_beta": portfolio_beta,
        "portfolio_dollar_beta": portfolio_dollar_beta,
    }


# ── Black-Scholes delta ───────────────────────────────────────────────────────


def compute_option_delta(
    underlying_price: float,
    strike: float,
    dte_years: float,
    sigma: float,
    right: str,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    dividend_yield: float = 0.0,
) -> float | None:
    """Compute Black-Scholes delta for a European option.

    Args:
        underlying_price: Current price of the underlying.
        strike: Option strike price.
        dte_years: Time to expiry in years (e.g. 0.25 = 3 months).
        sigma: Annualized volatility (e.g. 0.30 = 30%).
        right: "C" for call, "P" for put.
        risk_free_rate: Annualized risk-free rate.
        dividend_yield: Annualized continuous dividend yield.

    Returns:
        Delta value (0 to 1 for calls, -1 to 0 for puts), or None.
    """
    if dte_years <= 0 or sigma <= 0 or underlying_price <= 0 or strike <= 0:
        return None

    try:
        d1 = (
            math.log(underlying_price / strike)
            + (risk_free_rate - dividend_yield + 0.5 * sigma ** 2) * dte_years
        ) / (sigma * math.sqrt(dte_years))

        if right == "C":
            return float(math.exp(-dividend_yield * dte_years) * norm.cdf(d1))
        else:  # P
            return float(math.exp(-dividend_yield * dte_years) * (norm.cdf(d1) - 1))
    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def compute_option_beta(
    underlying_beta: float | None,
    underlying_price: float,
    option_price: float,
    strike: float,
    dte_years: float,
    sigma: float,
    right: str,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    dividend_yield: float = 0.0,
) -> float | None:
    """Compute option beta using elasticity-adjusted formula.

    Option Beta = Underlying Beta × Delta × (Underlying Price / Option Price)

    This captures both the directional exposure (delta) and the leverage
    effect (underlying/option price ratio).

    Args:
        underlying_beta: Beta of the underlying vs benchmark.
        underlying_price: Current price of the underlying.
        option_price: Current price of the option (per share, not per contract).
        strike: Option strike price.
        dte_years: Time to expiry in years.
        sigma: Annualized volatility.
        right: "C" for call, "P" for put.
        risk_free_rate: Annualized risk-free rate.
        dividend_yield: Annualized continuous dividend yield.

    Returns:
        Option beta, or None if inputs are insufficient.
    """
    if underlying_beta is None or option_price <= 0:
        return None

    delta = compute_option_delta(
        underlying_price=underlying_price,
        strike=strike,
        dte_years=dte_years,
        sigma=sigma,
        right=right,
        risk_free_rate=risk_free_rate,
        dividend_yield=dividend_yield,
    )
    if delta is None:
        return None

    leverage = underlying_price / option_price
    return float(underlying_beta * delta * leverage)
