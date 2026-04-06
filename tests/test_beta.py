"""Tests for portfolio beta calculation and option Greeks."""

import math

import numpy as np
import pandas as pd
import pytest
from datetime import date, timedelta

from src.beta import (
    compute_beta,
    compute_portfolio_beta,
    compute_option_delta,
    compute_option_beta,
    BENCHMARKS,
    DEFAULT_RISK_FREE_RATE,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_price_series(n: int, start_price: float = 100.0, seed: int = 42) -> list[dict]:
    """Generate synthetic daily price rows."""
    rng = np.random.RandomState(seed)
    prices = [start_price]
    for _ in range(n - 1):
        ret = rng.normal(0.0005, 0.02)
        prices.append(prices[-1] * (1 + ret))

    start = date(2025, 1, 2)
    rows = []
    for i, p in enumerate(prices):
        d = start + timedelta(days=i)
        rows.append({
            "symbol": "TEST",
            "price_date": d.isoformat(),
            "open": str(round(p * 0.999, 4)),
            "high": str(round(p * 1.01, 4)),
            "low": str(round(p * 0.99, 4)),
            "close": str(round(p, 4)),
            "adj_close": str(round(p, 4)),
            "volume": 1_000_000,
        })
    return rows


def _make_correlated_series(
    benchmark_rows: list[dict],
    beta: float = 1.5,
    noise: float = 0.005,
    seed: int = 99,
) -> list[dict]:
    """Generate a price series correlated with the benchmark at a target beta."""
    rng = np.random.RandomState(seed)
    bench_prices = [float(r["adj_close"]) for r in benchmark_rows]
    bench_returns = np.diff(np.log(bench_prices))

    stock_returns = beta * bench_returns + rng.normal(0, noise, len(bench_returns))
    stock_prices = [100.0]
    for r in stock_returns:
        stock_prices.append(stock_prices[-1] * np.exp(r))

    rows = []
    for i, p in enumerate(stock_prices):
        rows.append({
            "symbol": "CORR",
            "price_date": benchmark_rows[i]["price_date"],
            "open": str(round(p * 0.999, 4)),
            "high": str(round(p * 1.01, 4)),
            "low": str(round(p * 0.99, 4)),
            "close": str(round(p, 4)),
            "adj_close": str(round(p, 4)),
            "volume": 500_000,
        })
    return rows


# ── Tests: compute_beta ──────────────────────────────────────────────────────


class TestComputeBeta:
    """Tests for the single-symbol beta calculation."""

    def test_beta_of_benchmark_vs_itself_is_one(self):
        """Beta of any asset vs itself should be exactly 1.0."""
        prices = _make_price_series(260, seed=10)
        result = compute_beta(prices, prices)
        assert result is not None
        assert abs(result - 1.0) < 1e-6

    def test_beta_with_known_correlation(self):
        """A synthetic series with target beta ~1.5 should compute close to 1.5."""
        bench = _make_price_series(260, seed=20)
        stock = _make_correlated_series(bench, beta=1.5, noise=0.003, seed=30)
        result = compute_beta(stock, bench)
        assert result is not None
        assert abs(result - 1.5) < 0.15  # allow some noise

    def test_beta_with_low_correlation(self):
        """A series with target beta ~0.5 should compute close to 0.5."""
        bench = _make_price_series(260, seed=40)
        stock = _make_correlated_series(bench, beta=0.5, noise=0.003, seed=50)
        result = compute_beta(stock, bench)
        assert result is not None
        assert abs(result - 0.5) < 0.15

    def test_insufficient_data_returns_none(self):
        """Less than min_periods of data should return None."""
        bench = _make_price_series(20)
        stock = _make_price_series(20, seed=5)
        result = compute_beta(stock, bench, min_periods=60)
        assert result is None

    def test_empty_data_returns_none(self):
        result = compute_beta([], [])
        assert result is None

    def test_mismatched_dates_uses_intersection(self):
        """When stock and benchmark have different date ranges, uses intersection."""
        bench = _make_price_series(300, seed=60)
        # Stock starts 40 days later
        stock_full = _make_correlated_series(bench, beta=1.2, noise=0.003, seed=70)
        stock = stock_full[40:]  # 260 rows
        bench_trimmed = bench  # 300 rows - intersection will be 260

        result = compute_beta(stock, bench_trimmed)
        assert result is not None
        assert abs(result - 1.2) < 0.2

    def test_custom_lookback(self):
        """Shorter lookback still works."""
        bench = _make_price_series(120, seed=80)
        stock = _make_correlated_series(bench, beta=1.0, noise=0.002, seed=90)
        result = compute_beta(stock, bench, lookback_days=90, min_periods=60)
        assert result is not None


# ── Tests: compute_portfolio_beta ────────────────────────────────────────────


class TestComputePortfolioBeta:
    """Tests for portfolio-level beta aggregation."""

    def test_single_position(self):
        """Portfolio with one position has that position's beta."""
        bench = _make_price_series(260, seed=100)
        stock = _make_correlated_series(bench, beta=1.3, noise=0.003, seed=110)

        holdings = {"AAPL": {"market_value": 10000.0, "prices": stock}}
        result = compute_portfolio_beta(holdings, bench)

        assert result is not None
        assert "AAPL" in result["betas"]
        assert abs(result["betas"]["AAPL"] - 1.3) < 0.15
        assert abs(result["portfolio_beta"] - result["betas"]["AAPL"]) < 0.01
        assert abs(result["portfolio_dollar_beta"] - 10000.0 * result["betas"]["AAPL"]) < 200

    def test_weighted_average(self):
        """Portfolio beta should be market-value weighted average of individual betas."""
        bench = _make_price_series(260, seed=120)
        stock_a = _make_correlated_series(bench, beta=2.0, noise=0.002, seed=130)
        stock_b = _make_correlated_series(bench, beta=0.5, noise=0.002, seed=140)

        holdings = {
            "HIGH": {"market_value": 5000.0, "prices": stock_a},
            "LOW": {"market_value": 5000.0, "prices": stock_b},
        }
        result = compute_portfolio_beta(holdings, bench)

        assert result is not None
        # Equal weights → portfolio beta should be ~(2.0+0.5)/2 = 1.25
        assert abs(result["portfolio_beta"] - 1.25) < 0.2

    def test_dollar_beta_sum(self):
        """Dollar beta = sum of (beta_i * market_value_i)."""
        bench = _make_price_series(260, seed=150)
        stock = _make_correlated_series(bench, beta=1.0, noise=0.001, seed=160)

        holdings = {
            "A": {"market_value": 3000.0, "prices": stock},
            "B": {"market_value": 7000.0, "prices": stock},
        }
        result = compute_portfolio_beta(holdings, bench)

        expected_dollar = sum(
            result["betas"][s] * holdings[s]["market_value"] for s in holdings
        )
        assert abs(result["portfolio_dollar_beta"] - expected_dollar) < 1.0

    def test_skip_positions_without_enough_data(self):
        """Positions without enough price data are skipped (beta=None)."""
        bench = _make_price_series(260, seed=170)
        stock_ok = _make_correlated_series(bench, beta=1.0, noise=0.002, seed=180)
        stock_short = _make_price_series(10, seed=190)

        holdings = {
            "OK": {"market_value": 5000.0, "prices": stock_ok},
            "SHORT": {"market_value": 5000.0, "prices": stock_short},
        }
        result = compute_portfolio_beta(holdings, bench)

        assert result["betas"]["OK"] is not None
        assert result["betas"]["SHORT"] is None
        # Portfolio beta only uses OK
        assert abs(result["portfolio_beta"] - result["betas"]["OK"]) < 0.01

    def test_empty_holdings(self):
        bench = _make_price_series(260, seed=200)
        result = compute_portfolio_beta({}, bench)
        assert result["portfolio_beta"] is None
        assert result["portfolio_dollar_beta"] == 0.0

    def test_dollar_betas_per_symbol(self):
        """Each symbol should have a dollar_beta entry."""
        bench = _make_price_series(260, seed=210)
        stock = _make_correlated_series(bench, beta=1.5, noise=0.002, seed=220)

        holdings = {
            "TSLA": {"market_value": 20000.0, "prices": stock},
        }
        result = compute_portfolio_beta(holdings, bench)

        assert "TSLA" in result["dollar_betas"]
        assert abs(result["dollar_betas"]["TSLA"] - 1.5 * 20000) < 4000


# ── Tests: benchmarks config ─────────────────────────────────────────────────


class TestBenchmarks:
    def test_spy_and_qqq_in_benchmarks(self):
        assert "SPY" in BENCHMARKS
        assert "QQQ" in BENCHMARKS


# ── Tests: compute_option_delta ──────────────────────────────────────────────


class TestComputeOptionDelta:
    """Tests for Black-Scholes delta calculation."""

    def test_atm_call_delta_near_half(self):
        """ATM call with moderate vol and time should have delta ~0.5-0.6."""
        delta = compute_option_delta(
            underlying_price=100.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="C",
        )
        assert delta is not None
        assert 0.45 < delta < 0.65

    def test_atm_put_delta_near_neg_half(self):
        """ATM put should have delta ~-0.5 to -0.4."""
        delta = compute_option_delta(
            underlying_price=100.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="P",
        )
        assert delta is not None
        assert -0.6 < delta < -0.4

    def test_deep_itm_call_delta_near_one(self):
        """Deep ITM call should have delta approaching 1."""
        delta = compute_option_delta(
            underlying_price=150.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.20,
            right="C",
        )
        assert delta is not None
        assert delta > 0.95

    def test_deep_otm_call_delta_near_zero(self):
        """Deep OTM call should have delta near 0."""
        delta = compute_option_delta(
            underlying_price=50.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.20,
            right="C",
        )
        assert delta is not None
        assert delta < 0.05

    def test_deep_itm_put_delta_near_neg_one(self):
        """Deep ITM put should have delta approaching -1."""
        delta = compute_option_delta(
            underlying_price=50.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.20,
            right="P",
        )
        assert delta is not None
        assert delta < -0.95

    def test_deep_otm_put_delta_near_zero(self):
        """Deep OTM put should have delta near 0."""
        delta = compute_option_delta(
            underlying_price=150.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.20,
            right="P",
        )
        assert delta is not None
        assert abs(delta) < 0.05

    def test_call_put_parity(self):
        """Call delta - Put delta should equal ~1 (adjusted for dividends)."""
        call_d = compute_option_delta(
            underlying_price=100.0,
            strike=100.0,
            dte_years=0.5,
            sigma=0.25,
            right="C",
        )
        put_d = compute_option_delta(
            underlying_price=100.0,
            strike=100.0,
            dte_years=0.5,
            sigma=0.25,
            right="P",
        )
        assert call_d is not None and put_d is not None
        # With no dividends, call_delta - put_delta ≈ 1.0
        assert abs((call_d - put_d) - 1.0) < 0.02

    def test_expired_option_returns_none(self):
        """Zero or negative DTE should return None."""
        delta = compute_option_delta(
            underlying_price=100.0,
            strike=100.0,
            dte_years=0.0,
            sigma=0.25,
            right="C",
        )
        assert delta is None

    def test_zero_vol_returns_none(self):
        delta = compute_option_delta(
            underlying_price=100.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.0,
            right="C",
        )
        assert delta is None

    def test_zero_underlying_price_returns_none(self):
        delta = compute_option_delta(
            underlying_price=0.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.25,
            right="C",
        )
        assert delta is None

    def test_higher_vol_widens_delta(self):
        """Higher vol should push ATM delta closer to 0.5 and OTM delta higher."""
        delta_low_vol = compute_option_delta(
            underlying_price=80.0, strike=100.0, dte_years=0.25,
            sigma=0.10, right="C",
        )
        delta_high_vol = compute_option_delta(
            underlying_price=80.0, strike=100.0, dte_years=0.25,
            sigma=0.50, right="C",
        )
        assert delta_low_vol is not None and delta_high_vol is not None
        # Higher vol → OTM call has higher delta
        assert delta_high_vol > delta_low_vol

    def test_longer_dte_increases_delta_for_otm(self):
        """Longer time to expiry should increase OTM call delta."""
        delta_short = compute_option_delta(
            underlying_price=80.0, strike=100.0, dte_years=0.05,
            sigma=0.25, right="C",
        )
        delta_long = compute_option_delta(
            underlying_price=80.0, strike=100.0, dte_years=1.0,
            sigma=0.25, right="C",
        )
        assert delta_short is not None and delta_long is not None
        assert delta_long > delta_short

    def test_with_dividend_yield(self):
        """Dividend yield should reduce call delta slightly."""
        delta_no_div = compute_option_delta(
            underlying_price=100.0, strike=100.0, dte_years=0.5,
            sigma=0.25, right="C", dividend_yield=0.0,
        )
        delta_with_div = compute_option_delta(
            underlying_price=100.0, strike=100.0, dte_years=0.5,
            sigma=0.25, right="C", dividend_yield=0.03,
        )
        assert delta_no_div is not None and delta_with_div is not None
        assert delta_with_div < delta_no_div


# ── Tests: compute_option_beta ───────────────────────────────────────────────


class TestComputeOptionBeta:
    """Tests for option beta = underlying_beta * delta * leverage."""

    def test_atm_call_beta_higher_than_underlying(self):
        """ATM call should have beta > underlying beta due to leverage."""
        ob = compute_option_beta(
            underlying_beta=1.2,
            underlying_price=100.0,
            option_price=5.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="C",
        )
        assert ob is not None
        assert ob > 1.2  # leveraged

    def test_put_beta_is_negative(self):
        """Put option beta should be negative (inverse exposure)."""
        ob = compute_option_beta(
            underlying_beta=1.0,
            underlying_price=100.0,
            option_price=5.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="P",
        )
        assert ob is not None
        assert ob < 0

    def test_deep_itm_call_beta_close_to_leveraged_underlying(self):
        """Deep ITM call (delta~1) beta ≈ underlying_beta * (S/option_price)."""
        ob = compute_option_beta(
            underlying_beta=1.5,
            underlying_price=150.0,
            option_price=52.0,  # ~$2 time value on $50 ITM
            strike=100.0,
            dte_years=0.25,
            sigma=0.20,
            right="C",
        )
        assert ob is not None
        # delta ~1, leverage ~150/52 ~2.88, so beta ~1.5 * 1 * 2.88 ~4.3
        assert 3.0 < ob < 6.0

    def test_zero_option_price_returns_none(self):
        """Worthless option should return None."""
        ob = compute_option_beta(
            underlying_beta=1.0,
            underlying_price=100.0,
            option_price=0.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="C",
        )
        assert ob is None

    def test_none_underlying_beta_returns_none(self):
        ob = compute_option_beta(
            underlying_beta=None,
            underlying_price=100.0,
            option_price=5.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="C",
        )
        assert ob is None

    def test_expired_option_returns_none(self):
        ob = compute_option_beta(
            underlying_beta=1.0,
            underlying_price=100.0,
            option_price=5.0,
            strike=100.0,
            dte_years=0.0,
            sigma=0.30,
            right="C",
        )
        assert ob is None

    def test_short_put_beta_positive(self):
        """Short put (negative quantity) has positive beta exposure.
        The sign flip from quantity is handled at the portfolio level,
        not in compute_option_beta which works per-contract."""
        ob = compute_option_beta(
            underlying_beta=1.0,
            underlying_price=100.0,
            option_price=5.0,
            strike=100.0,
            dte_years=0.25,
            sigma=0.30,
            right="P",
        )
        # Per-contract put beta is negative (delta is negative)
        assert ob is not None
        assert ob < 0
