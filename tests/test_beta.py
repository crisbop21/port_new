"""Tests for portfolio beta calculation."""

import numpy as np
import pandas as pd
import pytest
from datetime import date, timedelta

from src.beta import (
    compute_beta,
    compute_portfolio_beta,
    BENCHMARKS,
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
