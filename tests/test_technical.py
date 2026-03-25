"""Tests for src/technical.py — signal computation, scoring, and composite ranking."""

import numpy as np
import pandas as pd
import pytest

from src.technical import (
    SIGNAL_LABELS,
    WEIGHT_PRESETS,
    composite_score,
    compute_all_rankings,
    compute_ma_flags,
    compute_signals,
    score_bollinger_pctb,
    score_macd,
    score_momentum_12_1,
    score_rsi,
    score_signals,
    score_sma_trend,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 300, start_price: float = 100.0, seed: int = 42) -> pd.DataFrame:
    """Generate n days of synthetic OHLCV data with a slight uptrend."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=n)
    close = [start_price]
    for _ in range(n - 1):
        close.append(close[-1] * (1 + rng.normal(0.0003, 0.015)))
    close = np.array(close)
    high = close * (1 + rng.uniform(0, 0.02, n))
    low = close * (1 - rng.uniform(0, 0.02, n))
    open_ = close * (1 + rng.normal(0, 0.005, n))
    volume = rng.integers(100_000, 5_000_000, n).astype(float)
    return pd.DataFrame({
        "price_date": dates,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "adj_close": close,
        "volume": volume,
    })


# ── Weight preset tests ─────────────────────────────────────────────────────

class TestWeightPresets:
    @pytest.mark.parametrize("preset_name", list(WEIGHT_PRESETS.keys()))
    def test_weights_sum_to_one(self, preset_name):
        """Each preset's weights must sum to 1.0."""
        weights = WEIGHT_PRESETS[preset_name]
        total = sum(weights.values())
        assert abs(total - 1.0) < 1e-9, (
            f"Preset '{preset_name}' sums to {total}, expected 1.0"
        )

    @pytest.mark.parametrize("preset_name", list(WEIGHT_PRESETS.keys()))
    def test_all_signals_present(self, preset_name):
        """Every preset must contain all 10 signal keys."""
        weights = WEIGHT_PRESETS[preset_name]
        assert set(weights.keys()) == set(SIGNAL_LABELS.keys())

    @pytest.mark.parametrize("preset_name", list(WEIGHT_PRESETS.keys()))
    def test_all_weights_positive(self, preset_name):
        weights = WEIGHT_PRESETS[preset_name]
        for key, w in weights.items():
            assert w > 0, f"{preset_name}.{key} weight must be positive"


# ── Momentum 12-1mo tests ───────────────────────────────────────────────────

class TestMomentum12_1:
    def test_momentum_skips_recent_month(self):
        """Momentum 12-1 should equal the return from 12mo ago to 1mo ago,
        i.e. close[-21] / close[-252] - 1, NOT ret_12m - ret_1m."""
        df = _make_ohlcv(n=300)
        close = pd.to_numeric(df["adj_close"])
        expected = float(close.iloc[-21] / close.iloc[-252] - 1)
        signals = compute_signals(df)
        assert signals["momentum_12_1"] is not None
        assert abs(signals["momentum_12_1"] - expected) < 1e-9, (
            f"Got {signals['momentum_12_1']}, expected {expected}"
        )

    def test_momentum_none_when_insufficient_data(self):
        df = _make_ohlcv(n=200)
        signals = compute_signals(df)
        assert signals["momentum_12_1"] is None


# ── Scoring boundary tests ───────────────────────────────────────────────────

class TestScoringBoundaries:
    """All scorers must return values in [0, 100]."""

    def test_momentum_score_range(self):
        for raw in np.linspace(-1.0, 2.0, 50):
            s = score_momentum_12_1(raw)
            assert 0 <= s <= 100

    def test_rsi_score_range(self):
        for raw in np.linspace(0, 100, 50):
            s = score_rsi(raw)
            assert 0 <= s <= 100

    def test_rsi_contrarian(self):
        """Oversold (RSI < 30) should score higher than overbought (RSI > 70)."""
        assert score_rsi(20) > score_rsi(80)

    def test_sma_trend_score_range(self):
        for raw in np.linspace(-1.0, 1.0, 50):
            s = score_sma_trend(raw)
            assert 0 <= s <= 100

    def test_bollinger_contrarian(self):
        """Oversold (%B < 0.2) should score higher than overbought (%B > 0.8)."""
        assert score_bollinger_pctb(0.1) > score_bollinger_pctb(0.9)

    def test_macd_positive_beats_negative(self):
        assert score_macd(0.01) > score_macd(-0.01)


# ── Signal computation tests ────────────────────────────────────────────────

class TestComputeSignals:
    def test_all_signals_present_with_enough_data(self):
        df = _make_ohlcv(n=300)
        signals = compute_signals(df)
        for key in SIGNAL_LABELS:
            assert key in signals, f"Missing signal: {key}"

    def test_all_signals_have_values_with_enough_data(self):
        """With 300 days of data, all 10 signals should be non-None."""
        df = _make_ohlcv(n=300)
        signals = compute_signals(df)
        for key in SIGNAL_LABELS:
            assert signals[key] is not None, f"Signal {key} is None with 300 days"

    def test_rsi_in_valid_range(self):
        df = _make_ohlcv(n=100)
        signals = compute_signals(df)
        assert 0 <= signals["rsi_14"] <= 100

    def test_volume_trend_positive(self):
        df = _make_ohlcv(n=100)
        signals = compute_signals(df)
        assert signals["volume_trend"] > 0

    def test_minimal_data_returns_partial(self):
        """With only 20 days, momentum and SMA trend should be None."""
        df = _make_ohlcv(n=20)
        signals = compute_signals(df)
        assert signals["momentum_12_1"] is None
        assert signals["sma_trend"] is None


# ── Composite score tests ───────────────────────────────────────────────────

class TestCompositeScore:
    def test_perfect_scores(self):
        scores = {k: 100.0 for k in SIGNAL_LABELS}
        assert composite_score(scores, "Balanced") == 100.0

    def test_zero_scores(self):
        scores = {k: 0.0 for k in SIGNAL_LABELS}
        assert composite_score(scores, "Balanced") == 0.0

    def test_missing_signals_renormalized(self):
        """If some signals are None, composite should still work."""
        scores = {k: None for k in SIGNAL_LABELS}
        scores["rsi_14"] = 80.0
        scores["macd"] = 60.0
        result = composite_score(scores, "Balanced")
        assert result is not None
        assert 0 <= result <= 100

    def test_all_none_returns_none(self):
        scores = {k: None for k in SIGNAL_LABELS}
        assert composite_score(scores) is None


# ── Rankings pipeline tests ──────────────────────────────────────────────────

class TestComputeAllRankings:
    def test_ranking_order(self):
        price_data = {
            "AAA": _make_ohlcv(n=300, start_price=100, seed=1),
            "BBB": _make_ohlcv(n=300, start_price=100, seed=2),
        }
        df = compute_all_rankings(price_data, preset="Balanced")
        assert not df.empty
        assert list(df["Rank"]) == [1, 2]
        # Composite should be descending
        assert df.iloc[0]["Composite"] >= df.iloc[1]["Composite"]

    def test_skips_insufficient_data(self):
        price_data = {
            "OK": _make_ohlcv(n=300),
            "SHORT": _make_ohlcv(n=5),
        }
        df = compute_all_rankings(price_data)
        assert len(df) == 1
        assert df.iloc[0]["Symbol"] == "OK"

    def test_empty_input(self):
        df = compute_all_rankings({})
        assert df.empty

    def test_rankings_include_ma_flags(self):
        """Rankings DataFrame should include MA flag columns."""
        price_data = {"AAA": _make_ohlcv(n=300, seed=1)}
        df = compute_all_rankings(price_data, preset="Balanced")
        for col in ("above_sma50", "above_sma100", "above_sma200"):
            assert col in df.columns, f"Missing column: {col}"


# ── Moving average flag tests ──────────────────────────────────────────────

class TestMAFlags:
    def test_flags_returned_with_enough_data(self):
        df = _make_ohlcv(n=300)
        flags = compute_ma_flags(df)
        assert "above_sma50" in flags
        assert "above_sma100" in flags
        assert "above_sma200" in flags

    def test_flags_are_boolean(self):
        df = _make_ohlcv(n=300)
        flags = compute_ma_flags(df)
        for key in ("above_sma50", "above_sma100", "above_sma200"):
            assert isinstance(flags[key], bool), f"{key} should be bool"

    def test_flags_none_when_insufficient_data(self):
        """With only 30 days, SMA100 and SMA200 flags should be None."""
        df = _make_ohlcv(n=30)
        flags = compute_ma_flags(df)
        assert flags["above_sma100"] is None
        assert flags["above_sma200"] is None

    def test_flag_above_when_price_above_sma(self):
        """If the last close is well above the mean, flag should be True."""
        df = _make_ohlcv(n=300, start_price=50.0, seed=10)
        # Force last close far above the rolling mean
        df = df.copy()
        df.loc[df.index[-1], "adj_close"] = 9999.0
        df.loc[df.index[-1], "close"] = 9999.0
        flags = compute_ma_flags(df)
        assert flags["above_sma50"] is True
        assert flags["above_sma100"] is True
        assert flags["above_sma200"] is True

    def test_flag_below_when_price_below_sma(self):
        """If the last close is well below the mean, flag should be False."""
        df = _make_ohlcv(n=300, start_price=200.0, seed=10)
        df = df.copy()
        df.loc[df.index[-1], "adj_close"] = 0.01
        df.loc[df.index[-1], "close"] = 0.01
        flags = compute_ma_flags(df)
        assert flags["above_sma50"] is False
        assert flags["above_sma100"] is False
        assert flags["above_sma200"] is False

    def test_sma50_available_without_sma200(self):
        """With 60 days, SMA50 should be available but SMA200 should not."""
        df = _make_ohlcv(n=60)
        flags = compute_ma_flags(df)
        assert flags["above_sma50"] is not None
        assert flags["above_sma200"] is None
