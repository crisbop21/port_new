"""Technical analysis signal computation and composite scoring.

Computes 10 signals from daily OHLCV data, scores each 0–100,
and produces a weighted composite rank per symbol.

Tier 1 (momentum/trend): Momentum 12-1mo, RSI 14, 50/200 SMA Trend,
                          Realized Vol 20d, Volume Trend
Tier 2 (confirmation):   MACD, Bollinger %B, ATR%, OBV Trend, ROC 20d
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Weight presets ────────────────────────────────────────────────────────────

WEIGHT_PRESETS = {
    "Momentum": {
        "momentum_12_1": 0.14,
        "rsi_14": 0.04,
        "sma_trend": 0.16,
        "realized_vol_20": 0.04,
        "volume_trend": 0.08,
        "macd": 0.14,
        "bollinger_pctb": 0.04,
        "atr_pct": 0.08,
        "obv_trend": 0.14,
        "roc_20": 0.14,
        # Momentum: 42%, Trend: 16%, Volume: 22%, Risk: 20%  (momentum-tilted)
    },
    "Balanced": {
        "momentum_12_1": 0.10,
        "rsi_14": 0.08,
        "sma_trend": 0.13,
        "realized_vol_20": 0.08,
        "volume_trend": 0.10,
        "macd": 0.10,
        "bollinger_pctb": 0.08,
        "atr_pct": 0.10,
        "obv_trend": 0.13,
        "roc_20": 0.10,
        # ~25% each category
    },
    "Defensive": {
        "momentum_12_1": 0.06,
        "rsi_14": 0.12,
        "sma_trend": 0.11,
        "realized_vol_20": 0.14,
        "volume_trend": 0.10,
        "macd": 0.06,
        "bollinger_pctb": 0.12,
        "atr_pct": 0.14,
        "obv_trend": 0.10,
        "roc_20": 0.05,
        # Risk/Mean-rev: 52%, Volume: 20%, Trend: 11%, Momentum: 17%  (defensive-tilted)
    },
}

SIGNAL_LABELS = {
    "momentum_12_1": "Momentum 12-1mo",
    "rsi_14": "RSI (14)",
    "sma_trend": "SMA Trend",
    "realized_vol_20": "Realized Vol 20d",
    "volume_trend": "Volume Trend",
    "macd": "MACD",
    "bollinger_pctb": "Bollinger %B",
    "atr_pct": "ATR %",
    "obv_trend": "OBV Trend",
    "roc_20": "ROC 20d",
}

SIGNAL_CATEGORIES = {
    "momentum_12_1": "Momentum",
    "rsi_14": "Risk / Mean-Rev",
    "sma_trend": "Trend",
    "realized_vol_20": "Risk / Mean-Rev",
    "volume_trend": "Volume",
    "macd": "Momentum",
    "bollinger_pctb": "Risk / Mean-Rev",
    "atr_pct": "Risk / Mean-Rev",
    "obv_trend": "Volume",
    "roc_20": "Momentum",
}

# ── Signal computation ────────────────────────────────────────────────────────


def compute_signals(df: pd.DataFrame) -> dict:
    """Compute all 10 technical signals from a DataFrame of daily OHLCV data.

    Expects columns: price_date, open, high, low, close, adj_close, volume.
    Returns a dict of {signal_name: raw_value} or None values if insufficient data.
    """
    df = df.copy()
    for col in ("open", "high", "low", "close", "adj_close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.sort_values("price_date").reset_index(drop=True)

    close = df["adj_close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    n = len(df)

    signals: dict = {}

    # 1. Momentum 12-1mo: return from 12 months ago to 1 month ago (skip recent month)
    if n >= 252:
        signals["momentum_12_1"] = float(close.iloc[-21] / close.iloc[-252] - 1)
    else:
        signals["momentum_12_1"] = None

    # 2. RSI 14
    if n >= 15:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta.clip(upper=0))
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        signals["rsi_14"] = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else None
    else:
        signals["rsi_14"] = None

    # 3. SMA Trend: composite distance from 50 & 200 SMA + golden/death cross
    if n >= 200:
        sma50 = close.rolling(50).mean()
        sma200 = close.rolling(200).mean()
        last_close = close.iloc[-1]
        dist_50 = (last_close - sma50.iloc[-1]) / sma50.iloc[-1]
        dist_200 = (last_close - sma200.iloc[-1]) / sma200.iloc[-1]
        cross = 1.0 if sma50.iloc[-1] > sma200.iloc[-1] else -1.0
        # Combined: distance above 200 SMA (primary) + 50 SMA bonus + cross state
        signals["sma_trend"] = float(dist_200 * 0.5 + dist_50 * 0.3 + cross * 0.2)
    elif n >= 50:
        sma50 = close.rolling(50).mean()
        last_close = close.iloc[-1]
        dist_50 = (last_close - sma50.iloc[-1]) / sma50.iloc[-1]
        signals["sma_trend"] = float(dist_50)
    else:
        signals["sma_trend"] = None

    # 4. Realized Volatility 20d (annualized)
    if n >= 21:
        log_ret = np.log(close / close.shift(1))
        vol_20 = log_ret.rolling(20).std().iloc[-1] * np.sqrt(252)
        signals["realized_vol_20"] = float(vol_20) if pd.notna(vol_20) else None
    else:
        signals["realized_vol_20"] = None

    # 5. Volume Trend: 20d avg / 50d avg
    if n >= 50:
        va20 = volume.rolling(20).mean().iloc[-1]
        va50 = volume.rolling(50).mean().iloc[-1]
        signals["volume_trend"] = float(va20 / va50) if va50 > 0 else None
    else:
        signals["volume_trend"] = None

    # 6. MACD: (12 EMA - 26 EMA) histogram normalized by price
    if n >= 35:
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram = macd_line - signal_line
        # Normalize by price so it's comparable across stocks
        signals["macd"] = float(histogram.iloc[-1] / close.iloc[-1]) if close.iloc[-1] > 0 else None
    else:
        signals["macd"] = None

    # 7. Bollinger %B
    if n >= 20:
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper = sma20 + 2 * std20
        lower = sma20 - 2 * std20
        band_width = upper.iloc[-1] - lower.iloc[-1]
        if band_width > 0:
            pctb = (close.iloc[-1] - lower.iloc[-1]) / band_width
            signals["bollinger_pctb"] = float(pctb)
        else:
            signals["bollinger_pctb"] = None
    else:
        signals["bollinger_pctb"] = None

    # 8. ATR% (14-day ATR as % of price)
    if n >= 15:
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr14 = tr.rolling(14).mean().iloc[-1]
        signals["atr_pct"] = float(atr14 / close.iloc[-1]) if close.iloc[-1] > 0 else None
    else:
        signals["atr_pct"] = None

    # 9. OBV Trend: slope of OBV over 20 days (normalized)
    if n >= 21:
        obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
        obv_20 = obv.iloc[-20:]
        x = np.arange(20)
        slope = np.polyfit(x, obv_20.values, 1)[0]
        # Normalize by avg volume so it's comparable
        avg_vol = volume.iloc[-20:].mean()
        signals["obv_trend"] = float(slope / avg_vol) if avg_vol > 0 else None
    else:
        signals["obv_trend"] = None

    # 10. ROC 20d: rate of change
    if n >= 21:
        signals["roc_20"] = float(close.iloc[-1] / close.iloc[-21] - 1)
    else:
        signals["roc_20"] = None

    return signals


def compute_ma_flags(df: pd.DataFrame) -> dict:
    """Return flags indicating whether current price is above/below key SMAs.

    Returns dict with keys: above_sma50, above_sma100, above_sma200.
    Each value is True/False or None if insufficient data for that SMA.
    """
    df = df.copy()
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df = df.sort_values("price_date").reset_index(drop=True)
    close = df["adj_close"]
    n = len(df)
    last_close = close.iloc[-1]

    flags: dict = {}
    for period in (50, 100, 200):
        key = f"above_sma{period}"
        if n >= period:
            sma = close.rolling(period).mean().iloc[-1]
            flags[key] = bool(last_close >= sma) if pd.notna(sma) else None
        else:
            flags[key] = None
    return flags


# ── Absolute scoring functions ────────────────────────────────────────────────
# Each maps a raw signal value to a 0–100 score.


def _clip(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def score_momentum_12_1(raw: float) -> float:
    """Higher momentum = higher score. Range roughly -0.5 to +1.0."""
    return _clip((raw + 0.3) / 1.0 * 100, 0, 100)


def score_rsi(raw: float) -> float:
    """Neutral ~50. Oversold (<30) = opportunity (high score).
    Overbought (>70) = caution (lower score).
    Uses contrarian interpretation for balanced/defensive use."""
    if raw <= 30:
        return _clip(80 + (30 - raw) / 30 * 20, 80, 100)  # 80-100
    elif raw <= 50:
        return _clip(50 + (50 - raw) / 20 * 30, 50, 80)   # 50-80
    elif raw <= 70:
        return _clip(50 - (raw - 50) / 20 * 20, 30, 50)   # 30-50
    else:
        return _clip(30 - (raw - 70) / 30 * 30, 0, 30)     # 0-30


def score_sma_trend(raw: float) -> float:
    """Above both SMAs with golden cross is best. Range roughly -0.5 to +0.7."""
    return _clip((raw + 0.3) / 0.8 * 100, 0, 100)


def score_realized_vol(raw: float) -> float:
    """Lower vol = higher score (low-vol anomaly). Range 0.05 to 1.0."""
    # 10% vol → 90 score, 50% vol → 30 score, 100% vol → 0
    return _clip((1.0 - raw) * 100, 0, 100)


def score_volume_trend(raw: float) -> float:
    """Ratio > 1 = accumulation. Range 0.3 to 3.0."""
    return _clip((raw - 0.5) / 2.0 * 100, 0, 100)


def score_macd(raw: float) -> float:
    """Positive histogram = bullish. Range roughly -0.03 to +0.03."""
    return _clip((raw + 0.02) / 0.04 * 100, 0, 100)


def score_bollinger_pctb(raw: float) -> float:
    """Mid-range (0.5) = neutral. Oversold (<0.2) = opportunity.
    Contrarian interpretation matching RSI approach."""
    if raw <= 0.2:
        return _clip(70 + (0.2 - raw) / 0.2 * 30, 70, 100)
    elif raw <= 0.5:
        return _clip(40 + (0.5 - raw) / 0.3 * 30, 40, 70)
    elif raw <= 0.8:
        return _clip(40 - (raw - 0.5) / 0.3 * 15, 25, 40)
    else:
        return _clip(25 - (raw - 0.8) / 0.4 * 25, 0, 25)


def score_atr_pct(raw: float) -> float:
    """Lower ATR% = less volatile = higher score. Range 0.005 to 0.10."""
    # 0.5% → 95, 3% → 60, 8% → 15
    return _clip((0.10 - raw) / 0.095 * 100, 0, 100)


def score_obv_trend(raw: float) -> float:
    """Positive slope = accumulation. Range roughly -2 to +2."""
    return _clip((raw + 1.5) / 3.0 * 100, 0, 100)


def score_roc_20(raw: float) -> float:
    """Higher short-term momentum = higher score. Range -0.20 to +0.20."""
    return _clip((raw + 0.15) / 0.35 * 100, 0, 100)


SCORERS = {
    "momentum_12_1": score_momentum_12_1,
    "rsi_14": score_rsi,
    "sma_trend": score_sma_trend,
    "realized_vol_20": score_realized_vol,
    "volume_trend": score_volume_trend,
    "macd": score_macd,
    "bollinger_pctb": score_bollinger_pctb,
    "atr_pct": score_atr_pct,
    "obv_trend": score_obv_trend,
    "roc_20": score_roc_20,
}


def score_signals(raw_signals: dict) -> dict:
    """Convert raw signal values to 0–100 scores."""
    scores = {}
    for key, raw in raw_signals.items():
        if raw is None or key not in SCORERS:
            scores[key] = None
        else:
            scores[key] = round(SCORERS[key](raw), 1)
    return scores


def composite_score(scores: dict, preset: str = "Balanced") -> float | None:
    """Compute weighted composite score from individual signal scores."""
    weights = WEIGHT_PRESETS.get(preset, WEIGHT_PRESETS["Balanced"])
    total_weight = 0.0
    weighted_sum = 0.0
    for key, weight in weights.items():
        val = scores.get(key)
        if val is not None:
            weighted_sum += val * weight
            total_weight += weight
    if total_weight == 0:
        return None
    # Renormalize if some signals are missing
    return round(weighted_sum / total_weight, 1)


def compute_all_rankings(
    price_data: dict[str, pd.DataFrame],
    preset: str = "Balanced",
) -> pd.DataFrame:
    """Compute signals, scores, and composite rank for all symbols.

    Args:
        price_data: {symbol: DataFrame of daily_prices rows}
        preset: weight preset name

    Returns:
        DataFrame with columns: Symbol, each signal raw + score, Composite, Rank
    """
    rows = []
    for symbol, df in price_data.items():
        if df.empty or len(df) < 15:
            logger.warning("Skipping %s: only %d rows of price data", symbol, len(df))
            continue

        raw = compute_signals(df)
        scores = score_signals(raw)
        comp = composite_score(scores, preset)
        ma_flags = compute_ma_flags(df)

        row = {"Symbol": symbol}
        for key in SIGNAL_LABELS:
            row[f"{key}_raw"] = raw.get(key)
            row[f"{key}_score"] = scores.get(key)
        row["Composite"] = comp
        row.update(ma_flags)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values("Composite", ascending=False).reset_index(drop=True)
    result["Rank"] = range(1, len(result) + 1)
    return result
