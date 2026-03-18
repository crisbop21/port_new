"""Generate a PDF guide explaining the 10 technical analysis signals.

Uses fpdf2 (free, no API key). Called from the Streamlit Technical page
to provide an in-app download button.
"""

from io import BytesIO

from fpdf import FPDF


# ── Signal definitions ────────────────────────────────────────────────────────

SIGNALS = [
    {
        "name": "Momentum 12-1mo",
        "key": "momentum_12_1",
        "category": "Momentum",
        "data_needed": "252 trading days (~1 year)",
        "what": (
            "Measures price appreciation over the past 12 months, excluding the "
            "most recent month. This 'skip month' approach is based on Jegadeesh "
            "& Titman's momentum research — the latest month often exhibits "
            "short-term reversal, so stripping it out gives a cleaner momentum signal."
        ),
        "how": (
            "Raw = (P_today / P_252days_ago - 1) - (P_today / P_21days_ago - 1). "
            "Essentially the 12-month return minus the 1-month return."
        ),
        "scoring": (
            "Mapped linearly from roughly -30% to +70% onto the 0-100 scale. "
            "A raw value of 0% maps to ~30; strong positive momentum of +50% "
            "maps to ~80."
        ),
        "interpretation": (
            "High score = strong medium-term uptrend. Stocks with persistent "
            "momentum tend to continue outperforming over the next 3-12 months "
            "(momentum factor). Low score = prolonged weakness."
        ),
    },
    {
        "name": "RSI (14)",
        "key": "rsi_14",
        "category": "Risk / Mean-Reversion",
        "data_needed": "15 trading days",
        "what": (
            "The Relative Strength Index measures the speed and magnitude of "
            "recent price changes to identify overbought or oversold conditions. "
            "Uses the standard 14-day lookback period."
        ),
        "how": (
            "Computes the average gain and average loss over 14 periods. "
            "RS = avg_gain / avg_loss. RSI = 100 - 100 / (1 + RS). "
            "Output ranges from 0 to 100."
        ),
        "scoring": (
            "Uses CONTRARIAN scoring: oversold (RSI < 30) scores 80-100 "
            "(buying opportunity), neutral (30-50) scores 50-80, mildly "
            "overbought (50-70) scores 30-50, heavily overbought (>70) "
            "scores 0-30."
        ),
        "interpretation": (
            "High score = stock is oversold and may be due for a bounce. "
            "Low score = overbought, risk of pullback. The contrarian approach "
            "rewards buying dips rather than chasing rallies. The Momentum "
            "preset downweights RSI; the Defensive preset gives it 10% weight."
        ),
    },
    {
        "name": "SMA Trend",
        "key": "sma_trend",
        "category": "Trend",
        "data_needed": "200 trading days (50 minimum)",
        "what": (
            "Composite indicator combining the stock's distance from its "
            "50-day and 200-day simple moving averages, plus the golden/death "
            "cross state (whether the 50 SMA is above or below the 200 SMA)."
        ),
        "how": (
            "dist_200 = (close - SMA200) / SMA200; dist_50 = (close - SMA50) / SMA50; "
            "cross = +1 if SMA50 > SMA200 else -1. "
            "Combined = 0.5 * dist_200 + 0.3 * dist_50 + 0.2 * cross. "
            "Falls back to just dist_50 if fewer than 200 days are available."
        ),
        "scoring": (
            "Mapped linearly from roughly -0.3 to +0.5 onto the 0-100 scale. "
            "Above both SMAs with golden cross scores ~80+; below both with "
            "death cross scores ~20 or lower."
        ),
        "interpretation": (
            "High score = strong uptrend across multiple timeframes. "
            "The 200 SMA is the most widely watched institutional level — "
            "stocks above it tend to attract buyers. Golden cross (50 > 200) "
            "confirms trend strength."
        ),
    },
    {
        "name": "Realized Volatility 20d",
        "key": "realized_vol_20",
        "category": "Risk / Mean-Reversion",
        "data_needed": "21 trading days",
        "what": (
            "The annualized standard deviation of daily log returns over "
            "the most recent 20 trading days. Measures how 'jumpy' the "
            "stock has been recently."
        ),
        "how": (
            "log_return = ln(close_t / close_{t-1}). "
            "vol = std(log_returns, 20 days) * sqrt(252). "
            "Output is an annualized percentage (e.g. 0.25 = 25% vol)."
        ),
        "scoring": (
            "INVERSE scoring: low vol = high score. 10% vol maps to ~90; "
            "50% vol maps to ~50; 100% vol maps to ~0. Based on the "
            "'low-volatility anomaly' — less volatile stocks have historically "
            "delivered better risk-adjusted returns."
        ),
        "interpretation": (
            "High score = calm, low-volatility stock (defensive). "
            "Low score = highly volatile, greater risk of large drawdowns. "
            "The Defensive preset gives this 12% weight, double the Momentum preset."
        ),
    },
    {
        "name": "Volume Trend",
        "key": "volume_trend",
        "category": "Volume",
        "data_needed": "50 trading days",
        "what": (
            "Ratio of the 20-day average volume to the 50-day average volume. "
            "Detects whether recent trading activity is accelerating or fading "
            "relative to the medium-term norm."
        ),
        "how": (
            "Raw = SMA(volume, 20) / SMA(volume, 50). "
            "A value of 1.0 means recent volume matches the 50-day average; "
            "> 1.0 means acceleration; < 1.0 means fading."
        ),
        "scoring": (
            "Mapped from 0.5x to 2.5x onto 0-100. Ratio of 1.0x (neutral) "
            "scores ~25; 1.5x (strong accumulation) scores ~50; 2.5x+ scores 100."
        ),
        "interpretation": (
            "High score = rising volume confirms price moves (accumulation). "
            "Low score = declining volume suggests the trend is losing "
            "participation. Volume is the 'fuel' behind price moves — "
            "price changes on high volume are more reliable."
        ),
    },
    {
        "name": "MACD",
        "key": "macd",
        "category": "Momentum",
        "data_needed": "35 trading days",
        "what": (
            "Moving Average Convergence Divergence — the histogram value "
            "(MACD line minus signal line), normalized by stock price. "
            "Captures short-term momentum shifts and trend reversals."
        ),
        "how": (
            "MACD_line = EMA(12) - EMA(26). Signal = EMA(MACD_line, 9). "
            "Histogram = MACD_line - Signal. "
            "Normalized = Histogram / close_price (makes it comparable across "
            "stocks at different price levels)."
        ),
        "scoring": (
            "Mapped from -0.02 to +0.02 (normalized) onto 0-100. "
            "Positive histogram (bullish) scores > 50; negative (bearish) < 50. "
            "A zero crossing often signals a trend change."
        ),
        "interpretation": (
            "High score = recent bullish momentum acceleration. "
            "Rising histogram = strengthening trend. Falling histogram even "
            "while positive = trend is decelerating. Crossovers from negative "
            "to positive are classic buy signals."
        ),
    },
    {
        "name": "Bollinger %B",
        "key": "bollinger_pctb",
        "category": "Risk / Mean-Reversion",
        "data_needed": "20 trading days",
        "what": (
            "Shows where the current price sits within the Bollinger Bands "
            "(20-day SMA +/- 2 standard deviations). Identifies whether a "
            "stock is stretched to extremes relative to recent volatility."
        ),
        "how": (
            "%B = (close - lower_band) / (upper_band - lower_band). "
            "Values: 0 = at lower band, 0.5 = at midpoint (SMA), 1.0 = at "
            "upper band. Can exceed 0-1 range in strong trends."
        ),
        "scoring": (
            "CONTRARIAN scoring (like RSI): near lower band (<0.2) scores "
            "70-100 (buying opportunity); midpoint (0.5) scores ~40-50; "
            "near upper band (>0.8) scores 0-25. Rewards buying when price "
            "is compressed, not extended."
        ),
        "interpretation": (
            "High score = price near lower band, potential mean-reversion "
            "bounce. Low score = stretched to upper band, risk of pullback. "
            "Works best in range-bound markets. In strong trends, low scores "
            "may persist (use with SMA Trend for context)."
        ),
    },
    {
        "name": "ATR %",
        "key": "atr_pct",
        "category": "Risk / Mean-Reversion",
        "data_needed": "15 trading days",
        "what": (
            "Average True Range as a percentage of the stock's price. "
            "The True Range captures intraday volatility including gaps, "
            "and expressing it as a percentage makes it comparable across "
            "different-priced stocks."
        ),
        "how": (
            "TR = max(high - low, |high - prev_close|, |low - prev_close|). "
            "ATR = SMA(TR, 14 days). ATR% = ATR / close. "
            "Example: $2 ATR on a $100 stock = 2% ATR."
        ),
        "scoring": (
            "INVERSE scoring: lower ATR% = higher score. 0.5% ATR maps "
            "to ~95 (very calm); 3% maps to ~70; 8% maps to ~20. "
            "Complements Realized Volatility by capturing intraday range, "
            "not just close-to-close moves."
        ),
        "interpretation": (
            "High score = tight daily ranges, orderly trading (suitable for "
            "tighter stops). Low score = wide swings, harder to manage risk. "
            "ATR% often spikes during selloffs and compresses during rallies, "
            "making it useful for position sizing."
        ),
    },
    {
        "name": "OBV Trend",
        "key": "obv_trend",
        "category": "Volume",
        "data_needed": "21 trading days",
        "what": (
            "The slope of On-Balance Volume over the last 20 days, normalized "
            "by average volume. OBV adds volume on up days and subtracts on "
            "down days — its trend reveals whether volume 'confirms' the price trend."
        ),
        "how": (
            "OBV = cumulative sum of (sign(close_change) * volume). "
            "Fit a linear regression to the last 20 OBV values. "
            "Normalized slope = slope / avg_volume_20d."
        ),
        "scoring": (
            "Mapped from -1.5 to +1.5 (normalized) onto 0-100. "
            "Positive slope (accumulation) > 50; negative slope (distribution) < 50."
        ),
        "interpretation": (
            "High score = rising OBV confirms price uptrend — 'smart money' "
            "is accumulating. Divergence (price rising but OBV falling) is a "
            "classic warning of impending weakness. OBV often leads price "
            "moves by a few days."
        ),
    },
    {
        "name": "ROC 20d",
        "key": "roc_20",
        "category": "Momentum",
        "data_needed": "21 trading days",
        "what": (
            "Rate of Change over 20 trading days — the simple percentage "
            "return over the past month. A quick snapshot of short-term "
            "momentum."
        ),
        "how": (
            "ROC = close_today / close_20days_ago - 1. "
            "Example: stock went from $100 to $108 = +8% ROC."
        ),
        "scoring": (
            "Mapped from -15% to +20% onto 0-100. ROC of 0% (flat) "
            "maps to ~43; +10% maps to ~71; -10% maps to ~14."
        ),
        "interpretation": (
            "High score = strong short-term momentum. Differs from the "
            "12-1mo momentum signal in timeframe — ROC 20d captures the "
            "most recent month that the 12-1mo signal intentionally skips. "
            "Together they cover short-term and medium-term momentum."
        ),
    },
]

PRESET_DESCRIPTIONS = {
    "Momentum": (
        "Overweights trend-following and momentum signals (Momentum 12-1mo, MACD, "
        "ROC 20d, SMA Trend). Best suited for markets with clear directional trends. "
        "Underweights mean-reversion and volatility signals. Aims to ride winners "
        "and cut losers."
    ),
    "Balanced": (
        "Distributes weight roughly equally across all four signal categories: "
        "momentum, trend, volume, and risk/mean-reversion. A good default for "
        "most market conditions. No single signal dominates the composite score."
    ),
    "Defensive": (
        "Overweights volatility, mean-reversion, and risk signals (Realized Vol, "
        "ATR%, RSI, Bollinger %B). Best suited for choppy or uncertain markets. "
        "Favors low-volatility stocks with oversold conditions. Underweights "
        "pure momentum signals."
    ),
}


# ── PDF generation ────────────────────────────────────────────────────────────


def generate_signal_guide_pdf() -> bytes:
    """Generate a PDF explaining all 10 technical signals and 3 presets.

    Returns the PDF as bytes, suitable for st.download_button.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ── Title page ────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 24)
    pdf.cell(0, 40, "", ln=True)  # top margin
    pdf.cell(0, 15, "Technical Analysis", ln=True, align="C")
    pdf.cell(0, 15, "Signal Guide", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 20, "", ln=True)
    pdf.cell(0, 8, "IBKR Trade Journal", ln=True, align="C")
    pdf.cell(0, 8, "10 Signals, 3 Presets, 0-100 Scoring", ln=True, align="C")

    # ── Table of contents ─────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 12, "Contents", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 4, "", ln=True)

    pdf.cell(0, 7, "1. How Scoring Works", ln=True)
    pdf.cell(0, 7, "2. Strategy Presets", ln=True)
    for i, sig in enumerate(SIGNALS, start=1):
        pdf.cell(0, 7, f"3.{i}. {sig['name']}  ({sig['category']})", ln=True)
    pdf.cell(0, 7, "4. Putting It Together", ln=True)

    # ── Scoring overview ──────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 12, "1. How Scoring Works", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 6, (
        "Each of the 10 signals is computed from daily OHLCV (open, high, low, "
        "close, volume) price data. Raw signal values are then mapped to a 0-100 "
        "score using fixed thresholds — not percentile ranks against other stocks. "
        "This means a score of 80 always means the same thing regardless of what "
        "other stocks are in your portfolio.\n\n"
        "Scores are then combined into a single Composite Score using weights "
        "from the selected strategy preset. If a signal cannot be computed (e.g. "
        "not enough price history), it is excluded and the remaining weights are "
        "renormalized so the composite still sums correctly.\n\n"
        "Score interpretation:\n"
        "  70-100: Favorable (green)\n"
        "  40-69:  Neutral (yellow)\n"
        "  0-39:   Unfavorable (red)\n\n"
        "Important: RSI and Bollinger %B use CONTRARIAN scoring. An oversold "
        "reading (typically considered 'bad' by momentum traders) scores HIGH "
        "because it represents a potential buying opportunity. This makes these "
        "signals work as a counterbalance to pure momentum signals."
    ))

    # ── Presets ────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 12, "2. Strategy Presets", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 6, (
        "Three presets control how the 10 signal scores are weighted into the "
        "composite. You can switch presets at any time to see how rankings change."
    ))
    pdf.cell(0, 4, "", ln=True)

    for name, desc in PRESET_DESCRIPTIONS.items():
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, name, ln=True)
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 6, desc)
        pdf.cell(0, 4, "", ln=True)

    # ── Individual signals ────────────────────────────────────────────────
    for i, sig in enumerate(SIGNALS, start=1):
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, f"3.{i}. {sig['name']}", ln=True)
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 6, f"Category: {sig['category']}  |  Data needed: {sig['data_needed']}", ln=True)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 4, "", ln=True)

        sections = [
            ("What it measures", sig["what"]),
            ("How it is calculated", sig["how"]),
            ("How it is scored (0-100)", sig["scoring"]),
            ("How to interpret the score", sig["interpretation"]),
        ]

        for heading, body in sections:
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 7, heading, ln=True)
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5.5, body)
            pdf.cell(0, 3, "", ln=True)

    # ── Putting it together ───────────────────────────────────────────────
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 12, "4. Putting It Together", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 6, (
        "The composite score ranks your portfolio holdings from most to least "
        "favorable based on the selected preset's priorities. Some guidelines:\n\n"
        "- No single signal should drive a decision. The composite deliberately "
        "blends momentum, trend, volume, and risk signals.\n\n"
        "- Contrarian signals (RSI, Bollinger) act as a sanity check against "
        "pure momentum. A stock with high momentum but deeply overbought RSI "
        "will have a tempered composite score.\n\n"
        "- Missing signals (insufficient data) do not penalize a stock. Weights "
        "are renormalized, so a stock with only 6 computable signals is still "
        "compared fairly.\n\n"
        "- Presets do not change the raw signals — only the weighting. Switching "
        "presets lets you see the same data through different lenses.\n\n"
        "- Signals are computed from stored daily prices. Make sure to keep "
        "prices up to date via the Prices page for accurate rankings.\n\n"
        "- These signals are technical in nature and do not account for "
        "fundamentals. Use alongside the Metrics page for a complete view."
    ))

    buf = BytesIO()
    pdf.output(buf)
    return buf.getvalue()
