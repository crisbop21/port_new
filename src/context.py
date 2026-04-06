"""Portfolio context assembly for the Options Advisor.

Collects positions, fundamentals, technicals, and price data for each
underlying, then serialises it into a markdown string suitable for
injecting into a Claude API prompt.
"""

import logging
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd

from src.beta import BENCHMARKS, compute_beta, compute_portfolio_beta
from src.db import (
    get_daily_prices,
    get_latest_price,
    get_latest_stock_metrics,
    get_latest_valuation_snapshots,
    get_positions_as_of,
    get_snapshot_dates,
    get_trades,
    get_account_ids,
)
from src.technical import SIGNAL_LABELS, compute_signals, compute_ma_flags, score_signals

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _compute_dte(expiry: date | str | None, today: date | None = None) -> int | None:
    """Compute days to expiration. Returns negative if expired."""
    if expiry is None:
        return None
    if isinstance(expiry, str):
        expiry = date.fromisoformat(expiry)
    if today is None:
        today = date.today()
    return (expiry - today).days


def _compute_moneyness(
    strike: Decimal | None,
    current_price: Decimal | None,
    right: str | None,
) -> float | None:
    """Compute moneyness as a fraction.

    Positive = ITM, negative = OTM.
    For calls: (price - strike) / strike
    For puts:  (strike - price) / strike
    """
    if strike is None or current_price is None or right is None:
        return None
    strike_f = float(strike)
    price_f = float(current_price)
    if strike_f == 0:
        return None
    if right == "C":
        return (price_f - strike_f) / strike_f
    else:  # P
        return (strike_f - price_f) / strike_f


def _extract_underlying(symbol: str, asset_class: str) -> str:
    """Extract the underlying ticker from a symbol string.

    Options symbols like 'AAPL 20240119 150.0 C' → 'AAPL'.
    Stocks/ETFs return as-is.
    """
    if asset_class == "OPT" and " " in symbol:
        return symbol.split(" ")[0]
    return symbol


def _format_number(value, decimal_places: int = 2) -> str:
    """Format a number for display. Handles None, large numbers."""
    if value is None:
        return "N/A"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    if v == 0:
        return f"{v:.{decimal_places}f}"
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.{decimal_places}f}B"
    if abs_v >= 1_000_000:
        return f"{v / 1_000_000:.{decimal_places}f}M"
    return f"{v:,.{decimal_places}f}"


def _compute_breakeven(
    strike: Decimal | None,
    cost_basis: Decimal | None,
    quantity: Decimal | None,
    right: str | None,
) -> Decimal | None:
    """Compute option breakeven price.

    Call: strike + (total_cost / 100 / quantity)
    Put:  strike - (total_cost / 100 / quantity)
    """
    if strike is None or cost_basis is None or quantity is None or right is None:
        return None
    try:
        qty = Decimal(str(quantity))
        if qty == 0:
            return None
        premium_per_share = Decimal(str(cost_basis)) / 100 / qty
        s = Decimal(str(strike))
        if right == "C":
            return s + premium_per_share
        else:
            return s - premium_per_share
    except Exception:
        return None


def _compute_realized_vol(daily_prices: list[dict], window: int = 20) -> float | None:
    """Compute annualized realized volatility from daily price data."""
    if not daily_prices or len(daily_prices) < window + 1:
        return None
    df = pd.DataFrame(daily_prices)
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df = df.sort_values("price_date").reset_index(drop=True)
    log_ret = np.log(df["adj_close"] / df["adj_close"].shift(1))
    vol = log_ret.rolling(window).std().iloc[-1] * np.sqrt(252)
    return float(vol) if pd.notna(vol) else None


# ── Main context builder ────────────────────────────────────────────────────


def build_position_context(
    account_id: str,
    vol_overrides: dict[str, float] | None = None,
    today: date | None = None,
) -> dict:
    """Assemble full portfolio context for the advisor.

    Args:
        account_id: IBKR account ID.
        vol_overrides: Optional dict of {underlying: implied_vol} overrides.
            When provided, these replace the computed realized vol.
        today: Override for current date (for testing).

    Returns:
        Dict with keys: account_id, as_of_date, positions, underlyings.
    """
    if today is None:
        today = date.today()
    if vol_overrides is None:
        vol_overrides = {}

    # Get latest snapshot date
    snapshot_dates = get_snapshot_dates(account_id)
    if not snapshot_dates:
        return {
            "account_id": account_id,
            "as_of_date": today.isoformat(),
            "positions": [],
            "underlyings": {},
        }

    latest_date = snapshot_dates[-1]
    positions_raw = get_positions_as_of(account_id, latest_date)

    # Collect unique underlyings
    underlyings_seen: set[str] = set()
    enriched_positions: list[dict] = []

    for pos in positions_raw:
        symbol = pos["symbol"]
        asset_class = pos["asset_class"]
        underlying = _extract_underlying(symbol, asset_class)
        underlyings_seen.add(underlying)

        strike = Decimal(str(pos["strike"])) if pos.get("strike") else None
        quantity = Decimal(str(pos["quantity"])) if pos.get("quantity") else None
        cost_basis = Decimal(str(pos["cost_basis"])) if pos.get("cost_basis") else None
        expiry_str = pos.get("expiry")
        right = pos.get("right")

        # Get current price for moneyness
        price_data = get_latest_price(underlying)
        current_price = Decimal(str(price_data["close"])) if price_data else None

        enriched = {
            "symbol": symbol,
            "underlying": underlying,
            "asset_class": asset_class,
            "quantity": int(Decimal(str(pos["quantity"]))) if pos.get("quantity") else 0,
            "cost_basis": float(Decimal(str(pos["cost_basis"]))) if pos.get("cost_basis") else 0,
            "market_value": float(Decimal(str(pos["market_value"]))) if pos.get("market_value") else 0,
            "unrealized_pnl": float(Decimal(str(pos["unrealized_pnl"]))) if pos.get("unrealized_pnl") else 0,
            "strike": strike,
            "right": right,
            "expiry": expiry_str,
            "dte": _compute_dte(expiry_str, today),
            "moneyness": _compute_moneyness(strike, current_price, right),
            "breakeven": _compute_breakeven(strike, cost_basis, quantity, right),
        }
        enriched_positions.append(enriched)

    # Build underlying context
    underlyings_ctx: dict[str, dict] = {}
    lookback_start = today - timedelta(days=365)

    for underlying in sorted(underlyings_seen):
        price_data = get_latest_price(underlying)
        current_price = float(price_data["close"]) if price_data else None

        # Daily prices for technicals + vol
        daily_rows = get_daily_prices(underlying, date_from=lookback_start)
        realized_vol = _compute_realized_vol(daily_rows)

        # Technical signals
        tech_signals = {}
        ma_flags = {}
        if daily_rows and len(daily_rows) >= 15:
            df = pd.DataFrame(daily_rows)
            raw_signals = compute_signals(df)
            scored = score_signals(raw_signals)
            ma_flags = compute_ma_flags(df)
            tech_signals = {
                "raw": {k: v for k, v in raw_signals.items() if v is not None},
                "scores": {k: v for k, v in scored.items() if v is not None},
            }

        # Fundamentals (raw metrics)
        metrics = get_latest_stock_metrics(underlying)
        fundamentals = {}
        for name, row in metrics.items():
            val = row.get("metric_value")
            if val is not None:
                fundamentals[name] = _format_number(val)

        # Valuation snapshot (precomputed ratios & scores)
        valuation_snaps = get_latest_valuation_snapshots([underlying])
        valuation = valuation_snaps.get(underlying, {})

        # Recent trades for this underlying
        recent_trades = get_trades(account_id=account_id)
        underlying_trades = [
            t for t in recent_trades
            if _extract_underlying(t["symbol"], t["asset_class"]) == underlying
        ]
        # Limit to last 20 trades
        trade_summary = []
        for t in underlying_trades[:20]:
            trade_summary.append({
                "date": str(t.get("trade_date", ""))[:10],
                "symbol": t["symbol"],
                "side": t["side"],
                "quantity": str(t["quantity"]),
                "price": str(t["price"]),
                "realized_pnl": str(t.get("realized_pnl", "0")),
            })

        vol_override = vol_overrides.get(underlying)

        underlyings_ctx[underlying] = {
            "current_price": current_price,
            "realized_vol_20d": realized_vol,
            "volatility_override": vol_override,
            "fundamentals": fundamentals,
            "valuation": valuation,
            "technical_signals": tech_signals,
            "ma_flags": ma_flags,
            "recent_trades": trade_summary,
        }

    # ── Beta calculation vs benchmarks ─────────────────────────────────────
    beta_data: dict[str, dict] = {}  # {benchmark: {portfolio_beta, betas, ...}}
    for bench in sorted(BENCHMARKS):
        bench_prices = get_daily_prices(bench, date_from=lookback_start)
        if not bench_prices:
            continue

        # Build holdings dict for beta computation
        holdings_for_beta: dict[str, dict] = {}
        for underlying in sorted(underlyings_seen):
            sym_prices = get_daily_prices(underlying, date_from=lookback_start)
            # Use market value sum across all positions for this underlying
            mv = sum(
                abs(float(p.get("market_value", 0) or 0))
                for p in positions_raw
                if _extract_underlying(p["symbol"], p["asset_class"]) == underlying
            )
            holdings_for_beta[underlying] = {
                "market_value": mv,
                "prices": sym_prices,
            }

        result = compute_portfolio_beta(holdings_for_beta, bench_prices)
        beta_data[bench] = result

        # Inject per-symbol beta into underlyings context
        for sym, beta_val in result["betas"].items():
            if sym in underlyings_ctx:
                underlyings_ctx[sym][f"beta_{bench.lower()}"] = beta_val
                dollar_beta = result["dollar_betas"].get(sym)
                underlyings_ctx[sym][f"dollar_beta_{bench.lower()}"] = dollar_beta

    return {
        "account_id": account_id,
        "as_of_date": latest_date.isoformat(),
        "positions": enriched_positions,
        "underlyings": underlyings_ctx,
        "beta": beta_data,
    }


# ── Serialization ───────────────────────────────────────────────────────────


def _moneyness_str(moneyness: float | None) -> str:
    """Format moneyness as a readable string."""
    if moneyness is None:
        return "N/A"
    if moneyness > 0.01:
        return f"ITM ({moneyness:+.1%})"
    elif moneyness < -0.01:
        return f"OTM ({moneyness:+.1%})"
    return "ATM"


def _fmt_pct(val, mult: float = 1.0) -> str:
    """Format a value as percentage, or N/A."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val) * mult:.1%}"
    except (TypeError, ValueError):
        return str(val)


def _fmt_ratio(val) -> str:
    """Format a ratio value, or N/A."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return str(val)


def serialize_context(ctx: dict) -> str:
    """Convert the context dict into a markdown string with tables for the Claude prompt."""
    lines: list[str] = []

    lines.append(f"# Portfolio Context (as of {ctx.get('as_of_date', 'unknown')})")
    lines.append(f"Account: {ctx.get('account_id', 'unknown')}")
    lines.append("")

    # ── Positions table ──────────────────────────────────────────────────────
    positions = ctx.get("positions", [])
    if not positions:
        lines.append("## Positions\nNo open positions.")
    else:
        lines.append(f"## Positions ({len(positions)} total)")
        lines.append("")

        # Options positions table
        opt_positions = [p for p in positions if p.get("asset_class") == "OPT"]
        stk_positions = [p for p in positions if p.get("asset_class") != "OPT"]

        if opt_positions:
            lines.append("### Options Positions")
            lines.append("")
            lines.append(
                "| Underlying | Type | Strike | Expiry | DTE | Moneyness | "
                "Qty | Breakeven | Cost | Value | P&L |"
            )
            lines.append(
                "|------------|------|--------|--------|-----|-----------|"
                "-----|-----------|------|-------|-----|"
            )
            for pos in opt_positions:
                right_label = "Call" if pos.get("right") == "C" else "Put"
                lines.append(
                    f"| {pos.get('underlying', '?')} "
                    f"| {right_label} "
                    f"| {pos.get('strike', '?')} "
                    f"| {pos.get('expiry', '?')} "
                    f"| {pos.get('dte', '?')} "
                    f"| {_moneyness_str(pos.get('moneyness'))} "
                    f"| {pos.get('quantity', '?')} "
                    f"| {pos.get('breakeven', 'N/A')} "
                    f"| {_format_number(pos.get('cost_basis'))} "
                    f"| {_format_number(pos.get('market_value'))} "
                    f"| {_format_number(pos.get('unrealized_pnl'))} |"
                )
            lines.append("")

        if stk_positions:
            lines.append("### Stock/ETF Positions")
            lines.append("")
            lines.append("| Symbol | Type | Qty | Cost | Value | P&L |")
            lines.append("|--------|------|-----|------|-------|-----|")
            for pos in stk_positions:
                lines.append(
                    f"| {pos.get('symbol', '?')} "
                    f"| {pos.get('asset_class', '?')} "
                    f"| {pos.get('quantity', '?')} "
                    f"| {_format_number(pos.get('cost_basis'))} "
                    f"| {_format_number(pos.get('market_value'))} "
                    f"| {_format_number(pos.get('unrealized_pnl'))} |"
                )
            lines.append("")

    # ── Portfolio Beta ───────────────────────────────────────────────────────
    beta_data = ctx.get("beta", {})
    if beta_data:
        lines.append("## Portfolio Beta")
        lines.append("")
        lines.append("| Benchmark | Portfolio Beta | Dollar Beta |")
        lines.append("|-----------|---------------|-------------|")
        for bench, data in sorted(beta_data.items()):
            pb = data.get("portfolio_beta")
            db = data.get("portfolio_dollar_beta", 0)
            pb_str = f"{pb:.2f}" if pb is not None else "N/A"
            db_str = f"${db:,.0f}" if db else "N/A"
            lines.append(f"| {bench} | {pb_str} | {db_str} |")
        lines.append("")

    # ── Underlying analysis tables ───────────────────────────────────────────
    underlyings = ctx.get("underlyings", {})
    if underlyings:
        lines.append("## Underlying Analysis")
        lines.append("")

        for sym, data in sorted(underlyings.items()):
            lines.append(f"### {sym}")

            price = data.get("current_price")
            vol = data.get("realized_vol_20d")
            vol_override = data.get("volatility_override")

            lines.append(f"- **Current Price**: {_format_number(price)}")
            if vol_override is not None:
                vol_line = f"- **Volatility**: {vol_override:.1%} (user override)"
                if vol is not None:
                    vol_line += f" | Realized 20d: {vol:.1%}"
                lines.append(vol_line)
            elif vol is not None:
                lines.append(f"- **Realized Vol 20d**: {vol:.1%}")

            # Beta vs benchmarks
            for bench in sorted(BENCHMARKS):
                beta_val = data.get(f"beta_{bench.lower()}")
                dollar_beta_val = data.get(f"dollar_beta_{bench.lower()}")
                if beta_val is not None:
                    db_str = f" (${dollar_beta_val:,.0f} dollar beta)" if dollar_beta_val else ""
                    lines.append(f"- **Beta vs {bench}**: {beta_val:.2f}{db_str}")

            lines.append("")

            # ── Fundamental Evaluation Table ─────────────────────────────────
            valuation = data.get("valuation", {})
            fundamentals = data.get("fundamentals", {})

            if valuation or fundamentals:
                lines.append("#### Fundamental Evaluation")
                lines.append("")
                lines.append("| Category | Metric | Value |")
                lines.append("|----------|--------|-------|")

                # Valuation ratios (from precomputed snapshot)
                val_metrics = [
                    ("Valuation", "P/E (TTM)", valuation.get("pe_ttm")),
                    ("Valuation", "P/B", valuation.get("pb")),
                    ("Valuation", "P/S", valuation.get("ps")),
                    ("Valuation", "EV/EBITDA", valuation.get("ev_ebitda")),
                    ("Valuation", "EV/Revenue", valuation.get("ev_revenue")),
                    ("Valuation", "PEG", valuation.get("peg")),
                    ("Valuation", "Earnings Yield", valuation.get("earnings_yield")),
                    ("Valuation", "Market Cap", valuation.get("market_cap")),
                ]
                for cat, name, val in val_metrics:
                    if val is not None:
                        if name in ("Earnings Yield",):
                            lines.append(f"| {cat} | {name} | {_fmt_pct(val)} |")
                        elif name == "Market Cap":
                            lines.append(f"| {cat} | {name} | {_format_number(val)} |")
                        else:
                            lines.append(f"| {cat} | {name} | {_fmt_ratio(val)} |")

                # Profitability
                prof_metrics = [
                    ("Profitability", "Gross Margin", valuation.get("gross_margin")),
                    ("Profitability", "Operating Margin", valuation.get("operating_margin")),
                    ("Profitability", "Net Margin", valuation.get("net_margin")),
                    ("Profitability", "ROE", valuation.get("roe")),
                    ("Profitability", "ROA", valuation.get("roa")),
                ]
                for cat, name, val in prof_metrics:
                    if val is not None:
                        lines.append(f"| {cat} | {name} | {_fmt_pct(val)} |")

                # Financial Health
                health_metrics = [
                    ("Health", "Debt/Equity", valuation.get("debt_to_equity")),
                    ("Health", "Current Ratio", valuation.get("current_ratio")),
                    ("Health", "Interest Coverage", valuation.get("interest_coverage")),
                    ("Health", "Dividend Yield", valuation.get("dividend_yield")),
                ]
                for cat, name, val in health_metrics:
                    if val is not None:
                        if name == "Dividend Yield":
                            lines.append(f"| {cat} | {name} | {_fmt_pct(val)} |")
                        else:
                            lines.append(f"| {cat} | {name} | {_fmt_ratio(val)} |")

                # Growth
                growth_metrics = [
                    ("Growth", "Revenue Growth", valuation.get("revenue_growth")),
                    ("Growth", "EPS Growth", valuation.get("eps_growth")),
                    ("Growth", "Net Income Growth", valuation.get("net_income_growth")),
                ]
                for cat, name, val in growth_metrics:
                    if val is not None:
                        lines.append(f"| {cat} | {name} | {_fmt_pct(val)} |")

                # Scores (from valuation snapshot)
                score_fields = [
                    ("Score", "Composite", valuation.get("score_composite")),
                    ("Score", "Valuation", valuation.get("score_valuation")),
                    ("Score", "Profitability", valuation.get("score_profitability")),
                    ("Score", "Health", valuation.get("score_health")),
                    ("Score", "Growth", valuation.get("score_growth")),
                ]
                for cat, name, val in score_fields:
                    if val is not None:
                        lines.append(f"| {cat} | {name} | {_fmt_ratio(val)}/100 |")

                # Raw fundamentals not covered by valuation snapshot
                shown = {
                    "pe_ttm", "pb", "ps", "ev_ebitda", "ev_revenue", "peg",
                    "earnings_yield", "market_cap", "gross_margin",
                    "operating_margin", "net_margin", "roe", "roa",
                    "debt_to_equity", "current_ratio", "interest_coverage",
                    "dividend_yield", "revenue_growth", "eps_growth",
                    "net_income_growth",
                }
                for name, val in fundamentals.items():
                    if name not in shown:
                        lines.append(f"| Fundamental | {name} | {val} |")

                lines.append("")

            # ── Technical Evaluation Table ───────────────────────────────────
            tech = data.get("technical_signals", {})
            raw = tech.get("raw", {})
            scores = tech.get("scores", {})
            ma_flags = data.get("ma_flags", {})

            if scores:
                lines.append("#### Technical Evaluation")
                lines.append("")
                lines.append("| Signal | Raw Value | Score (0-100) |")
                lines.append("|--------|-----------|---------------|")

                for key, label in SIGNAL_LABELS.items():
                    raw_val = raw.get(key)
                    score_val = scores.get(key)
                    if raw_val is not None or score_val is not None:
                        if key in ("momentum_12_1", "roc_20", "realized_vol_20",
                                   "atr_pct", "sma_trend"):
                            raw_str = f"{raw_val:.1%}" if raw_val is not None else "N/A"
                        elif key == "rsi_14":
                            raw_str = f"{raw_val:.1f}" if raw_val is not None else "N/A"
                        elif key == "volume_trend":
                            raw_str = f"{raw_val:.2f}x" if raw_val is not None else "N/A"
                        elif key == "bollinger_pctb":
                            raw_str = f"{raw_val:.2f}" if raw_val is not None else "N/A"
                        else:
                            raw_str = f"{raw_val:.4f}" if raw_val is not None else "N/A"
                        score_str = f"{score_val:.0f}" if score_val is not None else "N/A"
                        lines.append(f"| {label} | {raw_str} | {score_str} |")

                # MA flags
                if ma_flags:
                    for key, label in [("above_sma50", "SMA 50"), ("above_sma100", "SMA 100"), ("above_sma200", "SMA 200")]:
                        flag = ma_flags.get(key)
                        if flag is not None:
                            status = "Above" if flag else "Below"
                            lines.append(f"| {label} | {status} | — |")

                lines.append("")

            # ── Recent Trades ────────────────────────────────────────────────
            trades = data.get("recent_trades", [])
            if trades:
                lines.append("#### Recent Trades")
                lines.append("")
                lines.append("| Date | Side | Qty | Symbol | Price | P&L |")
                lines.append("|------|------|-----|--------|-------|-----|")
                for t in trades[:10]:
                    lines.append(
                        f"| {t['date']} | {t['side']} | {t['quantity']} "
                        f"| {t['symbol']} | {t['price']} | {t['realized_pnl']} |"
                    )
                if len(trades) > 10:
                    lines.append(f"\n*... and {len(trades) - 10} more trades*")
                lines.append("")

    return "\n".join(lines)
