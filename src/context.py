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

from src.db import (
    get_daily_prices,
    get_latest_price,
    get_latest_stock_metrics,
    get_positions_as_of,
    get_snapshot_dates,
    get_trades,
    get_account_ids,
)
from src.technical import compute_signals, score_signals

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
        if daily_rows and len(daily_rows) >= 15:
            df = pd.DataFrame(daily_rows)
            raw_signals = compute_signals(df)
            scored = score_signals(raw_signals)
            tech_signals = {
                "raw": {k: v for k, v in raw_signals.items() if v is not None},
                "scores": {k: v for k, v in scored.items() if v is not None},
            }

        # Fundamentals
        metrics = get_latest_stock_metrics(underlying)
        fundamentals = {}
        for name, row in metrics.items():
            val = row.get("metric_value")
            if val is not None:
                fundamentals[name] = _format_number(val)

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
            "technical_signals": tech_signals,
            "recent_trades": trade_summary,
        }

    return {
        "account_id": account_id,
        "as_of_date": latest_date.isoformat(),
        "positions": enriched_positions,
        "underlyings": underlyings_ctx,
    }


# ── Serialization ───────────────────────────────────────────────────────────


def serialize_context(ctx: dict) -> str:
    """Convert the context dict into a markdown string for the Claude prompt."""
    lines: list[str] = []

    lines.append(f"# Portfolio Context (as of {ctx.get('as_of_date', 'unknown')})")
    lines.append(f"Account: {ctx.get('account_id', 'unknown')}")
    lines.append("")

    # Positions
    positions = ctx.get("positions", [])
    if not positions:
        lines.append("## Positions\nNo open positions.")
    else:
        lines.append(f"## Positions ({len(positions)} total)")
        lines.append("")

        # Group by underlying
        by_underlying: dict[str, list[dict]] = {}
        for pos in positions:
            u = pos.get("underlying", pos.get("symbol", "?"))
            by_underlying.setdefault(u, []).append(pos)

        for underlying, pos_list in sorted(by_underlying.items()):
            lines.append(f"### {underlying}")

            for pos in pos_list:
                ac = pos.get("asset_class", "?")
                if ac == "OPT":
                    right_label = "Call" if pos.get("right") == "C" else "Put"
                    moneyness = pos.get("moneyness")
                    if moneyness is not None:
                        if moneyness > 0.01:
                            money_str = f"ITM ({moneyness:+.1%})"
                        elif moneyness < -0.01:
                            money_str = f"OTM ({moneyness:+.1%})"
                        else:
                            money_str = "ATM"
                    else:
                        money_str = "Moneyness: N/A"

                    lines.append(
                        f"- **{right_label}** | Strike: {pos.get('strike', '?')} | "
                        f"DTE: {pos.get('dte', '?')} | {money_str} | "
                        f"Qty: {pos.get('quantity', '?')} contracts"
                    )
                    if pos.get("breakeven"):
                        lines.append(f"  Breakeven: {pos['breakeven']}")
                else:
                    lines.append(
                        f"- **{ac}** | Qty: {pos.get('quantity', '?')} shares"
                    )

                lines.append(
                    f"  Cost: {_format_number(pos.get('cost_basis'))} | "
                    f"Value: {_format_number(pos.get('market_value'))} | "
                    f"P&L: {_format_number(pos.get('unrealized_pnl'))}"
                )
            lines.append("")

    # Underlyings
    underlyings = ctx.get("underlyings", {})
    if underlyings:
        lines.append("## Underlying Analysis")
        lines.append("")

        for sym, data in sorted(underlyings.items()):
            lines.append(f"### {sym}")

            price = data.get("current_price")
            lines.append(f"- Current Price: {_format_number(price)}")

            vol = data.get("realized_vol_20d")
            vol_override = data.get("volatility_override")
            if vol_override is not None:
                lines.append(
                    f"- Volatility: {vol_override:.1%} (user override) | "
                    f"Realized 20d: {vol:.1%}" if vol else
                    f"- Volatility: {vol_override:.1%} (user override)"
                )
            elif vol is not None:
                lines.append(f"- Realized Vol 20d: {vol:.1%}")

            # Fundamentals
            fundamentals = data.get("fundamentals", {})
            if fundamentals:
                fund_items = [f"{k}: {v}" for k, v in fundamentals.items()]
                lines.append(f"- Fundamentals: {' | '.join(fund_items)}")

            # Technicals
            tech = data.get("technical_signals", {})
            scores = tech.get("scores", {})
            if scores:
                top_signals = sorted(scores.items(), key=lambda x: x[1] or 0, reverse=True)[:5]
                sig_items = [f"{k}: {v:.0f}" for k, v in top_signals if v is not None]
                if sig_items:
                    lines.append(f"- Technical Scores (top 5): {' | '.join(sig_items)}")

            # Recent trades
            trades = data.get("recent_trades", [])
            if trades:
                lines.append(f"- Recent Trades ({len(trades)}):")
                for t in trades[:5]:
                    lines.append(
                        f"  {t['date']} {t['side']} {t['quantity']}x "
                        f"{t['symbol']} @ {t['price']} "
                        f"(P&L: {t['realized_pnl']})"
                    )
                if len(trades) > 5:
                    lines.append(f"  ... and {len(trades) - 5} more trades")

            lines.append("")

    return "\n".join(lines)
