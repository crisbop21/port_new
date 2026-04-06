"""Holdings page — view positions as of any snapshot date and reconcile."""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

from src.db import (
    get_account_ids,
    get_positions_as_of,
    get_snapshot_dates,
    reconcile_account,
    reconcile_pair,
)

# Beta imports — ALL beta-related imports guarded so the page still works
_BETA_AVAILABLE = False
_beta_import_err_msg = ""
try:
    from src.db import get_daily_prices, upsert_daily_prices
    from src.beta import (
        compute_portfolio_beta, compute_option_delta, compute_option_beta,
        BENCHMARKS, DEFAULT_RISK_FREE_RATE,
    )
    from src.price_fetcher import fetch_daily_prices
    _BETA_AVAILABLE = True
    logger.info("Beta module loaded successfully")
except Exception as _beta_import_err:
    logger.exception("Failed to import beta module")
    _beta_import_err_msg = str(_beta_import_err)

st.title("Holdings")

# ── Account selector ────────────────────────────────────────────────────────

account_ids = get_account_ids()

if not account_ids:
    st.info("No statements uploaded yet. Go to **Upload** to import a PDF.")
    st.stop()

account_options = ["All Accounts"] + account_ids
selected_account = st.selectbox("Account", account_options)

# ── Collect snapshot dates per account ──────────────────────────────────────

if selected_account == "All Accounts":
    target_accounts = account_ids
else:
    target_accounts = [selected_account]

snapshot_dates_by_acct: dict[str, list[date]] = {}
for acct in target_accounts:
    snapshot_dates_by_acct[acct] = get_snapshot_dates(acct)

all_snapshot_dates = sorted(
    {d for dates in snapshot_dates_by_acct.values() for d in dates}
)

if not all_snapshot_dates:
    st.warning("No position snapshots found. Upload a statement with positions.")
    st.stop()

# ── Date picker ─────────────────────────────────────────────────────────────

as_of = st.date_input(
    "As-of date (snapshot)",
    value=all_snapshot_dates[-1],
    min_value=all_snapshot_dates[0],
    max_value=all_snapshot_dates[-1],
    help="Select a date that matches a position snapshot (statement_date).",
)

is_exact_snapshot = as_of in all_snapshot_dates

if not is_exact_snapshot:
    st.warning(
        f"**{as_of}** does not match any snapshot date. "
        f"Available snapshot dates: {', '.join(d.isoformat() for d in all_snapshot_dates)}"
    )
    st.stop()

# ── Load holdings ───────────────────────────────────────────────────────────

with st.spinner("Loading holdings..."):
    rows: list[dict] = []
    for acct in target_accounts:
        positions = get_positions_as_of(acct, as_of)
        for p in positions:
            multiplier = 100 if p.get("asset_class") == "OPT" else 1
            # cost_basis from IBKR is already total cost for the position
            cost_value = abs(float(p["cost_basis"]))
            rows.append({
                **p,
                "multiplier": multiplier,
                "cost_value": cost_value,
            })

if not rows:
    st.warning("No holdings found as of this date.")
    st.stop()

df = pd.DataFrame(rows)

# Convert numeric columns
numeric_cols = [
    "quantity", "cost_basis", "cost_value",
    "market_price", "market_value", "unrealized_pnl", "strike",
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Metric cards ────────────────────────────────────────────────────────────

has_market_data = "market_value" in df.columns and df["market_value"].notna().any()

if has_market_data:
    total_market_value = df["market_value"].sum()
    total_unrealized = df["unrealized_pnl"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Market Value", f"${total_market_value:,.2f}")
    col2.metric("Unrealized P&L", f"${total_unrealized:,.2f}",
                delta=f"{total_unrealized:,.2f}")
    col3.metric("Positions", len(df))
else:
    total_cost_value = df["cost_value"].sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Cost Value", f"${total_cost_value:,.2f}")
    col2.metric("Positions", len(df))
    col3.metric("As-of", str(as_of))
    st.caption(
        "Market value and unrealized P&L may be unavailable. "
        "Cost value is the total cost basis reported by IBKR."
    )

st.divider()

# ── Beta vs Benchmark ──────────────────────────────────────────────────────

st.subheader("Portfolio Beta")

if not _BETA_AVAILABLE:
    st.error(f"Beta module failed to load: {_beta_import_err_msg}")
    logger.error("Beta section skipped — import failed: %s", _beta_import_err_msg)
else:
    logger.info("Rendering beta section")

    beta_col1, beta_col2 = st.columns([1, 1])
    with beta_col1:
        benchmark = st.selectbox(
            "Benchmark",
            options=sorted(BENCHMARKS),
            index=0,  # QQQ first alphabetically
            help="Select the benchmark index to compute beta against.",
        )
    with beta_col2:
        calculate_beta = st.button("Calculate Beta", type="primary")

    # Collect unique stock/ETF symbols for beta (use underlying for options)
    beta_symbols: set[str] = set()
    for _, row in df.iterrows():
        sym = row["symbol"]
        ac = row["asset_class"]
        if ac == "OPT" and " " in sym:
            beta_symbols.add(sym.split(" ")[0])
        else:
            beta_symbols.add(sym)

    logger.info("Beta symbols extracted: %s", sorted(beta_symbols))

    # Store beta results in session state so they persist across reruns
    if "beta_result" not in st.session_state:
        st.session_state.beta_result = None
        st.session_state.beta_benchmark = None

    if calculate_beta:
        lookback_start = as_of - timedelta(days=400)  # buffer for 252 trading days
        logger.info(
            "Calculate Beta clicked — benchmark=%s, lookback=%s to %s, symbols=%s",
            benchmark, lookback_start, as_of, sorted(beta_symbols),
        )

        with st.spinner(f"Fetching prices and computing beta vs {benchmark}..."):
            try:
                # Fetch benchmark prices (auto-fetch if missing)
                bench_prices = get_daily_prices(benchmark, date_from=lookback_start, date_to=as_of)
                logger.info("Benchmark %s: %d price rows from DB", benchmark, len(bench_prices))

                if len(bench_prices) < 60:
                    st.info(f"Fetching {benchmark} prices from Yahoo Finance...")
                    fetched, errs = fetch_daily_prices(benchmark, start=lookback_start, end=as_of)
                    logger.info("Fetched %d rows for %s from yfinance (errors: %s)", len(fetched), benchmark, errs)
                    if fetched:
                        upsert_daily_prices(fetched)
                        bench_prices = get_daily_prices(benchmark, date_from=lookback_start, date_to=as_of)
                        logger.info("After upsert, benchmark %s: %d rows", benchmark, len(bench_prices))
                    if errs:
                        for e in errs:
                            st.warning(f"Price fetch warning: {e}")

                if len(bench_prices) < 60:
                    st.error(
                        f"Not enough price data for **{benchmark}** ({len(bench_prices)} rows, "
                        f"need at least 60). Go to **Prices** page and click "
                        f"**Fetch All Portfolio Prices** — it now includes {benchmark} automatically."
                    )
                else:
                    # ── Step 1: compute raw underlying betas ──────────────
                    holdings_for_beta: dict[str, dict] = {}
                    for sym in sorted(beta_symbols):
                        sym_prices = get_daily_prices(sym, date_from=lookback_start, date_to=as_of)
                        logger.info("Symbol %s: %d price rows from DB", sym, len(sym_prices))

                        if len(sym_prices) < 60:
                            st.info(f"Fetching {sym} prices from Yahoo Finance...")
                            fetched, errs = fetch_daily_prices(sym, start=lookback_start, end=as_of)
                            logger.info("Fetched %d rows for %s from yfinance", len(fetched), sym)
                            if fetched:
                                upsert_daily_prices(fetched)
                                sym_prices = get_daily_prices(sym, date_from=lookback_start, date_to=as_of)
                            if errs:
                                for e in errs:
                                    st.warning(f"{sym}: {e}")

                        # Market value for stocks/ETFs only (options handled separately)
                        mv = 0.0
                        for _, r in df.iterrows():
                            if r["asset_class"] in ("STK", "ETF") and r["symbol"] == sym:
                                mv += abs(float(r.get("market_value", 0) or 0))

                        holdings_for_beta[sym] = {"market_value": mv, "prices": sym_prices}

                    # Get raw underlying betas (used for stocks + as input for option beta)
                    raw_result = compute_portfolio_beta(holdings_for_beta, bench_prices)
                    underlying_betas = raw_result["betas"]
                    logger.info("Underlying betas: %s", underlying_betas)

                    # ── Step 2: compute per-position effective beta ───────
                    # For STK/ETF: effective_beta = underlying_beta
                    # For OPT: effective_beta = option_beta (delta × leverage adjusted)
                    position_betas: list[dict] = []  # {symbol, effective_beta, market_value}

                    for _, r in df.iterrows():
                        sym = r["symbol"]
                        ac = r["asset_class"]
                        mv = abs(float(r.get("market_value", 0) or 0))
                        underlying = sym.split(" ")[0] if ac == "OPT" and " " in sym else sym
                        u_beta = underlying_betas.get(underlying)

                        if ac in ("STK", "ETF"):
                            position_betas.append({
                                "symbol": sym, "effective_beta": u_beta,
                                "market_value": mv,
                            })
                        elif ac == "OPT":
                            # Compute option beta using delta + leverage
                            strike_val = float(r.get("strike", 0) or 0)
                            right_val = r.get("right")
                            expiry_val = r.get("expiry")
                            mkt_price = abs(float(r.get("market_price", 0) or 0))

                            # Get underlying price
                            u_price = None
                            for _, ur in df.iterrows():
                                if ur["symbol"] == underlying and ur["asset_class"] in ("STK", "ETF"):
                                    u_price = float(ur.get("market_price", 0) or 0)
                                    break
                            if not u_price or u_price <= 0:
                                latest_p = get_daily_prices(underlying, date_from=as_of - timedelta(days=10), date_to=as_of)
                                if latest_p:
                                    u_price = float(latest_p[-1].get("close", 0) or 0)

                            # DTE
                            dte_years = None
                            if expiry_val:
                                try:
                                    exp_d = date.fromisoformat(str(expiry_val)) if isinstance(expiry_val, str) else expiry_val
                                    dte_years = max((exp_d - as_of).days, 0) / 365.0
                                except (ValueError, TypeError):
                                    pass

                            # Realized vol
                            sigma = 0.30
                            vol_prices = get_daily_prices(underlying, date_from=as_of - timedelta(days=60), date_to=as_of)
                            if len(vol_prices) >= 21:
                                vdf = pd.DataFrame(vol_prices)
                                vdf["adj_close"] = pd.to_numeric(vdf["adj_close"], errors="coerce")
                                vdf = vdf.sort_values("price_date")
                                lr = np.log(vdf["adj_close"] / vdf["adj_close"].shift(1))
                                cv = lr.rolling(20).std().iloc[-1] * np.sqrt(252)
                                if pd.notna(cv) and cv > 0:
                                    sigma = float(cv)

                            ob = None
                            if (u_price and u_price > 0 and strike_val > 0
                                    and dte_years and dte_years > 0 and right_val
                                    and mkt_price > 0):
                                ob = compute_option_beta(
                                    underlying_beta=u_beta,
                                    underlying_price=u_price,
                                    option_price=mkt_price,
                                    strike=strike_val,
                                    dte_years=dte_years,
                                    sigma=sigma,
                                    right=right_val,
                                )

                            position_betas.append({
                                "symbol": sym, "effective_beta": ob,
                                "market_value": mv,
                            })

                    # ── Step 3: aggregate portfolio beta ──────────────────
                    total_mv = sum(p["market_value"] for p in position_betas if p["effective_beta"] is not None)
                    if total_mv > 0:
                        port_beta = sum(
                            p["effective_beta"] * p["market_value"]
                            for p in position_betas if p["effective_beta"] is not None
                        ) / total_mv
                    else:
                        port_beta = None

                    port_dollar_beta = sum(
                        p["effective_beta"] * p["market_value"]
                        for p in position_betas if p["effective_beta"] is not None
                    )

                    result = {
                        "betas": underlying_betas,
                        "dollar_betas": raw_result["dollar_betas"],
                        "portfolio_beta": port_beta,
                        "portfolio_dollar_beta": port_dollar_beta,
                        "position_betas": position_betas,
                    }

                    logger.info(
                        "Portfolio beta (option-adjusted): %.3f, dollar_beta: %.0f",
                        port_beta or 0, port_dollar_beta,
                    )
                    st.session_state.beta_result = result
                    st.session_state.beta_benchmark = benchmark
                    st.success("Beta calculation complete!")

            except Exception as e:
                logger.exception("Beta calculation failed")
                st.error(f"Beta calculation error: {e}")

    # Display beta results if available
    beta_result = st.session_state.get("beta_result")
    beta_benchmark = st.session_state.get("beta_benchmark")

    if beta_result is not None and beta_benchmark is not None:
        if beta_result["portfolio_beta"] is not None:
            bc1, bc2, bc3 = st.columns(3)
            bc1.metric(f"Portfolio Beta vs {beta_benchmark}", f"{beta_result['portfolio_beta']:.2f}")
            bc2.metric(
                f"Dollar Beta vs {beta_benchmark}",
                f"${beta_result['portfolio_dollar_beta']:,.0f}",
                help="For every 1% move in the benchmark, portfolio moves by approx this dollar amount / 100.",
            )
            valid_count = sum(1 for b in beta_result["betas"].values() if b is not None)
            bc3.metric("Symbols with Beta", f"{valid_count}/{len(beta_result['betas'])}")

            # Diagnostic expander
            with st.expander("Beta details per symbol"):
                beta_detail_rows = []
                for sym in sorted(beta_result["betas"].keys()):
                    b = beta_result["betas"][sym]
                    db = beta_result["dollar_betas"][sym]
                    beta_detail_rows.append({
                        "Symbol": sym,
                        "Beta": f"{b:.3f}" if b is not None else "N/A (insufficient data)",
                        "Dollar Beta": f"${db:,.0f}" if db is not None else "N/A",
                    })
                st.dataframe(pd.DataFrame(beta_detail_rows), use_container_width=True, hide_index=True)
        else:
            st.warning(
                f"Could not compute portfolio beta. Ensure daily prices are fetched "
                f"for your holdings and {beta_benchmark} (go to **Prices** page)."
            )
    else:
        st.caption("Click **Calculate Beta** to compute portfolio beta vs the selected benchmark.")

st.divider()

# ── Consolidated market value per symbol ───────────────────────────────────

st.subheader("Market Value by Symbol" if has_market_data else "Cost Basis by Symbol")

consol_rows = []
for _, row in df.iterrows():
    if has_market_data and pd.notna(row.get("market_value")):
        value = abs(float(row["market_value"]))
    elif row["asset_class"] == "OPT":
        value = float(row.get("strike", 0) or 0) * 100 * abs(float(row["quantity"]))
    else:
        value = abs(float(row["quantity"])) * float(row["cost_basis"])
    qty = abs(float(row["quantity"]))
    # cost_basis from IBKR is already total cost for the position
    cost = abs(float(row.get("cost_basis", 0) or 0))
    consol_rows.append({
        "symbol": row["symbol"],
        "market_value": value,
        "total_cost": cost,
        "quantity": qty,
        "asset_class": row["asset_class"],
        "strike": float(row.get("strike", 0) or 0),
        "right": row.get("right"),
    })

consol_df = (
    pd.DataFrame(consol_rows)
    .groupby("symbol", as_index=False)
    .agg(
        market_value=("market_value", "sum"),
        total_cost=("total_cost", "sum"),
        quantity=("quantity", "sum"),
        asset_class=("asset_class", "first"),
        strike=("strike", "first"),
        right=("right", "first"),
    )
)

# Breakeven calculation:
# STK/ETF: weighted average cost basis = total_cost / quantity
# OPT calls: strike + avg premium per share (total_cost / quantity / 100)
# OPT puts:  strike - avg premium per share (total_cost / quantity / 100)
safe_qty = consol_df["quantity"].replace(0, float("nan"))
is_opt = consol_df["asset_class"] == "OPT"
avg_premium = consol_df["total_cost"] / safe_qty / 100  # per-share premium for options
is_call = consol_df["right"] == "C"

consol_df["breakeven"] = float("nan")
consol_df.loc[~is_opt, "breakeven"] = consol_df.loc[~is_opt, "total_cost"] / safe_qty[~is_opt]
consol_df.loc[is_opt & is_call, "breakeven"] = (
    consol_df.loc[is_opt & is_call, "strike"] + avg_premium[is_opt & is_call]
)
consol_df.loc[is_opt & ~is_call, "breakeven"] = (
    consol_df.loc[is_opt & ~is_call, "strike"] - avg_premium[is_opt & ~is_call]
)
total_mv = consol_df["market_value"].sum()
consol_df["pct_of_account"] = (
    (consol_df["market_value"] / total_mv * 100) if total_mv else 0.0
)
consol_df = consol_df.sort_values("market_value", ascending=False).reset_index(drop=True)

# Add beta and dollar beta columns if beta has been calculated
_beta_result_for_table = st.session_state.get("beta_result") if _BETA_AVAILABLE else None
_beta_bench_for_table = st.session_state.get("beta_benchmark") if _BETA_AVAILABLE else None

if _beta_result_for_table is not None:
    consol_df["beta"] = consol_df["symbol"].map(
        lambda s: _beta_result_for_table["betas"].get(s)
    )
    consol_df["dollar_beta"] = consol_df["symbol"].map(
        lambda s: _beta_result_for_table["dollar_betas"].get(s)
    )

col_table, col_chart = st.columns([1, 1])

with col_table:
    base_cols = ["symbol", "quantity", "breakeven", "market_value", "pct_of_account"]
    if _beta_result_for_table is not None:
        base_cols += ["beta", "dollar_beta"]

    display_mv = consol_df[base_cols].copy()
    display_mv["quantity"] = display_mv["quantity"].map(lambda v: f"{v:,.2f}")
    display_mv["breakeven"] = display_mv["breakeven"].map(lambda v: f"${v:,.2f}")
    display_mv["market_value"] = display_mv["market_value"].map(lambda v: f"${v:,.2f}")
    display_mv["pct_of_account"] = display_mv["pct_of_account"].map(lambda v: f"{v:.1f}%")

    rename_map = {
        "symbol": "Symbol",
        "quantity": "Total Qty",
        "breakeven": "Avg Breakeven",
        "market_value": "Market Value ($)",
        "pct_of_account": "% of Account",
    }

    if _beta_result_for_table is not None:
        display_mv["beta"] = display_mv["beta"].map(lambda v: f"{v:.2f}" if v is not None else "N/A")
        display_mv["dollar_beta"] = display_mv["dollar_beta"].map(lambda v: f"${v:,.0f}" if v is not None else "N/A")
        rename_map["beta"] = f"Beta ({_beta_bench_for_table})"
        rename_map["dollar_beta"] = "Dollar Beta"

    display_mv = display_mv.rename(columns=rename_map)
    st.dataframe(display_mv, use_container_width=True, hide_index=True)

with col_chart:
    import altair as alt

    chart_data = consol_df.copy()
    pie = (
        alt.Chart(chart_data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("market_value:Q", title="Market Value"),
            color=alt.Color("symbol:N", title="Symbol"),
            tooltip=[
                alt.Tooltip("symbol:N", title="Symbol"),
                alt.Tooltip("market_value:Q", title="Market Value", format="$,.2f"),
                alt.Tooltip("pct_of_account:Q", title="% of Account", format=".1f"),
            ],
        )
        .properties(height=350)
    )
    st.altair_chart(pie, use_container_width=True)

st.divider()

# ── Table grouped by asset class ────────────────────────────────────────────

if has_market_data:
    display_cols = [
        "symbol", "asset_class", "quantity", "cost_basis",
        "multiplier", "cost_value",
        "market_price", "market_value", "unrealized_pnl",
    ]
else:
    display_cols = ["symbol", "asset_class", "quantity", "cost_basis", "multiplier", "cost_value"]

if df["asset_class"].eq("OPT").any():
    display_cols += ["expiry", "strike", "right"]

for asset_class, group in df.groupby("asset_class", sort=True):
    label = {"STK": "Stocks", "OPT": "Options", "ETF": "ETFs"}.get(asset_class, asset_class)
    st.subheader(f"{label} ({len(group)})")

    show_cols = [c for c in display_cols if c in group.columns]
    detail_df = group[show_cols].sort_values("symbol").reset_index(drop=True).copy()

    # Compute delta and option beta for options positions
    if asset_class == "OPT" and _BETA_AVAILABLE and _beta_result_for_table is not None:
        deltas = []
        opt_betas = []
        opt_dollar_betas = []
        for _, orow in detail_df.iterrows():
            sym = orow["symbol"]
            underlying = sym.split(" ")[0] if " " in sym else sym
            strike_val = float(orow.get("strike", 0) or 0)
            right_val = orow.get("right")
            expiry_val = orow.get("expiry")
            mkt_price = float(orow.get("market_price", 0) or 0)
            qty = float(orow.get("quantity", 0) or 0)
            mkt_value = float(orow.get("market_value", 0) or 0)

            # Get underlying price from market_price of the underlying
            underlying_beta = _beta_result_for_table["betas"].get(underlying)
            underlying_price = None
            # First try: look up from STK/ETF holdings
            for _, urow in df.iterrows():
                if urow["symbol"] == underlying and urow["asset_class"] in ("STK", "ETF"):
                    underlying_price = float(urow.get("market_price", 0) or 0)
                    break
            # Fallback: get latest price from daily_prices DB (covers cases
            # like QQQ where user holds options but not the underlying stock)
            if not underlying_price or underlying_price <= 0:
                latest = get_daily_prices(underlying, date_from=as_of - timedelta(days=10), date_to=as_of)
                if latest:
                    underlying_price = float(latest[-1].get("close", 0) or 0)

            # Compute DTE in years
            dte_years = None
            if expiry_val:
                try:
                    from datetime import date as _date
                    exp_date = _date.fromisoformat(str(expiry_val)) if isinstance(expiry_val, str) else expiry_val
                    dte_days = (exp_date - as_of).days
                    dte_years = max(dte_days, 0) / 365.0
                except (ValueError, TypeError):
                    pass

            # Use realized vol from daily prices or a default
            sigma = 0.30  # default fallback
            if _BETA_AVAILABLE:
                lookback_start_vol = as_of - timedelta(days=60)
                vol_prices = get_daily_prices(underlying, date_from=lookback_start_vol, date_to=as_of)
                if len(vol_prices) >= 21:
                    vol_df = pd.DataFrame(vol_prices)
                    vol_df["adj_close"] = pd.to_numeric(vol_df["adj_close"], errors="coerce")
                    vol_df = vol_df.sort_values("price_date")
                    log_ret = np.log(vol_df["adj_close"] / vol_df["adj_close"].shift(1))
                    computed_vol = log_ret.rolling(20).std().iloc[-1] * np.sqrt(252)
                    if pd.notna(computed_vol) and computed_vol > 0:
                        sigma = float(computed_vol)

            # Option price per share
            option_price_per_share = abs(mkt_price) if mkt_price else 0

            if (underlying_price and underlying_price > 0 and strike_val > 0
                    and dte_years and dte_years > 0 and right_val and sigma > 0):
                delta = compute_option_delta(
                    underlying_price=underlying_price,
                    strike=strike_val,
                    dte_years=dte_years,
                    sigma=sigma,
                    right=right_val,
                )
                ob = compute_option_beta(
                    underlying_beta=underlying_beta,
                    underlying_price=underlying_price,
                    option_price=option_price_per_share if option_price_per_share > 0 else 0.01,
                    strike=strike_val,
                    dte_years=dte_years,
                    sigma=sigma,
                    right=right_val,
                )
            else:
                delta = None
                ob = None

            deltas.append(delta)
            opt_betas.append(ob)
            # Dollar beta = option_beta * market_value (already accounts for quantity sign via market_value)
            if ob is not None and mkt_value != 0:
                opt_dollar_betas.append(ob * abs(mkt_value))
            else:
                opt_dollar_betas.append(None)

        detail_df["delta"] = deltas
        detail_df["opt_beta"] = opt_betas
        detail_df["opt_dollar_beta"] = opt_dollar_betas
        show_cols = show_cols + ["delta", "opt_beta", "opt_dollar_beta"]

    dollar_cols = ["cost_value", "market_value", "cost_basis", "market_price", "unrealized_pnl", "strike"]
    for col in dollar_cols:
        if col in detail_df.columns:
            detail_df[col] = detail_df[col].map(lambda v: f"${v:,.2f}")
    if "multiplier" in detail_df.columns:
        detail_df["multiplier"] = detail_df["multiplier"].astype(int)

    # Format delta and option beta columns (pd.notna handles both None and NaN)
    if "delta" in detail_df.columns:
        detail_df["delta"] = detail_df["delta"].map(
            lambda v: f"{v:.3f}" if pd.notna(v) else "N/A"
        )
    if "opt_beta" in detail_df.columns:
        detail_df["opt_beta"] = detail_df["opt_beta"].map(
            lambda v: f"{v:.2f}" if pd.notna(v) else "N/A"
        )
    if "opt_dollar_beta" in detail_df.columns:
        detail_df["opt_dollar_beta"] = detail_df["opt_dollar_beta"].map(
            lambda v: f"${v:,.0f}" if pd.notna(v) else "N/A"
        )

    col_labels = {
        "cost_value": "Cost Value", "market_value": "Market Value",
        "cost_basis": "Cost Basis", "market_price": "Market Price",
        "unrealized_pnl": "Unrealized P&L", "multiplier": "Mult",
        "delta": "Delta", "opt_beta": f"Option Beta",
        "opt_dollar_beta": "Dollar Beta",
    }
    detail_df = detail_df.rename(columns={k: v for k, v in col_labels.items() if k in detail_df.columns})
    st.dataframe(detail_df, use_container_width=True)

# ── Reconciliation ──────────────────────────────────────────────────────────

st.divider()
st.subheader("Reconciliation")
st.caption(
    "For each consecutive pair of position snapshots (by statement_date), "
    "rolls forward from base holdings through trades and compares against "
    "the next snapshot. Mismatches indicate missing trades, corporate actions, "
    "or data gaps."
)

# Check if any account has enough snapshots
any_reconcilable = any(
    len(dates) >= 2 for dates in snapshot_dates_by_acct.values()
)

if not any_reconcilable:
    st.info(
        "Each account needs at least 2 position snapshots (different statement_dates) "
        "to reconcile. Upload more statements to enable this."
    )
    st.stop()

# Optional: pick a specific pair
all_recon_dates = sorted(
    {d for dates in snapshot_dates_by_acct.values() for d in dates}
)

use_custom_dates = False
custom_base: date | None = None
custom_target: date | None = None

if len(all_recon_dates) >= 2:
    use_custom_dates = st.checkbox(
        "Choose reconciliation dates",
        help="Select specific base and target snapshot dates instead of running all pairs.",
    )

if use_custom_dates and len(all_recon_dates) >= 2:
    col_b, col_t = st.columns(2)
    with col_b:
        custom_base = st.selectbox(
            "Base date (start from)",
            options=all_recon_dates[:-1],
            index=0,
            format_func=lambda d: d.isoformat(),
        )
    with col_t:
        valid_targets = [d for d in all_recon_dates if d > custom_base]
        custom_target = st.selectbox(
            "Target date (compare against)",
            options=valid_targets,
            index=len(valid_targets) - 1 if valid_targets else 0,
            format_func=lambda d: d.isoformat(),
        )

run_recon = st.button("Run Reconciliation", type="primary")

if not run_recon:
    st.stop()

for acct in target_accounts:
    acct_dates = snapshot_dates_by_acct.get(acct, [])
    if len(acct_dates) < 2:
        st.info(f"**{acct}**: Only {len(acct_dates)} snapshot(s) — need at least 2 to reconcile.")
        continue

    if use_custom_dates and custom_base and custom_target:
        # Validate the chosen dates exist for this account
        if custom_base not in acct_dates or custom_target not in acct_dates:
            st.info(
                f"**{acct}**: Selected dates not available for this account. "
                f"Available: {', '.join(d.isoformat() for d in acct_dates)}"
            )
            continue
        pair_results = [reconcile_pair(acct, custom_base, custom_target)]
    else:
        pair_results = reconcile_account(acct)

    st.markdown(f"### {acct}")

    for result in pair_results:
        header = f"**{result['base_date']} -> {result['target_date']}**"

        if result["ok"]:
            st.success(f"{header}: All holdings reconcile.")
        else:
            st.error(f"{header}: Reconciliation FAILED — differences found.")

        # Gaps summary
        gaps = result.get("gaps", {})
        if gaps.get("missing_from_target"):
            with st.expander(
                f"Reconstructed but not in target snapshot ({len(gaps['missing_from_target'])})",
                expanded=True,
            ):
                st.caption(
                    "These positions exist after rolling forward but are absent from "
                    "the target snapshot. Possible causes: position closed by a missing "
                    "trade, option expired/exercised, or corporate action."
                )
                st.dataframe(pd.DataFrame(gaps["missing_from_target"]), use_container_width=True)

        if gaps.get("missing_from_reconstruction"):
            with st.expander(
                f"In target but not reconstructed ({len(gaps['missing_from_reconstruction'])})",
                expanded=True,
            ):
                st.caption(
                    "These positions appear in the target snapshot but could not be "
                    "built from the base snapshot + trades. Possible causes: "
                    "missing statement, transfer-in, or corporate action."
                )
                st.dataframe(
                    pd.DataFrame(gaps["missing_from_reconstruction"]),
                    use_container_width=True,
                )

        # Per-holding detail
        if result["holdings"]:
            summary_rows = []
            for symbol, h in result["holdings"].items():
                summary_rows.append({
                    "Symbol": symbol,
                    "Base Qty": h["base_qty"],
                    "Trades": len(h["trades"]),
                    "Reconstructed Qty": h["reconstructed_qty"],
                    "Expected Qty": h["expected_qty"],
                    "Diff": h["diff"],
                    "Match": h["match"],
                })

            summary_df = pd.DataFrame(summary_rows)
            for col in ["Base Qty", "Reconstructed Qty", "Expected Qty", "Diff"]:
                summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce")

            mismatched = summary_df[~summary_df["Match"]].reset_index(drop=True)
            matched = summary_df[summary_df["Match"]].reset_index(drop=True)

            if not mismatched.empty:
                st.error(f"{len(mismatched)} holding(s) do not match:")
                st.dataframe(
                    mismatched,
                    use_container_width=True,
                    column_config={
                        "Base Qty": st.column_config.NumberColumn(format="%.4f"),
                        "Reconstructed Qty": st.column_config.NumberColumn(format="%.4f"),
                        "Expected Qty": st.column_config.NumberColumn(format="%.4f"),
                        "Diff": st.column_config.NumberColumn(format="%.4f"),
                    },
                )

            if not matched.empty:
                with st.expander(f"{len(matched)} holding(s) match", expanded=False):
                    st.dataframe(
                        matched,
                        use_container_width=True,
                        column_config={
                            "Base Qty": st.column_config.NumberColumn(format="%.4f"),
                            "Reconstructed Qty": st.column_config.NumberColumn(format="%.4f"),
                            "Expected Qty": st.column_config.NumberColumn(format="%.4f"),
                            "Diff": st.column_config.NumberColumn(format="%.4f"),
                        },
                    )

            # Per-holding trade ledger expanders
            for symbol, h in result["holdings"].items():
                status = "OK" if h["match"] else "MISMATCH"
                with st.expander(
                    f"{symbol}: {h['base_qty']} -> {h['reconstructed_qty']} "
                    f"(expected {h['expected_qty']}) [{status}]",
                    expanded=not h["match"],
                ):
                    if not h["trades"]:
                        st.caption("No trades in this period.")
                    else:
                        ledger_df = pd.DataFrame(h["trades"])
                        ledger_df.columns = ["Date", "Side", "Quantity", "Running Qty"]
                        for col in ["Quantity", "Running Qty"]:
                            ledger_df[col] = pd.to_numeric(ledger_df[col], errors="coerce")
                        st.dataframe(
                            ledger_df,
                            use_container_width=True,
                            column_config={
                                "Quantity": st.column_config.NumberColumn(format="%.4f"),
                                "Running Qty": st.column_config.NumberColumn(format="%.4f"),
                            },
                        )
