"""Generate a PDF report of portfolio holdings."""

from datetime import date

import pandas as pd
from fpdf import FPDF


def generate_holdings_pdf(
    df: pd.DataFrame,
    account_id: str,
    as_of: date,
    beta_result: dict | None = None,
    beta_benchmark: str | None = None,
) -> bytes:
    """Build a PDF summarising the holdings in *df* and return the raw bytes.

    Parameters
    ----------
    df : pd.DataFrame
        Holdings dataframe with columns: symbol, asset_class, quantity,
        cost_basis, cost_value, multiplier, and optionally market_price,
        market_value, unrealized_pnl, expiry, strike, right.
    account_id : str
        Account identifier (or "All Accounts").
    as_of : date
        Snapshot date.
    beta_result : dict | None
        Beta calculation result from session state. Keys: portfolio_beta,
        portfolio_dollar_beta, betas, dollar_betas, position_betas.
    beta_benchmark : str | None
        Benchmark symbol (e.g. "SPY") used for the beta calculation.
    """
    has_market_data = "market_value" in df.columns and df["market_value"].notna().any()

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.compress = False
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # ── Title / header ──────────────────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Portfolio Holdings Report", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, f"Account: {account_id}    |    As-of: {as_of.isoformat()}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ── Summary metrics ─────────────────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)

    if has_market_data:
        total_mv = df["market_value"].sum()
        total_pnl = df["unrealized_pnl"].sum()
        pdf.cell(0, 6, f"Total Market Value: ${total_mv:,.2f}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 6, f"Unrealized P&L: ${total_pnl:,.2f}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 6, f"Positions: {len(df)}", new_x="LMARGIN", new_y="NEXT")
    else:
        total_cost = df["cost_value"].sum()
        pdf.cell(0, 6, f"Total Cost Value: ${total_cost:,.2f}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 6, f"Positions: {len(df)}", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(4)

    # ── Portfolio Beta ──────────────────────────────────────────────────────
    if beta_result is not None and beta_benchmark is not None:
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, f"Portfolio Beta vs {beta_benchmark}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)

        port_beta = beta_result.get("portfolio_beta")
        port_dollar_beta = beta_result.get("portfolio_dollar_beta")
        if port_beta is not None:
            pdf.cell(0, 6, f"Portfolio Beta: {port_beta:.2f}", new_x="LMARGIN", new_y="NEXT")
        if port_dollar_beta is not None:
            pdf.cell(0, 6, f"Dollar Beta: ${port_dollar_beta:,.0f}", new_x="LMARGIN", new_y="NEXT")

        # Per-symbol beta table
        betas = beta_result.get("betas", {})
        dollar_betas = beta_result.get("dollar_betas", {})
        if betas:
            pdf.ln(2)
            beta_cols = ["Symbol", f"Beta ({beta_benchmark})", "Dollar Beta"]
            beta_rows = []
            for sym in sorted(betas.keys()):
                b = betas[sym]
                db = dollar_betas.get(sym)
                beta_rows.append([
                    sym,
                    f"{b:.3f}" if b is not None else "N/A",
                    f"${db:,.0f}" if db is not None else "N/A",
                ])
            _render_table(pdf, beta_cols, beta_rows)

        pdf.ln(4)

    # ── Consolidated Market Value by Symbol ──────────────────────────────────
    pdf.set_font("Helvetica", "B", 12)
    label = "Market Value by Symbol" if has_market_data else "Cost Basis by Symbol"
    pdf.cell(0, 8, label, new_x="LMARGIN", new_y="NEXT")

    consol = _build_consolidated(df, has_market_data)
    consol_cols = ["Symbol", "Qty", "Market Value", "% of Account"]
    consol_data = []
    total_mv_consol = consol["market_value"].sum()
    for _, r in consol.iterrows():
        pct = (r["market_value"] / total_mv_consol * 100) if total_mv_consol else 0
        consol_data.append([
            str(r["symbol"]),
            f"{r['quantity']:,.2f}",
            f"${r['market_value']:,.2f}",
            f"{pct:.1f}%",
        ])
    _render_table(pdf, consol_cols, consol_data)
    pdf.ln(4)

    # ── Detailed holdings by asset class ─────────────────────────────────────
    class_labels = {"STK": "Stocks", "OPT": "Options", "ETF": "ETFs"}

    for asset_class in sorted(df["asset_class"].unique()):
        group = df[df["asset_class"] == asset_class].sort_values("symbol").reset_index(drop=True)
        class_label = class_labels.get(asset_class, asset_class)

        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, f"{class_label} ({len(group)})", new_x="LMARGIN", new_y="NEXT")

        cols, rows = _detail_rows(group, asset_class, has_market_data)
        _render_table(pdf, cols, rows)
        pdf.ln(4)

    pdf.compress = False
    return bytes(pdf.output())


# ── helpers ──────────────────────────────────────────────────────────────────

def _build_consolidated(df: pd.DataFrame, has_market_data: bool) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        if has_market_data and pd.notna(row.get("market_value")):
            value = abs(float(row["market_value"]))
        else:
            value = abs(float(row["quantity"])) * float(row.get("cost_basis", 0) or 0)
        records.append({
            "symbol": row["symbol"],
            "market_value": value,
            "quantity": abs(float(row["quantity"])),
        })
    consol = (
        pd.DataFrame(records)
        .groupby("symbol", as_index=False)
        .agg(market_value=("market_value", "sum"), quantity=("quantity", "sum"))
        .sort_values("market_value", ascending=False)
        .reset_index(drop=True)
    )
    return consol


def _detail_rows(group: pd.DataFrame, asset_class: str, has_market_data: bool):
    is_opt = asset_class == "OPT"

    cols = ["Symbol", "Qty", "Cost Basis"]
    if has_market_data:
        cols += ["Mkt Price", "Mkt Value", "Unreal P&L"]
    if is_opt:
        cols += ["Expiry", "Strike", "Right"]

    rows = []
    for _, r in group.iterrows():
        row_data = [
            str(r["symbol"]),
            f"{float(r['quantity']):,.2f}",
            f"${float(r.get('cost_basis', 0) or 0):,.2f}",
        ]
        if has_market_data:
            row_data += [
                f"${float(r.get('market_price', 0) or 0):,.2f}",
                f"${float(r.get('market_value', 0) or 0):,.2f}",
                f"${float(r.get('unrealized_pnl', 0) or 0):,.2f}",
            ]
        if is_opt:
            expiry = r.get("expiry", "")
            strike = r.get("strike", 0)
            right = r.get("right", "")
            row_data += [
                str(expiry) if expiry else "",
                f"${float(strike or 0):,.2f}",
                str(right) if right else "",
            ]
        rows.append(row_data)
    return cols, rows


def _render_table(pdf: FPDF, headers: list[str], rows: list[list[str]]):
    """Draw a simple table with alternating row shading."""
    pdf.set_font("Helvetica", "B", 9)
    n_cols = len(headers)
    # Use available page width minus margins
    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
    col_w = usable_w / n_cols

    # Header row
    pdf.set_fill_color(60, 60, 60)
    pdf.set_text_color(255, 255, 255)
    for h in headers:
        pdf.cell(col_w, 7, h, border=1, fill=True, align="C")
    pdf.ln()

    # Data rows
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(0, 0, 0)
    for i, row in enumerate(rows):
        if i % 2 == 0:
            pdf.set_fill_color(245, 245, 245)
        else:
            pdf.set_fill_color(255, 255, 255)
        for val in row:
            pdf.cell(col_w, 6, val, border=1, fill=True, align="C")
        pdf.ln()
