"""Parse IBKR Custom Date Range PDF statements into structured data.

IBKR PDFs use a consistent tabular layout where:
- The first column of each row identifies the section (e.g. "Statement",
  "Open Positions", "Trades").
- The second column identifies the row type: "Header" for column names,
  "Data" for data rows, "SubTotal"/"Total" for aggregates.
- Asset class sub-sections appear as "Data" rows whose third column is
  an asset class label like "Stocks", "Equity and Index Options", etc.

This parser extracts all tables across pages, concatenates them, then
walks the rows to pull out statement metadata, open positions, and trades.
"""

import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import BinaryIO

from src.models import (
    ParsedStatement,
    Position,
    StatementMeta,
    Trade,
)

logger = logging.getLogger(__name__)

# ── IBKR asset-class labels → our codes ──────────────────────────────────────
ASSET_CLASS_MAP: dict[str, str] = {
    "Stocks": "STK",
    "Equity and Index Options": "OPT",
    "ETFs": "ETF",
}

# Labels we intentionally skip (not in scope)
SKIPPED_ASSET_LABELS: set[str] = {
    "Bonds", "Futures", "Forex", "Warrants", "Structured Products",
    "Fund", "Funds", "Cash",
}

# Rows whose second column matches any of these are not data rows.
NON_DATA_MARKERS: set[str] = {"Header", "SubTotal", "Total"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_decimal(value: str | None) -> Decimal:
    """Convert a string to Decimal, stripping commas. Returns 0 for blanks."""
    if not value or not value.strip():
        return Decimal("0")
    try:
        return Decimal(value.strip().replace(",", ""))
    except InvalidOperation:
        return Decimal("0")


def _parse_date(value: str, fmt: str = "%Y-%m-%d") -> date:
    return datetime.strptime(value.strip(), fmt).date()


def _parse_datetime(value: str) -> datetime:
    """Parse IBKR date-time strings like '2024-01-15, 10:30:00'."""
    value = value.strip()
    for fmt in ("%Y-%m-%d, %H:%M:%S", "%Y-%m-%d;%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    # Fallback: date only
    return datetime.strptime(value[:10], "%Y-%m-%d")


def _parse_option_symbol(symbol: str) -> dict:
    """Parse IBKR option symbol format.

    Common formats:
        'AAPL 20240119C00150000'  (OSI format)
        'AAPL 20240119 150.0 C'   (space-separated)

    Returns dict with expiry, strike, right — or empty dict on failure.
    """
    symbol = symbol.strip()

    # Space-separated: "AAPL 20240119 150.0 C"
    parts = symbol.split()
    if len(parts) == 4:
        try:
            return {
                "expiry": _parse_date(parts[1], "%Y%m%d"),
                "strike": Decimal(parts[2]),
                "right": parts[3].upper(),
            }
        except (ValueError, InvalidOperation):
            pass

    # OSI format: "AAPL  240119C00150000" — 6-digit date, C/P, 8-digit strike
    m = re.search(r"(\d{6})([CP])(\d{8})", symbol)
    if m:
        raw_date, right, raw_strike = m.groups()
        try:
            expiry = datetime.strptime(raw_date, "%y%m%d").date()
            strike = Decimal(raw_strike) / Decimal("1000")
            return {"expiry": expiry, "strike": strike, "right": right}
        except (ValueError, InvalidOperation):
            pass

    logger.warning("Could not parse option symbol: %s", symbol)
    return {}


# ── Table extraction ─────────────────────────────────────────────────────────

def _extract_tables(pdf_file: BinaryIO) -> list[list[str]]:
    """Return all table rows across all pages as lists of strings."""
    import pdfplumber  # lazy import — heavy dep only needed at parse time

    all_rows: list[list[str]] = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    # Normalise: replace None with empty string
                    all_rows.append([cell if cell is not None else "" for cell in row])
    return all_rows


# ── Statement metadata ───────────────────────────────────────────────────────

def _parse_period_date(text: str) -> date:
    """Parse period date in multiple formats."""
    text = text.strip()
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse period date: {text}")


def _extract_meta(rows: list[list[str]]) -> StatementMeta:
    """Pull account ID, period, and base currency from the Statement section."""
    account_id = ""
    period_start = None
    period_end = None
    base_currency = "USD"

    for row in rows:
        if len(row) < 3 or row[0] != "Statement":
            continue
        if row[1] == "Header":
            continue
        field_name = row[2].strip() if row[2] else ""
        field_value = row[3].strip() if len(row) > 3 and row[3] else ""

        if field_name == "Account":
            account_id = field_value
        elif field_name == "Period":
            parts = re.split(r"\s+-\s+", field_value)
            if len(parts) == 2:
                period_start = _parse_period_date(parts[0])
                period_end = _parse_period_date(parts[1])
        elif field_name in ("Base Currency", "BaseCurrency"):
            base_currency = field_value

    if not account_id:
        raise ValueError("Could not find account ID in statement.")
    if period_start is None or period_end is None:
        raise ValueError("Could not find statement period.")

    return StatementMeta(
        account_id=account_id,
        period_start=period_start,
        period_end=period_end,
        base_currency=base_currency,
    )


# ── Column mapping ───────────────────────────────────────────────────────────

POSITION_COL_MAP: dict[str, str] = {
    "symbol": "symbol",
    "quantity": "quantity",
    "cost basis": "cost_basis",
    "cost price": "cost_basis",
    "close price": "market_price",
    "mark price": "market_price",
    "value": "market_value",
    "market value": "market_value",
    "unrealized p/l": "unrealized_pnl",
    "unrealized p&l": "unrealized_pnl",
    "currency": "currency",
}

TRADE_COL_MAP: dict[str, str] = {
    "symbol": "symbol",
    "date/time": "trade_date",
    "datetime": "trade_date",
    "quantity": "quantity",
    "t. price": "price",
    "trade price": "price",
    "price": "price",
    "proceeds": "proceeds",
    "comm/fee": "commission",
    "commission": "commission",
    "realized p/l": "realized_pnl",
    "realized p&l": "realized_pnl",
    "currency": "currency",
}


def _map_columns(header_row: list[str], col_map: dict[str, str]) -> dict[int, str]:
    """Map column indices to model field names."""
    mapping: dict[int, str] = {}
    for i, col in enumerate(header_row):
        key = col.strip().lower()
        if key in col_map:
            mapping[i] = col_map[key]
    return mapping


# ── Position extraction ──────────────────────────────────────────────────────

def _extract_positions(
    rows: list[list[str]], period_end: date,
) -> tuple[list[Position], list[dict]]:
    """Walk rows for the Open Positions section."""
    positions: list[Position] = []
    skipped: list[dict] = []
    in_section = False
    current_asset_class: str | None = None
    col_mapping: dict[int, str] = {}

    for row in rows:
        if len(row) < 2:
            continue

        section = row[0].strip()
        row_type = row[1].strip() if row[1] else ""

        # Enter/exit section
        if section == "Open Positions":
            in_section = True
        elif in_section and section and section != "Open Positions":
            in_section = False
            continue

        if not in_section:
            continue

        if row_type == "Header":
            col_mapping = _map_columns(row[2:], POSITION_COL_MAP)
            continue

        if row_type in NON_DATA_MARKERS:
            continue

        if row_type != "Data":
            continue

        data_cells = row[2:]
        first_cell = data_cells[0].strip() if data_cells and data_cells[0] else ""

        # Asset class sub-header
        if first_cell in ASSET_CLASS_MAP:
            current_asset_class = ASSET_CLASS_MAP[first_cell]
            continue
        if first_cell in SKIPPED_ASSET_LABELS:
            current_asset_class = None
            logger.warning("Skipping unsupported asset class section: %s", first_cell)
            continue

        # In a skipped asset class
        if current_asset_class is None:
            if first_cell:
                skipped.append({"section": "Open Positions", "reason": "unsupported asset class", "row": data_cells})
            continue

        if not col_mapping:
            continue

        # Map columns
        fields: dict = {}
        for idx, field_name in col_mapping.items():
            if idx < len(data_cells):
                fields[field_name] = data_cells[idx].strip() if data_cells[idx] else ""

        symbol = fields.get("symbol", "").strip()
        if not symbol:
            continue

        try:
            opt_fields: dict = {}
            if current_asset_class == "OPT":
                opt_fields = _parse_option_symbol(symbol)

            pos = Position(
                symbol=symbol,
                asset_class=current_asset_class,
                quantity=_to_decimal(fields.get("quantity")),
                cost_basis=_to_decimal(fields.get("cost_basis")),
                market_price=_to_decimal(fields.get("market_price")),
                market_value=_to_decimal(fields.get("market_value")),
                unrealized_pnl=_to_decimal(fields.get("unrealized_pnl")),
                currency=fields.get("currency", "USD"),
                statement_date=period_end,
                **opt_fields,
            )
            positions.append(pos)
        except Exception as e:
            logger.warning("Skipping position row: %s — %s", data_cells, e)
            skipped.append({"section": "Open Positions", "reason": str(e), "row": data_cells})

    return positions, skipped


# ── Trade extraction ─────────────────────────────────────────────────────────

def _extract_trades(rows: list[list[str]]) -> tuple[list[Trade], list[dict]]:
    """Walk rows for the Trades section."""
    trades: list[Trade] = []
    skipped: list[dict] = []
    in_section = False
    current_asset_class: str | None = None
    col_mapping: dict[int, str] = {}

    for row in rows:
        if len(row) < 2:
            continue

        section = row[0].strip()
        row_type = row[1].strip() if row[1] else ""

        if section == "Trades":
            in_section = True
        elif in_section and section and section != "Trades":
            in_section = False
            continue

        if not in_section:
            continue

        if row_type == "Header":
            col_mapping = _map_columns(row[2:], TRADE_COL_MAP)
            continue

        if row_type in NON_DATA_MARKERS:
            continue

        if row_type != "Data":
            continue

        data_cells = row[2:]
        first_cell = data_cells[0].strip() if data_cells and data_cells[0] else ""

        if first_cell in ASSET_CLASS_MAP:
            current_asset_class = ASSET_CLASS_MAP[first_cell]
            continue
        if first_cell in SKIPPED_ASSET_LABELS:
            current_asset_class = None
            logger.warning("Skipping unsupported asset class section: %s", first_cell)
            continue

        if current_asset_class is None:
            if first_cell:
                skipped.append({"section": "Trades", "reason": "unsupported asset class", "row": data_cells})
            continue

        if not col_mapping:
            continue

        fields: dict = {}
        for idx, field_name in col_mapping.items():
            if idx < len(data_cells):
                fields[field_name] = data_cells[idx].strip() if data_cells[idx] else ""

        symbol = fields.get("symbol", "").strip()
        if not symbol:
            continue

        # Determine side from quantity sign
        raw_qty = _to_decimal(fields.get("quantity"))
        if raw_qty > 0:
            side = "BOT"
        elif raw_qty < 0:
            side = "SLD"
        else:
            continue

        try:
            opt_fields: dict = {}
            if current_asset_class == "OPT":
                opt_fields = _parse_option_symbol(symbol)

            trade = Trade(
                trade_date=_parse_datetime(fields.get("trade_date", "")),
                symbol=symbol,
                asset_class=current_asset_class,
                side=side,
                quantity=abs(raw_qty),
                price=_to_decimal(fields.get("price")),
                proceeds=_to_decimal(fields.get("proceeds")),
                commission=_to_decimal(fields.get("commission")),
                realized_pnl=_to_decimal(fields.get("realized_pnl")),
                currency=fields.get("currency", "USD"),
                **opt_fields,
            )
            trades.append(trade)
        except Exception as e:
            logger.warning("Skipping trade row: %s — %s", data_cells, e)
            skipped.append({"section": "Trades", "reason": str(e), "row": data_cells})

    return trades, skipped


# ── Public API ────────────────────────────────────────────────────────────────

def parse_statement(pdf_file: BinaryIO) -> ParsedStatement:
    """Parse an IBKR Custom Date Range PDF into a ParsedStatement.

    Args:
        pdf_file: A file-like object (e.g. BytesIO or UploadedFile) of the PDF.

    Returns:
        ParsedStatement with metadata, positions, trades, and skipped rows.

    Raises:
        ValueError: If required metadata (account ID, period) cannot be found.
    """
    rows = _extract_tables(pdf_file)
    if not rows:
        raise ValueError("No tables found in the PDF. Is this a valid IBKR statement?")

    meta = _extract_meta(rows)
    positions, pos_skipped = _extract_positions(rows, meta.period_end)
    trades, trade_skipped = _extract_trades(rows)
    skipped = pos_skipped + trade_skipped

    logger.info(
        "Parsed statement %s (%s to %s): %d positions, %d trades, %d skipped",
        meta.account_id, meta.period_start, meta.period_end,
        len(positions), len(trades), len(skipped),
    )

    return ParsedStatement(
        meta=meta,
        positions=positions,
        trades=trades,
        skipped_rows=skipped,
    )
