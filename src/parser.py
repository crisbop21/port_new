"""Parse IBKR Custom Date Range PDF statements into structured data.

IBKR Custom Date Range PDFs use a flat tabular layout:
- Section headers are rows like ['Open Positions', '', '', ...]
- Column headers follow section/asset-class headers: ['Symbol', 'Quantity', ...]
- Asset class sub-headers: ['Stocks', '', ...], ['Equity and Index Options', '', ...]
- Currency sub-headers: ['USD', '', ...]
- Total rows: ['Total', ...] or ['Total AAPL', ...] or ['Total in SGD', ...]
- Data rows: ['AAPL', '100', '1', ...]

Multi-account PDFs repeat this structure per account, with each account
starting at an "Account Information" table.
"""

import logging
import re
from datetime import date, datetime, timedelta
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

SKIPPED_ASSET_LABELS: set[str] = {
    "Bonds", "Futures", "Forex", "Warrants", "Structured Products",
    "Fund", "Funds", "Cash",
}

ALL_ASSET_LABELS: set[str] = set(ASSET_CLASS_MAP) | SKIPPED_ASSET_LABELS

CURRENCY_CODES: set[str] = {
    "USD", "SGD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "HKD",
    "MXN", "CNH", "NZD", "SEK", "NOK", "DKK", "KRW", "INR", "BRL",
}

# Section names used to detect section boundaries.
SECTION_NAMES: set[str] = {
    "Account Information", "Account Summary", "Net Asset Value",
    "Change in NAV", "Mark-to-Market Performance Summary",
    "Realized & Unrealized Performance Summary", "Cash Report",
    "Open Positions", "Forex Balances", "Net Stock Position Summary",
    "Trades", "Transaction Fees", "Interest", "Dividends",
    "Withholding Tax", "Notes/Legal Notes", "Time Weighted Rate of Return",
    "Deposits & Withdrawals", "Fees", "Other Fees", "Sales Tax",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_decimal(value: str | None) -> Decimal:
    """Convert a string to Decimal, stripping commas. Returns 0 for blanks."""
    if not value or not value.strip():
        return Decimal("0")
    cleaned = value.strip().replace(",", "").replace("\n", "")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return Decimal("0")


def _parse_datetime(value: str) -> datetime:
    """Parse IBKR date-time strings, handling embedded newlines.

    Real format: '2026-01-12,\\n09:30:00'
    """
    value = value.strip().replace("\n", " ")
    for fmt in ("%Y-%m-%d, %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d;%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.strptime(value[:10], "%Y-%m-%d")


def _parse_period_date(text: str) -> date:
    """Parse period date in multiple formats (e.g. 'March 6, 2026')."""
    text = text.strip()
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse period date: {text}")


def _parse_option_symbol(symbol: str) -> dict:
    """Parse IBKR option symbol into expiry, strike, right.

    Formats seen in real PDFs:
        'EEM 31MAR26 48 C'       (DDMMMYY with month abbrev)
        'PYPL 18SEP26 87.5 C'
        'AAPL 20240119 150.0 C'  (YYYYMMDD numeric)
        'AAPL  240119C00150000'  (OSI compact)
    """
    symbol = symbol.strip()
    parts = symbol.split()

    if len(parts) >= 4:
        date_str = parts[-3]
        strike_str = parts[-2]
        right = parts[-1].upper()

        if right not in ("C", "P"):
            logger.warning("Could not parse option symbol: %s", symbol)
            return {}

        # Try DDMMMYY: "31MAR26", "18SEP26", "16JAN26"
        m = re.match(r"^(\d{1,2})([A-Z]{3})(\d{2})$", date_str)
        if m:
            day, mon, yr = m.groups()
            try:
                expiry = datetime.strptime(f"{day}{mon}{yr}", "%d%b%y").date()
                return {
                    "expiry": expiry,
                    "strike": Decimal(strike_str),
                    "right": right,
                }
            except (ValueError, InvalidOperation):
                pass

        # Try YYYYMMDD: "20240119"
        if len(date_str) == 8 and date_str.isdigit():
            try:
                expiry = datetime.strptime(date_str, "%Y%m%d").date()
                return {
                    "expiry": expiry,
                    "strike": Decimal(strike_str),
                    "right": right,
                }
            except (ValueError, InvalidOperation):
                pass

    # OSI compact: "AAPL  240119C00150000"
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


# ── Row classification helpers ───────────────────────────────────────────────

def _is_all_empty(cells: list[str], start: int = 1) -> bool:
    """Check if all cells from index `start` onward are empty."""
    return all(not (c or "").strip() for c in cells[start:])


def _is_section_header(row: list[str]) -> str | None:
    """If row is a section header, return the section name; else None."""
    if not row or not row[0]:
        return None
    first = row[0].strip()
    if first in SECTION_NAMES and _is_all_empty(row, 1):
        return first
    return None


def _is_column_header(row: list[str]) -> bool:
    """Detect column header rows (start with 'Symbol' or 'Description')."""
    if not row or not row[0]:
        return False
    first = row[0].strip()
    return first in ("Symbol", "Description")


def _is_asset_class(row: list[str]) -> str | None:
    """If row is an asset class sub-header, return the label; else None."""
    if not row or not row[0]:
        return None
    first = row[0].strip()
    if first in ALL_ASSET_LABELS and _is_all_empty(row, 1):
        return first
    return None


def _is_currency(row: list[str]) -> bool:
    """Detect currency sub-header rows."""
    if not row or not row[0]:
        return False
    return row[0].strip() in CURRENCY_CODES and _is_all_empty(row, 1)


def _is_total(row: list[str]) -> bool:
    """Detect Total / Total <symbol> / Total in SGD rows."""
    if not row or not row[0]:
        return False
    return row[0].strip().startswith("Total")


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
                    all_rows.append([cell if cell is not None else "" for cell in row])
    return all_rows


# ── Account splitting ────────────────────────────────────────────────────────

def _split_accounts(rows: list[list[str]]) -> list[list[list[str]]]:
    """Split rows into per-account groups using 'Account Information' markers."""
    account_starts: list[int] = []
    for i, row in enumerate(rows):
        if row and row[0] and row[0].strip() == "Account Information":
            account_starts.append(i)

    if not account_starts:
        return [rows]

    groups: list[list[list[str]]] = []
    for idx, start in enumerate(account_starts):
        end = account_starts[idx + 1] if idx + 1 < len(account_starts) else len(rows)
        groups.append(rows[start:end])
    return groups


# ── Metadata extraction ──────────────────────────────────────────────────────

def _extract_meta(rows: list[list[str]]) -> StatementMeta:
    """Extract account ID, base currency, and period from account rows."""
    account_id = ""
    base_currency = "USD"
    period_start = None
    period_end = None

    in_account_info = False
    in_nav = False

    for row in rows:
        section = _is_section_header(row)
        if section == "Account Information":
            in_account_info = True
            in_nav = False
            continue
        elif section == "Net Asset Value":
            in_account_info = False
            in_nav = True
            continue
        elif section is not None:
            in_account_info = False
            in_nav = False
            continue

        if in_account_info and len(row) >= 2:
            key = (row[0] or "").strip()
            val = (row[1] or "").strip()
            if key == "Account":
                account_id = val
            elif key == "Base Currency":
                base_currency = val

        if in_nav and len(row) >= 3 and not period_end:
            # NAV header row: ['December 31, 2025', '', 'March 6, 2026', ...]
            first = (row[0] or "").strip()
            third = (row[2] or "").strip()
            if first and third:
                try:
                    prior_date = _parse_period_date(first)
                    end_date = _parse_period_date(third)
                    period_start = prior_date + timedelta(days=1)
                    period_end = end_date
                except ValueError:
                    pass

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
    "description": "symbol",  # continuation pages use "Description" header
    "quantity": "quantity",
    "cost basis": "cost_basis",
    "close price": "market_price",
    "value": "market_value",
    "unrealized p/l": "unrealized_pnl",
}

TRADE_COL_MAP: dict[str, str] = {
    "symbol": "symbol",
    "description": "symbol",  # continuation pages use "Description" header
    "date/time": "trade_date",
    "quantity": "quantity",
    "t. price": "price",
    "proceeds": "proceeds",
    "comm/fee": "commission",
    "realized p/l": "realized_pnl",
}


def _map_columns(header_row: list[str], col_map: dict[str, str]) -> dict[int, str]:
    """Map column indices to model field names."""
    mapping: dict[int, str] = {}
    for i, col in enumerate(header_row):
        key = (col or "").strip().lower()
        if key in col_map:
            mapping[i] = col_map[key]
    return mapping


# ── Position extraction ──────────────────────────────────────────────────────

def _extract_positions(
    rows: list[list[str]], period_end: date,
) -> tuple[list[Position], list[dict]]:
    """Extract Open Positions from an account's rows."""
    positions: list[Position] = []
    skipped: list[dict] = []
    in_section = False
    current_asset_class: str | None = None
    col_mapping: dict[int, str] = {}

    for row in rows:
        if not row:
            continue

        section = _is_section_header(row)
        if section == "Open Positions":
            in_section = True
            current_asset_class = None
            col_mapping = {}
            logger.debug("Entered Open Positions section")
            continue
        elif section is not None and in_section:
            # Different section started → leave Open Positions
            logger.debug("Left Open Positions at section: %s", section)
            break

        if not in_section:
            continue

        # Column header row
        if _is_column_header(row):
            col_mapping = _map_columns(row, POSITION_COL_MAP)
            logger.debug("Column header mapped: %s", col_mapping)
            continue

        # Asset class sub-header
        ac_label = _is_asset_class(row)
        if ac_label is not None:
            if ac_label in ASSET_CLASS_MAP:
                current_asset_class = ASSET_CLASS_MAP[ac_label]
                logger.debug("Asset class: %s → %s", ac_label, current_asset_class)
            else:
                current_asset_class = None
                logger.warning("Skipping unsupported asset class: %s", ac_label)
            continue

        # Currency sub-header
        if _is_currency(row):
            continue

        # Total rows
        if _is_total(row):
            continue

        # In a skipped or unknown asset class
        if current_asset_class is None:
            logger.debug("Skipping row (no asset class): %s", row[:2])
            continue

        if not col_mapping:
            logger.debug("Skipping row (no col mapping): %s", row[:2])
            continue

        # Map columns to fields
        fields: dict = {}
        for idx, field_name in col_mapping.items():
            if idx < len(row):
                fields[field_name] = (row[idx] or "").strip()

        symbol = fields.get("symbol", "").strip()
        if not symbol:
            logger.debug("Skipping row (empty symbol): %s", row[:3])
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
                currency="USD",
                statement_date=period_end,
                **opt_fields,
            )
            positions.append(pos)
        except Exception as e:
            logger.warning("Skipping position row: %s — %s", row, e)
            skipped.append({"section": "Open Positions", "reason": str(e), "row": row})

    return positions, skipped


# ── Position diagnostic ──────────────────────────────────────────────────────

def diagnose_positions(rows: list[list[str]]) -> list[dict]:
    """Classify every row for diagnostic display.

    Returns a list of dicts, one per row, with keys:
        row_num: 1-based index
        raw_row: the original row (list of strings)
        classification: one of section_header, column_header, asset_class,
            asset_class_skipped, currency, total, parsed, error,
            skipped_no_asset_class, skipped_no_mapping, skipped_empty_symbol,
            outside_section
        asset_class: current asset class at this row (or None)
        detail: human-readable explanation
    """
    result: list[dict] = []
    in_section = False
    current_asset_class: str | None = None
    col_mapping: dict[int, str] = {}

    for i, row in enumerate(rows):
        entry: dict = {
            "row_num": i + 1,
            "raw_row": row,
            "classification": "",
            "asset_class": current_asset_class,
            "detail": "",
        }

        if not row:
            entry["classification"] = "empty"
            entry["detail"] = "Empty row"
            result.append(entry)
            continue

        section = _is_section_header(row)
        if section == "Open Positions":
            in_section = True
            current_asset_class = None
            col_mapping = {}
            entry["classification"] = "section_header"
            entry["detail"] = "Open Positions"
            entry["asset_class"] = None
            result.append(entry)
            continue
        elif section is not None and in_section:
            entry["classification"] = "section_header"
            entry["detail"] = f"{section} (exits Open Positions)"
            entry["asset_class"] = current_asset_class
            result.append(entry)
            break
        elif section is not None:
            entry["classification"] = "outside_section"
            entry["detail"] = f"Section: {section}"
            result.append(entry)
            continue

        if not in_section:
            entry["classification"] = "outside_section"
            first_cell = (row[0] or "")[:40] if row else ""
            entry["detail"] = f"Before Open Positions: {first_cell}"
            result.append(entry)
            continue

        # Column header
        if _is_column_header(row):
            col_mapping = _map_columns(row, POSITION_COL_MAP)
            entry["classification"] = "column_header"
            entry["detail"] = f"Mapped {len(col_mapping)} columns"
            entry["asset_class"] = current_asset_class
            result.append(entry)
            continue

        # Asset class sub-header
        ac_label = _is_asset_class(row)
        if ac_label is not None:
            if ac_label in ASSET_CLASS_MAP:
                current_asset_class = ASSET_CLASS_MAP[ac_label]
                entry["classification"] = "asset_class"
                entry["detail"] = f"{ac_label} → {current_asset_class}"
            else:
                current_asset_class = None
                entry["classification"] = "asset_class_skipped"
                entry["detail"] = f"{ac_label} (unsupported — rows below will be skipped)"
            entry["asset_class"] = current_asset_class
            result.append(entry)
            continue

        # Currency
        if _is_currency(row):
            entry["classification"] = "currency"
            entry["detail"] = (row[0] or "").strip()
            entry["asset_class"] = current_asset_class
            result.append(entry)
            continue

        # Total
        if _is_total(row):
            entry["classification"] = "total"
            entry["detail"] = (row[0] or "").strip()
            entry["asset_class"] = current_asset_class
            result.append(entry)
            continue

        # No asset class set
        if current_asset_class is None:
            entry["classification"] = "skipped_no_asset_class"
            first_cell = (row[0] or "").strip()[:40]
            entry["detail"] = f"No active asset class: {first_cell}"
            entry["asset_class"] = None
            result.append(entry)
            continue

        # No column mapping
        if not col_mapping:
            entry["classification"] = "skipped_no_mapping"
            first_cell = (row[0] or "").strip()[:40]
            entry["detail"] = f"No column mapping: {first_cell}"
            entry["asset_class"] = current_asset_class
            result.append(entry)
            continue

        # Map columns to fields
        fields: dict = {}
        for idx, field_name in col_mapping.items():
            if idx < len(row):
                fields[field_name] = (row[idx] or "").strip()

        symbol = fields.get("symbol", "").strip()
        if not symbol:
            entry["classification"] = "skipped_empty_symbol"
            entry["detail"] = f"Empty symbol in row: {row[:3]}"
            entry["asset_class"] = current_asset_class
            result.append(entry)
            continue

        # Try to build a Position
        try:
            opt_fields: dict = {}
            if current_asset_class == "OPT":
                opt_fields = _parse_option_symbol(symbol)

            Position(
                symbol=symbol,
                asset_class=current_asset_class,
                quantity=_to_decimal(fields.get("quantity")),
                cost_basis=_to_decimal(fields.get("cost_basis")),
                market_price=_to_decimal(fields.get("market_price")),
                market_value=_to_decimal(fields.get("market_value")),
                unrealized_pnl=_to_decimal(fields.get("unrealized_pnl")),
                currency="USD",
                statement_date=date.today(),
                **opt_fields,
            )
            entry["classification"] = "parsed"
            entry["detail"] = symbol
        except Exception as e:
            entry["classification"] = "error"
            entry["detail"] = f"{symbol}: {e}"

        entry["asset_class"] = current_asset_class
        result.append(entry)

    return result


# ── Trade extraction ─────────────────────────────────────────────────────────

def _extract_trades(rows: list[list[str]]) -> tuple[list[Trade], list[dict]]:
    """Extract Trades from an account's rows."""
    trades: list[Trade] = []
    skipped: list[dict] = []
    in_section = False
    current_asset_class: str | None = None
    col_mapping: dict[int, str] = {}

    for row in rows:
        if not row:
            continue

        section = _is_section_header(row)
        if section == "Trades":
            in_section = True
            # Don't reset asset class/mapping — trades span multiple pages
            # and the section header repeats without column headers
            continue
        elif section is not None and section != "Trades" and in_section:
            break

        if not in_section:
            continue

        if _is_column_header(row):
            col_mapping = _map_columns(row, TRADE_COL_MAP)
            continue

        ac_label = _is_asset_class(row)
        if ac_label is not None:
            if ac_label in ASSET_CLASS_MAP:
                current_asset_class = ASSET_CLASS_MAP[ac_label]
            else:
                current_asset_class = None
                logger.warning("Skipping unsupported asset class in trades: %s", ac_label)
            continue

        if _is_currency(row):
            continue

        if _is_total(row):
            continue

        if current_asset_class is None:
            continue

        if not col_mapping:
            continue

        fields: dict = {}
        for idx, field_name in col_mapping.items():
            if idx < len(row):
                fields[field_name] = (row[idx] or "").strip()

        symbol = fields.get("symbol", "").strip()
        if not symbol:
            continue

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
                currency="USD",
                **opt_fields,
            )
            trades.append(trade)
        except Exception as e:
            logger.warning("Skipping trade row: %s — %s", row, e)
            skipped.append({"section": "Trades", "reason": str(e), "row": row})

    return trades, skipped


# ── Public API ────────────────────────────────────────────────────────────────

def parse_statement(pdf_file: BinaryIO) -> list[ParsedStatement]:
    """Parse an IBKR Custom Date Range PDF into ParsedStatements.

    Multi-account PDFs produce one ParsedStatement per account.

    Args:
        pdf_file: A file-like object (e.g. BytesIO or UploadedFile) of the PDF.

    Returns:
        List of ParsedStatement, one per account found.

    Raises:
        ValueError: If no tables found or required metadata missing.
    """
    all_rows = _extract_tables(pdf_file)
    if not all_rows:
        raise ValueError("No tables found in the PDF. Is this a valid IBKR statement?")

    account_groups = _split_accounts(all_rows)
    results: list[ParsedStatement] = []

    for group in account_groups:
        meta = _extract_meta(group)
        positions, pos_skipped = _extract_positions(group, meta.period_end)
        trades, trade_skipped = _extract_trades(group)
        skipped = pos_skipped + trade_skipped

        logger.info(
            "Parsed account %s (%s to %s): %d positions, %d trades, %d skipped",
            meta.account_id, meta.period_start, meta.period_end,
            len(positions), len(trades), len(skipped),
        )

        results.append(ParsedStatement(
            meta=meta,
            positions=positions,
            trades=trades,
            skipped_rows=skipped,
        ))

    return results
