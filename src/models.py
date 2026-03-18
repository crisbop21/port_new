import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, field_validator, model_validator

logger = logging.getLogger(__name__)

SUPPORTED_ASSET_CLASSES = {"STK", "OPT", "ETF"}


class StatementMeta(BaseModel):
    account_id: str
    period_start: date
    period_end: date
    base_currency: str


class Position(BaseModel):
    symbol: str
    asset_class: str
    quantity: Decimal
    cost_basis: Decimal
    market_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    currency: str
    statement_date: date
    # Options-only fields
    expiry: date | None = None
    strike: Decimal | None = None
    right: Literal["C", "P"] | None = None

    @model_validator(mode="after")
    def check_asset_class(self):
        if self.asset_class not in SUPPORTED_ASSET_CLASSES:
            raise ValueError(
                f"Unsupported asset class '{self.asset_class}' for {self.symbol}. "
                f"Supported: {SUPPORTED_ASSET_CLASSES}"
            )
        if self.asset_class == "OPT" and (self.expiry is None or self.strike is None or self.right is None):
            raise ValueError(
                f"Options position {self.symbol} missing expiry, strike, or right."
            )
        return self


class Trade(BaseModel):
    trade_date: datetime
    symbol: str
    asset_class: str
    side: Literal["BOT", "SLD"]
    quantity: Decimal
    price: Decimal
    proceeds: Decimal
    commission: Decimal
    realized_pnl: Decimal
    currency: str
    # Options-only fields
    expiry: date | None = None
    strike: Decimal | None = None
    right: Literal["C", "P"] | None = None

    @model_validator(mode="after")
    def check_asset_class(self):
        if self.asset_class not in SUPPORTED_ASSET_CLASSES:
            raise ValueError(
                f"Unsupported asset class '{self.asset_class}' for {self.symbol}. "
                f"Supported: {SUPPORTED_ASSET_CLASSES}"
            )
        if self.asset_class == "OPT" and (self.expiry is None or self.strike is None or self.right is None):
            raise ValueError(
                f"Options trade {self.symbol} missing expiry, strike, or right."
            )
        return self


class ParsedStatement(BaseModel):
    meta: StatementMeta
    positions: list[Position] = []
    trades: list[Trade] = []
    skipped_rows: list[dict] = []


# ── Stock metrics ────────────────────────────────────────────────────────────

KNOWN_METRICS = {
    "revenue",
    "net_income",
    "eps_basic",
    "eps_diluted",
    "total_assets",
    "total_liabilities",
    "stockholders_equity",
    "shares_outstanding",
    "operating_income",
    "cash_and_equivalents",
    "gross_profit",
    "current_assets",
    "current_liabilities",
    "long_term_debt",
    "capital_expenditures",
    "dividends_paid",
    "interest_expense",
}


class StockMetric(BaseModel):
    """A single fundamental metric for a stock, sourced from SEC EDGAR."""

    symbol: str
    metric_name: str
    metric_value: Decimal
    period_end: date
    period_start: date | None = None       # start of reporting period (for duration calc)
    fiscal_period: str | None = None       # 'FY', 'Q1', 'Q2', 'Q3', 'Q4'
    source: str = "SEC_EDGAR"
    cik: str | None = None
    filing_type: str | None = None

    @field_validator("symbol")
    @classmethod
    def symbol_not_blank(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("symbol must not be blank")
        return v

    @field_validator("metric_name")
    @classmethod
    def metric_name_known(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in KNOWN_METRICS:
            logger.warning("Metric '%s' not in KNOWN_METRICS — allowing but verify", v)
        return v

    @model_validator(mode="after")
    def check_source_has_cik(self):
        if self.source == "SEC_EDGAR" and not self.cik:
            raise ValueError(
                f"SEC_EDGAR metric '{self.metric_name}' for {self.symbol} "
                f"must include a CIK for traceability."
            )
        return self


# ── Daily prices ─────────────────────────────────────────────────────────────


class DailyPrice(BaseModel):
    """One day of OHLCV price data for a symbol, sourced from Yahoo Finance."""

    symbol: str
    price_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adj_close: Decimal
    volume: int

    @field_validator("symbol")
    @classmethod
    def symbol_not_blank(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("symbol must not be blank")
        return v
