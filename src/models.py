import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, model_validator

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
