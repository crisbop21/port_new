"""Fetch daily OHLCV prices from Yahoo Finance via yfinance.

Free tier only — no API key required.
"""

import logging
from datetime import date, timedelta
from decimal import Decimal

import yfinance as yf

from src.models import DailyPrice

logger = logging.getLogger(__name__)


def fetch_daily_prices(
    symbol: str,
    start: date | None = None,
    end: date | None = None,
) -> tuple[list[DailyPrice], list[str]]:
    """Fetch daily OHLCV data for a single symbol.

    Args:
        symbol: Ticker symbol (e.g. "AAPL").
        start: First date to fetch (inclusive). Defaults to 1 year ago.
        end: Last date to fetch (inclusive). Defaults to today.

    Returns:
        (prices, errors) — list of validated DailyPrice objects and error messages.
    """
    if not start:
        start = date.today() - timedelta(days=365)
    if not end:
        end = date.today()

    prices: list[DailyPrice] = []
    errors: list[str] = []

    try:
        ticker = yf.Ticker(symbol)
        # yfinance end is exclusive, so add 1 day
        df = ticker.history(
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            auto_adjust=False,
        )

        if df.empty:
            errors.append(f"No price data returned for {symbol}")
            return prices, errors

        for idx, row in df.iterrows():
            try:
                price_date = idx.date() if hasattr(idx, "date") else idx
                prices.append(DailyPrice(
                    symbol=symbol.upper(),
                    price_date=price_date,
                    open=Decimal(str(round(row["Open"], 4))),
                    high=Decimal(str(round(row["High"], 4))),
                    low=Decimal(str(round(row["Low"], 4))),
                    close=Decimal(str(round(row["Close"], 4))),
                    adj_close=Decimal(str(round(row.get("Adj Close", row["Close"]), 4))),
                    volume=int(row["Volume"]),
                ))
            except Exception as e:
                errors.append(f"{symbol} row {idx}: {e}")

    except Exception as e:
        errors.append(f"Failed to fetch {symbol}: {e}")
        logger.exception("yfinance error for %s", symbol)

    logger.info("Fetched %d prices for %s (%d errors)", len(prices), symbol, len(errors))
    return prices, errors


def fetch_missing_prices(
    symbol: str,
    start: date,
    end: date,
) -> tuple[list[DailyPrice], list[str]]:
    """Fetch only the prices not already stored in the database.

    Checks the existing date range in DB and only fetches the gaps
    (before the earliest stored date and/or after the latest stored date).
    Returns (prices, errors) — only the newly fetched data.
    """
    from src.db import get_price_date_range

    min_stored, max_stored = get_price_date_range(symbol)

    if min_stored is None:
        # No data at all — fetch the full range
        return fetch_daily_prices(symbol, start=start, end=end)

    all_prices: list[DailyPrice] = []
    all_errors: list[str] = []

    # Fetch earlier gap: requested start → day before earliest stored
    if start < min_stored:
        early_end = min_stored - timedelta(days=1)
        if early_end >= start:
            prices, errs = fetch_daily_prices(symbol, start=start, end=early_end)
            all_prices.extend(prices)
            all_errors.extend(errs)

    # Fetch later gap: day after latest stored → requested end
    if end > max_stored:
        late_start = max_stored + timedelta(days=1)
        if late_start <= end:
            prices, errs = fetch_daily_prices(symbol, start=late_start, end=end)
            all_prices.extend(prices)
            all_errors.extend(errs)

    if not all_prices and not all_errors:
        logger.info("No missing prices for %s (%s to %s already covered)", symbol, start, end)

    return all_prices, all_errors


def fetch_prices_for_symbols(
    symbols: list[str],
    start: date | None = None,
    end: date | None = None,
) -> tuple[list[DailyPrice], list[str]]:
    """Fetch daily prices for multiple symbols sequentially.

    Returns:
        (all_prices, all_errors)
    """
    all_prices: list[DailyPrice] = []
    all_errors: list[str] = []

    for sym in symbols:
        prices, errs = fetch_daily_prices(sym, start=start, end=end)
        all_prices.extend(prices)
        all_errors.extend(errs)

    return all_prices, all_errors
