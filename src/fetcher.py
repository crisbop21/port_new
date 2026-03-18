"""SEC EDGAR data fetcher for stock fundamental metrics.

Free tier only — no API keys required.  SEC asks for a User-Agent with
contact info; configure via EDGAR_USER_AGENT env var or it defaults to
a generic journal identifier.

Rate limit: SEC allows 10 req/s.  We throttle to ~5 req/s to be safe.
"""

import logging
import os
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

from src.models import StockMetric

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_DEFAULT_USER_AGENT = "IBKRTradeJournal/1.0 (bojaca.cristian21@gmail.com)"
_REQUEST_INTERVAL = 0.2  # seconds between SEC requests (~5 req/s)
_REQUEST_TIMEOUT = 15  # seconds

# XBRL taxonomy tags → our metric_name mapping.
# Each entry: our_name → list of XBRL tags to try (first match wins).
XBRL_TAG_MAP: dict[str, list[str]] = {
    "revenue": [
        "us-gaap:Revenues",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:SalesRevenueNet",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
    ],
    "net_income": [
        "us-gaap:NetIncomeLoss",
        "us-gaap:ProfitLoss",
    ],
    "eps_basic": [
        "us-gaap:EarningsPerShareBasic",
    ],
    "eps_diluted": [
        "us-gaap:EarningsPerShareDiluted",
    ],
    "total_assets": [
        "us-gaap:Assets",
    ],
    "total_liabilities": [
        "us-gaap:Liabilities",
    ],
    "stockholders_equity": [
        "us-gaap:StockholdersEquity",
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "shares_outstanding": [
        "us-gaap:CommonStockSharesOutstanding",
        "dei:EntityCommonStockSharesOutstanding",
    ],
    "operating_income": [
        "us-gaap:OperatingIncomeLoss",
    ],
    "cash_and_equivalents": [
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
        "us-gaap:Cash",
    ],
    "gross_profit": [
        "us-gaap:GrossProfit",
    ],
    "current_assets": [
        "us-gaap:AssetsCurrent",
    ],
    "current_liabilities": [
        "us-gaap:LiabilitiesCurrent",
    ],
    "long_term_debt": [
        "us-gaap:LongTermDebt",
        "us-gaap:LongTermDebtNoncurrent",
    ],
    "capital_expenditures": [
        "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
        "us-gaap:CapitalExpenditureDiscontinuedOperations",
    ],
    "dividends_paid": [
        "us-gaap:PaymentsOfDividends",
        "us-gaap:PaymentsOfDividendsCommonStock",
    ],
    "interest_expense": [
        "us-gaap:InterestExpense",
        "us-gaap:InterestExpenseDebt",
    ],
}


# Common ticker aliases — maps tickers that SEC doesn't list to their
# SEC-recognized equivalents (same company, different share class or name).
_TICKER_ALIASES: dict[str, str] = {
    "GOOG": "GOOGL",       # Alphabet Class C → Class A
    "BRK.A": "BRK-A",      # Berkshire variants
    "BRK/A": "BRK-A",
    "BRK.B": "BRK-B",
    "BRK/B": "BRK-B",
}


def _get_user_agent() -> str:
    return os.environ.get("EDGAR_USER_AGENT", _DEFAULT_USER_AGENT)


def _get_session() -> requests.Session:
    """Build a requests session with the required User-Agent header."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": _get_user_agent(),
        "Accept-Encoding": "gzip, deflate",
    })
    return s


_last_request_time: float = 0.0


def _throttle() -> None:
    """Enforce minimum interval between SEC requests."""
    global _last_request_time
    elapsed = time.monotonic() - _last_request_time
    if elapsed < _REQUEST_INTERVAL:
        time.sleep(_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.monotonic()


def _sec_get(session: requests.Session, url: str) -> dict | None:
    """GET a JSON endpoint from SEC EDGAR with throttling and error handling.

    Returns parsed JSON dict on success, None on failure.
    Every failure is logged with the URL and status for debugging.
    """
    _throttle()
    logger.debug("SEC request: GET %s", url)
    try:
        resp = session.get(url, timeout=_REQUEST_TIMEOUT)
    except requests.RequestException as e:
        logger.error("SEC request failed (network): %s — %s", url, e)
        return None

    if resp.status_code == 403:
        logger.error(
            "SEC 403 (forbidden): %s — likely bad User-Agent. "
            "SEC requires format 'Company admin@email.com'. "
            "Set EDGAR_USER_AGENT env var. Current: %s",
            url, session.headers.get("User-Agent"),
        )
        return None
    if resp.status_code == 404:
        logger.warning("SEC 404 (not found): %s", url)
        return None
    if resp.status_code == 429:
        logger.warning("SEC 429 (rate limited): %s — backing off 2s", url)
        time.sleep(2)
        return None
    if resp.status_code != 200:
        logger.error(
            "SEC unexpected status %d: %s — body preview: %.200s",
            resp.status_code, url, resp.text,
        )
        return None

    try:
        return resp.json()
    except ValueError as e:
        logger.error("SEC response not valid JSON: %s — %s", url, e)
        return None


# ── CIK lookup ───────────────────────────────────────────────────────────────

# Cached in-memory for the process lifetime (file is ~2 MB, changes rarely).
_cik_cache: dict[str, str] | None = None


def _load_cik_map(session: requests.Session) -> dict[str, str]:
    """Download SEC ticker→CIK mapping and cache it.

    Returns dict mapping uppercase ticker → zero-padded CIK string.
    """
    global _cik_cache
    if _cik_cache is not None:
        return _cik_cache

    url = "https://www.sec.gov/files/company_tickers.json"
    data = _sec_get(session, url)
    if data is None:
        logger.error("Failed to load CIK map from %s", url)
        # Do NOT cache empty result — allow retry on next call
        return {}

    mapping: dict[str, str] = {}
    for entry in data.values():
        ticker = str(entry.get("ticker", "")).upper().strip()
        cik_int = entry.get("cik_str")
        if ticker and cik_int is not None:
            mapping[ticker] = str(cik_int).zfill(10)

    logger.info("Loaded CIK map: %d tickers", len(mapping))
    _cik_cache = mapping
    return _cik_cache


def get_cik(ticker: str) -> str | None:
    """Look up the CIK for a ticker symbol.

    Returns the zero-padded CIK string, or None if not found.
    """
    session = _get_session()
    cik_map = _load_cik_map(session)
    normalized = ticker.upper().strip()
    cik = cik_map.get(normalized)
    if cik is None and normalized in _TICKER_ALIASES:
        cik = cik_map.get(_TICKER_ALIASES[normalized])
    if cik is None:
        logger.info("No CIK found for ticker '%s' — may be ETF or delisted", ticker)
    else:
        logger.debug("CIK for %s: %s", ticker, cik)
    return cik


# ── Company facts ────────────────────────────────────────────────────────────


def _get_company_facts(session: requests.Session, cik: str) -> dict | None:
    """Fetch the full XBRL facts blob for a company from SEC EDGAR."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    data = _sec_get(session, url)
    if data is None:
        logger.warning("No company facts for CIK %s", cik)
    return data


def _extract_fact_values(
    facts: dict,
    xbrl_tag: str,
) -> list[dict[str, Any]]:
    """Pull the list of reported values for a single XBRL tag.

    Navigates the nested structure:
        facts → us-gaap (or dei) → tag → units → USD (or shares, pure)

    Returns a flat list of dicts with keys: val, end, form, fy, fp, filed.
    Returns [] if the tag is missing (not an error — many tags are optional).
    """
    taxonomy, tag_name = xbrl_tag.split(":", 1)
    taxonomy_data = facts.get("facts", {}).get(taxonomy, {})
    tag_data = taxonomy_data.get(tag_name, {})
    units = tag_data.get("units", {})

    # Try USD first, then shares, then pure (for ratios like EPS)
    for unit_key in ["USD", "shares", "USD/shares", "pure"]:
        if unit_key in units:
            return units[unit_key]

    return []


def _pick_latest_annual(values: list[dict]) -> dict | None:
    """From a list of XBRL fact entries, pick the most recent 10-K value.

    Falls back to 10-Q if no 10-K exists.

    Returns the single best dict, or None.
    """
    results = _pick_all_annual(values)
    return results[0] if results else None


def _pick_all_annual(values: list[dict]) -> list[dict]:
    """From a list of XBRL fact entries, return all 10-K and 10-Q values.

    Includes both annual and quarterly filings.  Deduplicates by
    (end date, fiscal period), keeping the most recently filed entry for
    each combination.

    Returns a list sorted by end date descending (most recent first).
    """
    if not values:
        return []

    # Include both 10-K and 10-Q filings
    candidates = [v for v in values if v.get("form") in ("10-K", "10-Q")]
    if not candidates:
        candidates = values

    # Deduplicate by (end, fp) — keep the most recently filed for each combo.
    # This preserves both the Q3 YTD and the FY for the same end-date
    # (which happen when fiscal year ends on a quarter boundary).
    by_key: dict[tuple[str, str], dict] = {}
    for v in candidates:
        end = v.get("end", "")
        fp = v.get("fp", "")
        filed = v.get("filed", "")
        key = (end, fp)
        if end and (key not in by_key or filed > by_key[key].get("filed", "")):
            by_key[key] = v

    # Sort by end date descending
    result = sorted(by_key.values(), key=lambda v: v.get("end", ""), reverse=True)
    return result


# ── Main orchestrator ────────────────────────────────────────────────────────


def fetch_metrics_for_symbol(symbol: str) -> tuple[list[StockMetric], list[str]]:
    """Fetch fundamental metrics for a single stock symbol from SEC EDGAR.

    Returns:
        (metrics, errors) — list of validated StockMetric objects and a list
        of human-readable error/warning strings for the UI to display.
    """
    symbol = symbol.upper().strip()
    errors: list[str] = []
    metrics: list[StockMetric] = []

    logger.info("=== Fetching metrics for %s ===", symbol)

    # Step 1: CIK lookup (try alias if direct lookup fails)
    session = _get_session()
    cik_map = _load_cik_map(session)
    if not cik_map:
        msg = (
            "Failed to download SEC ticker database — "
            "check your internet connection and EDGAR_USER_AGENT env var"
        )
        logger.error(msg)
        errors.append(msg)
        return metrics, errors
    cik = cik_map.get(symbol)
    lookup_symbol = symbol
    if cik is None and symbol in _TICKER_ALIASES:
        lookup_symbol = _TICKER_ALIASES[symbol]
        cik = cik_map.get(lookup_symbol)
        if cik is not None:
            logger.info("%s: resolved via alias → %s", symbol, lookup_symbol)
    if cik is None:
        msg = f"{symbol}: no CIK found — not in SEC database (ETF or foreign?)"
        logger.warning(msg)
        errors.append(msg)
        return metrics, errors

    logger.info("%s: CIK = %s", symbol, cik)

    # Step 2: Fetch company facts
    facts = _get_company_facts(session, cik)
    if facts is None:
        msg = f"{symbol} (CIK {cik}): failed to fetch company facts from EDGAR"
        logger.error(msg)
        errors.append(msg)
        return metrics, errors

    company_name = facts.get("entityName", "Unknown")
    logger.info("%s: entity = %s", symbol, company_name)

    # Step 3: Extract each metric (all historical periods)
    for metric_name, xbrl_tags in XBRL_TAG_MAP.items():
        all_values: list[dict] = []
        matched_tag: str | None = None

        for tag in xbrl_tags:
            values = _extract_fact_values(facts, tag)
            if values:
                all_values = _pick_all_annual(values)
                if all_values:
                    matched_tag = tag
                    break  # first matching tag wins

        if not all_values:
            msg = f"{symbol}: metric '{metric_name}' — no data found (tried {len(xbrl_tags)} XBRL tags)"
            logger.debug(msg)
            # Not an error — many companies don't report all metrics
            continue

        for entry in all_values:
            # Parse the value
            raw_val = entry.get("val")
            raw_end = entry.get("end")
            raw_start = entry.get("start")
            fiscal_period = entry.get("fp")  # FY, Q1, Q2, Q3, Q4
            filing_form = entry.get("form", "unknown")

            if raw_val is None or raw_end is None:
                msg = (
                    f"{symbol}: metric '{metric_name}' has null val or end date "
                    f"(tag={matched_tag}, raw={entry})"
                )
                logger.warning(msg)
                errors.append(msg)
                continue

            try:
                metric_value = Decimal(str(raw_val))
            except (InvalidOperation, ValueError) as e:
                msg = (
                    f"{symbol}: metric '{metric_name}' value not numeric: "
                    f"raw_val={raw_val!r} — {e}"
                )
                logger.warning(msg)
                errors.append(msg)
                continue

            try:
                period_end = date.fromisoformat(str(raw_end))
            except ValueError as e:
                msg = (
                    f"{symbol}: metric '{metric_name}' bad date: "
                    f"raw_end={raw_end!r} — {e}"
                )
                logger.warning(msg)
                errors.append(msg)
                continue

            period_start = None
            if raw_start:
                try:
                    period_start = date.fromisoformat(str(raw_start))
                except ValueError:
                    pass  # non-critical — we can still use fiscal_period

            # Build validated model
            try:
                metric = StockMetric(
                    symbol=symbol,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    period_end=period_end,
                    period_start=period_start,
                    fiscal_period=fiscal_period,
                    source="SEC_EDGAR",
                    cik=cik,
                    filing_type=filing_form,
                )
                metrics.append(metric)
                logger.debug(
                    "%s: %s = %s (period=%s→%s, fp=%s, tag=%s, form=%s)",
                    symbol, metric_name, metric_value,
                    period_start, period_end,
                    fiscal_period, matched_tag, filing_form,
                )
            except Exception as e:
                msg = f"{symbol}: Pydantic validation failed for '{metric_name}': {e}"
                logger.error(msg)
                errors.append(msg)

    logger.info(
        "%s: extracted %d metrics, %d issues",
        symbol, len(metrics), len(errors),
    )
    return metrics, errors


def clear_cik_cache() -> None:
    """Clear the in-memory CIK cache (useful for testing)."""
    global _cik_cache
    _cik_cache = None
    logger.debug("CIK cache cleared")
