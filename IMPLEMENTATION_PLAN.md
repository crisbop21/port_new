# IBKR Trade Journal — Implementation Plan

## Current State (Completed)

| Feature | Status |
|---------|--------|
| PDF parsing (multi-account, stocks/ETFs/options) | Done |
| Supabase storage with deduplication | Done |
| Upload page with duplicate preview | Done |
| Holdings page with snapshot viewer | Done |
| Consolidated holdings table + donut chart | Done |
| Trade History page with filters & analytics | Done |
| Dashboard with portfolio metrics & charts | Done |
| Holdings reconciliation (forward-roll validation) | Done |
| Per-holding trade ledger in reconciliation | Done |

---

## Phase 2 — Data Integrity & Analytics

### Step 1: Cash Balance Parsing & Display

**Goal:** Parse the "Cash Report" section from IBKR PDFs and display cash balances alongside holdings.

**Implementation:**
- Add `CashBalance` model to `src/models.py` with fields: `currency`, `ending_balance`, `statement_date`
- Add `_extract_cash_report()` to `src/parser.py` to parse the Cash Report section
- Add `cash_balances` field to `ParsedStatement`
- Create `cash_balances` table in `sql/004_cash.sql`
- Add DB functions: `_cash_row()`, insert in `upsert_statement()`, `get_cash_balances()`
- Display cash balances on Holdings page

**Tests:**
```python
# tests/test_models.py
class TestCashBalance:
    def test_valid_cash_balance(self):
        cb = CashBalance(currency="USD", ending_balance=Decimal("10000.00"), statement_date=date(2026, 3, 6))
        assert cb.ending_balance == Decimal("10000.00")

    def test_negative_balance(self):
        cb = CashBalance(currency="USD", ending_balance=Decimal("-500.00"), statement_date=date(2026, 3, 6))
        assert cb.ending_balance == Decimal("-500.00")

    def test_decimal_precision_preserved(self):
        cb = CashBalance(currency="SGD", ending_balance=Decimal("12345.67"), statement_date=date(2026, 3, 6))
        assert cb.ending_balance == Decimal("12345.67")

# tests/test_parser.py
def _cash_report_rows():
    return [
        ["Cash Report", "", "", "", ""],
        ["Currency", "Prior Period", "Deposits", "Withdrawals", "Ending Balance"],
        ["USD", "10,000.00", "5,000.00", "0.00", "15,000.00"],
        ["SGD", "0.00", "0.00", "0.00", "0.00"],
        ["Total", "10,000.00", "5,000.00", "0.00", "15,000.00"],
    ]

class TestExtractCashReport:
    def test_extracts_cash_balances(self):
        rows = _cash_report_rows()
        balances = _extract_cash_report(rows, date(2026, 3, 6))
        assert len(balances) == 2  # USD and SGD, not Total
        usd = [b for b in balances if b.currency == "USD"][0]
        assert usd.ending_balance == Decimal("15000.00")

    def test_skips_total_row(self):
        rows = _cash_report_rows()
        balances = _extract_cash_report(rows, date(2026, 3, 6))
        symbols = [b.currency for b in balances]
        assert "Total" not in symbols

    def test_no_cash_section_returns_empty(self):
        rows = _position_rows()  # no cash report
        balances = _extract_cash_report(rows, date(2026, 3, 6))
        assert balances == []

# tests/test_db.py
class TestCashRow:
    def test_structure(self):
        cb = CashBalance(currency="USD", ending_balance=Decimal("15000.00"), statement_date=date(2026, 3, 6))
        row = _cash_row(cb, "stmt-123")
        assert row["statement_id"] == "stmt-123"
        assert row["currency"] == "USD"
        assert row["ending_balance"] == "15000.00"
```

---

### Step 2: Dividend & Interest Tracking

**Goal:** Parse Dividends and Interest sections from IBKR PDFs and display income history.

**Implementation:**
- Add `Income` model to `src/models.py` with fields: `income_date`, `symbol`, `income_type` (dividend/interest), `amount`, `currency`
- Add `_extract_dividends()` and `_extract_interest()` to `src/parser.py`
- Add `income` list to `ParsedStatement`
- Create `income` table in `sql/005_income.sql`
- Add DB functions: `_income_row()`, dedup fingerprint, insert in `upsert_statement()`, `get_income()`
- Add Income tab or section to Dashboard

**Tests:**
```python
# tests/test_models.py
class TestIncome:
    def test_valid_dividend(self):
        inc = Income(income_date=date(2026, 2, 15), symbol="AAPL", income_type="dividend",
                     amount=Decimal("92.00"), currency="USD")
        assert inc.income_type == "dividend"

    def test_valid_interest(self):
        inc = Income(income_date=date(2026, 2, 28), symbol="", income_type="interest",
                     amount=Decimal("12.50"), currency="USD")
        assert inc.amount == Decimal("12.50")

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            Income(income_date=date(2026, 2, 15), symbol="AAPL", income_type="bonus",
                   amount=Decimal("50"), currency="USD")

# tests/test_parser.py
def _dividend_rows():
    return [
        ["Dividends", "", "", "", ""],
        ["Currency", "Date", "Description", "Amount", ""],
        ["USD", "2026-02-15", "AAPL (US0378331005) Cash Dividend", "92.00", ""],
        ["USD", "2026-02-20", "MSFT (US5949181045) Cash Dividend", "68.00", ""],
        ["Total", "", "", "160.00", ""],
    ]

class TestExtractDividends:
    def test_extracts_dividends(self):
        rows = _dividend_rows()
        dividends = _extract_dividends(rows)
        assert len(dividends) == 2
        assert dividends[0].symbol == "AAPL"
        assert dividends[0].amount == Decimal("92.00")

    def test_skips_total_row(self):
        rows = _dividend_rows()
        dividends = _extract_dividends(rows)
        assert all(d.symbol != "Total" for d in dividends)

    def test_parses_symbol_from_description(self):
        rows = _dividend_rows()
        dividends = _extract_dividends(rows)
        assert dividends[1].symbol == "MSFT"

# tests/test_db.py
class TestIncomeFingerprint:
    def test_same_income_same_fingerprint(self):
        row1 = {"income_date": "2026-02-15", "symbol": "AAPL", "income_type": "dividend", "amount": "92.00"}
        row2 = {"income_date": "2026-02-15", "symbol": "AAPL", "income_type": "dividend", "amount": "92.00"}
        assert _income_fingerprint(row1) == _income_fingerprint(row2)

    def test_different_dates_different_fingerprint(self):
        row1 = {"income_date": "2026-02-15", "symbol": "AAPL", "income_type": "dividend", "amount": "92.00"}
        row2 = {"income_date": "2026-03-15", "symbol": "AAPL", "income_type": "dividend", "amount": "92.00"}
        assert _income_fingerprint(row1) != _income_fingerprint(row2)
```

---

### Step 3: Withholding Tax Tracking

**Goal:** Parse Withholding Tax section and correlate with dividends for net income calculation.

**Implementation:**
- Add `WithholdingTax` model to `src/models.py` with fields: `tax_date`, `symbol`, `amount`, `currency`
- Add `_extract_withholding_tax()` to `src/parser.py`
- Add `withholding_taxes` list to `ParsedStatement`
- Create `withholding_taxes` table in `sql/006_withholding.sql`
- Add DB functions for storage/retrieval
- Show gross vs. net dividend income on Dashboard

**Tests:**
```python
# tests/test_parser.py
def _withholding_tax_rows():
    return [
        ["Withholding Tax", "", "", "", ""],
        ["Currency", "Date", "Description", "Amount", ""],
        ["USD", "2026-02-15", "AAPL (US0378331005) Cash Dividend", "-13.80", ""],
        ["Total", "", "", "-13.80", ""],
    ]

class TestExtractWithholdingTax:
    def test_extracts_tax(self):
        rows = _withholding_tax_rows()
        taxes = _extract_withholding_tax(rows)
        assert len(taxes) == 1
        assert taxes[0].amount == Decimal("-13.80")
        assert taxes[0].symbol == "AAPL"

    def test_amount_is_negative(self):
        rows = _withholding_tax_rows()
        taxes = _extract_withholding_tax(rows)
        assert taxes[0].amount < 0

# tests/test_models.py
class TestWithholdingTax:
    def test_valid(self):
        wt = WithholdingTax(tax_date=date(2026, 2, 15), symbol="AAPL",
                            amount=Decimal("-13.80"), currency="USD")
        assert wt.amount == Decimal("-13.80")

    def test_precision_preserved(self):
        wt = WithholdingTax(tax_date=date(2026, 2, 15), symbol="AAPL",
                            amount=Decimal("-13.8042"), currency="USD")
        assert wt.amount == Decimal("-13.8042")
```

---

### Step 4: Transaction Fees Parsing

**Goal:** Parse Transaction Fees section for a complete cost picture.

**Implementation:**
- Add `Fee` model to `src/models.py` with fields: `fee_date`, `symbol`, `description`, `amount`, `currency`
- Add `_extract_fees()` to `src/parser.py`
- Add `fees` list to `ParsedStatement`
- Create `fees` table in `sql/007_fees.sql`
- Show total fees alongside commissions on Trade History page

**Tests:**
```python
# tests/test_parser.py
def _fee_rows():
    return [
        ["Transaction Fees", "", "", "", ""],
        ["Currency", "Date", "Description", "Amount", ""],
        ["USD", "2026-01-31", "IBKR Tiered - AAPL", "-0.35", ""],
        ["Total", "", "", "-0.35", ""],
    ]

class TestExtractFees:
    def test_extracts_fees(self):
        rows = _fee_rows()
        fees = _extract_fees(rows)
        assert len(fees) == 1
        assert fees[0].amount == Decimal("-0.35")

    def test_skips_total(self):
        rows = _fee_rows()
        fees = _extract_fees(rows)
        assert all(f.description != "Total" for f in fees)

# tests/test_models.py
class TestFee:
    def test_valid(self):
        f = Fee(fee_date=date(2026, 1, 31), symbol="AAPL",
                description="IBKR Tiered", amount=Decimal("-0.35"), currency="USD")
        assert f.amount == Decimal("-0.35")
```

---

### Step 5: Multi-Currency NAV Tracking

**Goal:** Parse Net Asset Value section to track total portfolio value over time.

**Implementation:**
- Add `NAVSnapshot` model to `src/models.py` with fields: `statement_date`, `total_value`, `cash_value`, `stock_value`, `option_value`, `currency`
- Add `_extract_nav()` to `src/parser.py`
- Add `nav` field to `ParsedStatement`
- Create `nav_snapshots` table in `sql/008_nav.sql`
- Add portfolio value time series chart to Dashboard

**Tests:**
```python
# tests/test_parser.py
class TestExtractNAV:
    def test_extracts_nav_totals(self):
        rows = _nav_rows()
        nav = _extract_nav(rows, date(2026, 3, 6))
        assert nav.total_value == Decimal("45000.00")

    def test_parses_prior_and_current(self):
        rows = _nav_rows()
        nav = _extract_nav(rows, date(2026, 3, 6))
        assert nav.statement_date == date(2026, 3, 6)

# tests/test_models.py
class TestNAVSnapshot:
    def test_valid(self):
        nav = NAVSnapshot(statement_date=date(2026, 3, 6), total_value=Decimal("45000.00"),
                          cash_value=Decimal("5000.00"), stock_value=Decimal("37550.00"),
                          option_value=Decimal("5806.98"), currency="SGD")
        assert nav.total_value == Decimal("45000.00")

    def test_decimal_precision(self):
        nav = NAVSnapshot(statement_date=date(2026, 3, 6), total_value=Decimal("123456.78"),
                          cash_value=Decimal("0"), stock_value=Decimal("0"),
                          option_value=Decimal("0"), currency="USD")
        assert nav.total_value == Decimal("123456.78")
```

---

## Phase 2b — UX Improvements

### Step 6: Bulk PDF Upload

**Goal:** Allow users to upload multiple PDFs at once and process them sequentially.

**Implementation:**
- Modify Upload page to accept multiple files via `st.file_uploader(accept_multiple_files=True)`
- Process each file in sequence, collecting results
- Show per-file status (success/error/duplicates) in a summary table
- Clear caches once after all files are processed

**Tests:**
```python
# tests/test_parser.py
class TestBulkParsing:
    def test_multiple_pdfs_parsed_independently(self):
        """Each PDF should produce independent ParsedStatements."""
        rows1 = _full_account_rows()
        rows2 = _account_info_rows("U1111111") + _nav_rows() + _position_rows()

        with patch("src.parser._extract_tables", return_value=rows1):
            r1 = parse_statement(io.BytesIO(b"fake1"))
        with patch("src.parser._extract_tables", return_value=rows2):
            r2 = parse_statement(io.BytesIO(b"fake2"))

        assert r1[0].meta.account_id != r2[0].meta.account_id
```

---

### Step 7: Export to CSV

**Goal:** Allow users to export filtered trades and holdings data to CSV.

**Implementation:**
- Add "Download CSV" button to Trade History page (uses `st.download_button`)
- Add "Download CSV" button to Holdings page
- Format Decimal values correctly in CSV output
- Include all visible columns

**Tests:**
```python
# tests/test_export.py (new file)
class TestCSVExport:
    def test_trades_to_csv(self):
        """Trades data exports with correct columns and decimal precision."""
        trades = [
            {"symbol": "AAPL", "trade_date": "2026-01-15T10:30:00", "side": "BOT",
             "quantity": "50", "price": "175.00", "commission": "-1.09", "realized_pnl": "0.00"},
        ]
        df = pd.DataFrame(trades)
        csv = df.to_csv(index=False)
        assert "175.00" in csv
        assert "-1.09" in csv

    def test_positions_to_csv(self):
        """Positions data exports with option fields included."""
        positions = [
            {"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
             "cost_basis": "15000.00", "market_value": "17550.00"},
        ]
        df = pd.DataFrame(positions)
        csv = df.to_csv(index=False)
        assert "15000.00" in csv
```

---

### Step 8: Statement Management Page

**Goal:** Let users view, inspect, and delete uploaded statements.

**Implementation:**
- Add new page `pages/5_Statements.py`
- Show table of all uploaded statements with: account_id, period, upload date, position count, trade count
- Add "Delete" button per statement (with confirmation)
- Add `delete_statement()` to `src/db.py` (cascading via FK)
- Clear caches after deletion

**Tests:**
```python
# tests/test_db.py
class TestDeleteStatement:
    @patch("src.db.get_client")
    def test_delete_removes_statement(self, mock_get_client):
        from src.db import delete_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        delete_statement("stmt-uuid-001")

        # Should call delete on statements table
        table_calls = [call.args[0] for call in client.table.call_args_list]
        assert "statements" in table_calls

    @patch("src.db.get_client")
    @patch("src.db.st")
    def test_delete_surfaces_error(self, mock_st, mock_get_client):
        from src.db import delete_statement

        client = MagicMock()
        client.table.side_effect = Exception("connection refused")
        mock_get_client.return_value = client

        with pytest.raises(Exception, match="connection refused"):
            delete_statement("stmt-uuid-001")

        mock_st.error.assert_called_once()
```

---

## Phase 3 — Intelligence (requires Anthropic API)

### Step 9: AI Trade Journal Entries

**Goal:** Use Claude API to generate narrative trade journal entries from trade data.

**Implementation:**
- Add `anthropic` to `requirements.txt`
- Create `src/ai.py` with `generate_journal_entry(trades, holdings)` function
- Add new page `pages/6_Journal.py` for AI-generated insights
- Rate-limit API calls, cache responses
- Allow users to provide their own API key via settings

**Tests:**
```python
# tests/test_ai.py (new file)
class TestJournalGeneration:
    @patch("src.ai.anthropic.Anthropic")
    def test_generates_entry_from_trades(self, mock_anthropic):
        mock_client = MagicMock()
        mock_client.messages.create.return_value = MagicMock(
            content=[MagicMock(text="Today you bought 50 shares of AAPL...")]
        )
        mock_anthropic.return_value = mock_client

        result = generate_journal_entry(trades=[...], holdings=[...])
        assert "AAPL" in result

    def test_empty_trades_returns_message(self):
        result = generate_journal_entry(trades=[], holdings=[])
        assert "no trades" in result.lower()

    @patch("src.ai.anthropic.Anthropic")
    def test_api_error_surfaces_cleanly(self, mock_anthropic):
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("rate limited")
        mock_anthropic.return_value = mock_client

        with pytest.raises(Exception):
            generate_journal_entry(trades=[...], holdings=[...])
```

---

### Step 10: AI-Powered Trade Pattern Analysis

**Goal:** Use Claude to identify trading patterns and suggest improvements.

**Implementation:**
- Add `analyze_patterns(trades, timeframe)` to `src/ai.py`
- Aggregate trade data by symbol, time-of-day, holding period
- Prompt Claude with structured trade stats for pattern identification
- Display analysis on Journal page with expandable sections

**Tests:**
```python
# tests/test_ai.py
class TestPatternAnalysis:
    def test_aggregates_trades_by_symbol(self):
        trades = [
            {"symbol": "AAPL", "side": "BOT", "realized_pnl": "0"},
            {"symbol": "AAPL", "side": "SLD", "realized_pnl": "250.00"},
            {"symbol": "MSFT", "side": "SLD", "realized_pnl": "-100.00"},
        ]
        stats = _aggregate_trade_stats(trades)
        assert stats["AAPL"]["trade_count"] == 2
        assert stats["AAPL"]["total_pnl"] == Decimal("250.00")
        assert stats["MSFT"]["total_pnl"] == Decimal("-100.00")

    def test_calculates_win_rate_per_symbol(self):
        trades = [
            {"symbol": "AAPL", "side": "SLD", "realized_pnl": "100"},
            {"symbol": "AAPL", "side": "SLD", "realized_pnl": "-50"},
            {"symbol": "AAPL", "side": "SLD", "realized_pnl": "200"},
        ]
        stats = _aggregate_trade_stats(trades)
        assert stats["AAPL"]["win_rate"] == pytest.approx(0.6667, rel=0.01)
```

---

## Summary of Steps

| Step | Feature | Phase | New Files | Modified Files |
|------|---------|-------|-----------|----------------|
| 1 | Cash Balance Parsing | 2 | `sql/004_cash.sql` | `models.py`, `parser.py`, `db.py`, `2_Holdings.py` |
| 2 | Dividend & Interest Tracking | 2 | `sql/005_income.sql` | `models.py`, `parser.py`, `db.py`, `4_Dashboard.py` |
| 3 | Withholding Tax Tracking | 2 | `sql/006_withholding.sql` | `models.py`, `parser.py`, `db.py`, `4_Dashboard.py` |
| 4 | Transaction Fees Parsing | 2 | `sql/007_fees.sql` | `models.py`, `parser.py`, `db.py`, `3_Trades.py` |
| 5 | Multi-Currency NAV Tracking | 2 | `sql/008_nav.sql` | `models.py`, `parser.py`, `db.py`, `4_Dashboard.py` |
| 6 | Bulk PDF Upload | 2b | — | `1_Upload.py` |
| 7 | Export to CSV | 2b | `tests/test_export.py` | `2_Holdings.py`, `3_Trades.py` |
| 8 | Statement Management Page | 2b | `pages/5_Statements.py` | `db.py` |
| 9 | AI Trade Journal Entries | 3 | `src/ai.py`, `pages/6_Journal.py`, `tests/test_ai.py` | `requirements.txt` |
| 10 | AI Trade Pattern Analysis | 3 | — | `src/ai.py`, `pages/6_Journal.py` |

## Test Coverage Approach

- **Unit tests** for all new models (Pydantic validation, precision)
- **Unit tests** for all parser functions (synthetic rows, edge cases)
- **Unit tests** for all DB functions (mocked Supabase client)
- **Fingerprint tests** for all new deduplication logic
- **Integration tests** against real PDFs (skipped when fixtures unavailable)
- All tests runnable with `pytest` — no external dependencies required
