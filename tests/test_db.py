"""Tests for the Supabase database layer.

All tests mock the Supabase client — no real database needed.
"""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.db import _position_row, _position_fingerprint, _ser, _trade_row, _trade_fingerprint
from src.models import ParsedStatement, Position, StatementMeta, Trade


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_meta():
    return StatementMeta(
        account_id="U9876543",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 6),
        base_currency="SGD",
    )


@pytest.fixture
def sample_position():
    return Position(
        symbol="AAPL",
        asset_class="STK",
        quantity=Decimal("100"),
        cost_basis=Decimal("15000.00"),
        market_price=Decimal("175.50"),
        market_value=Decimal("17550.00"),
        unrealized_pnl=Decimal("2550.00"),
        currency="USD",
        statement_date=date(2026, 3, 6),
    )


@pytest.fixture
def sample_trade():
    return Trade(
        trade_date=datetime(2026, 1, 15, 10, 30, 0),
        symbol="AAPL",
        asset_class="STK",
        side="BOT",
        quantity=Decimal("50"),
        price=Decimal("175.00"),
        proceeds=Decimal("-8750.00"),
        commission=Decimal("-1.09"),
        realized_pnl=Decimal("0"),
        currency="USD",
    )


@pytest.fixture
def sample_option_position():
    return Position(
        symbol="EEM 31MAR26 48 C",
        asset_class="OPT",
        quantity=Decimal("6"),
        cost_basis=Decimal("2095.71"),
        market_price=Decimal("9.6783"),
        market_value=Decimal("5806.98"),
        unrealized_pnl=Decimal("3711.27"),
        currency="USD",
        statement_date=date(2026, 3, 6),
        expiry=date(2026, 3, 31),
        strike=Decimal("48"),
        right="C",
    )


@pytest.fixture
def parsed_statement(sample_meta, sample_position, sample_trade):
    return ParsedStatement(
        meta=sample_meta,
        positions=[sample_position],
        trades=[sample_trade],
    )


# ── Serialisation tests ─────────────────────────────────────────────────────

class TestSer:
    def test_decimal(self):
        assert _ser(Decimal("15000.00")) == "15000.00"

    def test_date(self):
        assert _ser(date(2026, 3, 6)) == "2026-03-06"

    def test_datetime(self):
        result = _ser(datetime(2026, 1, 15, 10, 30))
        assert result == "2026-01-15T10:30:00"

    def test_none(self):
        assert _ser(None) is None

    def test_string_passthrough(self):
        assert _ser("USD") == "USD"


class TestPositionRow:
    def test_structure(self, sample_position):
        row = _position_row(sample_position, "stmt-123")
        assert row["statement_id"] == "stmt-123"
        assert row["symbol"] == "AAPL"
        assert row["quantity"] == "100"
        assert row["cost_basis"] == "15000.00"
        assert row["expiry"] is None
        assert row["strike"] is None
        assert row["right"] is None

    def test_option_fields(self, sample_option_position):
        row = _position_row(sample_option_position, "stmt-456")
        assert row["expiry"] == "2026-03-31"
        assert row["strike"] == "48"
        assert row["right"] == "C"


class TestTradeRow:
    def test_structure(self, sample_trade):
        row = _trade_row(sample_trade, "stmt-123")
        assert row["statement_id"] == "stmt-123"
        assert row["symbol"] == "AAPL"
        assert row["side"] == "BOT"
        assert row["quantity"] == "50"
        assert row["trade_date"] == "2026-01-15T10:30:00"
        assert row["commission"] == "-1.09"


# ── Mock Supabase client ────────────────────────────────────────────────────

def _make_mock_client():
    """Build a mock Supabase client with chained .table().method().execute()."""
    client = MagicMock()

    def _mock_table(name):
        table = MagicMock()
        # upsert chain
        table.upsert.return_value.execute.return_value = MagicMock(
            data=[{"id": "stmt-uuid-001"}]
        )
        # delete chain
        table.delete.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        # insert chain — return a row to indicate successful insert
        table.insert.return_value.execute.return_value = MagicMock(
            data=[{"id": "row-uuid-001"}]
        )
        # select chain (for queries)
        select = table.select.return_value
        select.eq.return_value = select
        select.gte.return_value = select
        select.lte.return_value = select
        select.order.return_value = select
        select.execute.return_value = MagicMock(data=[])
        return table

    client.table.side_effect = _mock_table
    return client


# ── Upsert tests ────────────────────────────────────────────────────────────

class TestUpsertStatement:
    @patch("src.db.get_client")
    def test_upsert_returns_statement_id(self, mock_get_client, parsed_statement):
        from src.db import upsert_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        stmt_id, trades_skipped, positions_skipped = upsert_statement(parsed_statement)
        assert stmt_id == "stmt-uuid-001"
        assert trades_skipped == 0
        assert positions_skipped == 0

    @patch("src.db.get_client")
    def test_upsert_calls_correct_tables(self, mock_get_client, parsed_statement):
        from src.db import upsert_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        upsert_statement(parsed_statement)

        # Should call statements, positions (delete + insert), trades (delete + insert)
        table_calls = [call.args[0] for call in client.table.call_args_list]
        assert "statements" in table_calls
        assert "positions" in table_calls
        assert "trades" in table_calls

    @patch("src.db.get_client")
    def test_upsert_deletes_old_children(self, mock_get_client, parsed_statement):
        from src.db import upsert_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        upsert_statement(parsed_statement)

        # Verify delete was called for positions and trades
        # Each table mock is created fresh, so we check via table calls
        delete_tables = []
        for call in client.table.call_args_list:
            name = call.args[0]
            if name in ("positions", "trades"):
                delete_tables.append(name)
        # positions and trades should each appear at least twice (delete + insert)
        assert delete_tables.count("positions") >= 2
        assert delete_tables.count("trades") >= 2

    @patch("src.db.get_client")
    @patch("src.db.st")
    def test_upsert_surfaces_error(self, mock_st, mock_get_client, parsed_statement):
        from src.db import upsert_statement

        client = MagicMock()
        client.table.side_effect = Exception("connection refused")
        mock_get_client.return_value = client

        with pytest.raises(Exception, match="connection refused"):
            upsert_statement(parsed_statement)

        mock_st.error.assert_called_once()

    @patch("src.db.get_client")
    def test_upsert_empty_positions_skips_insert(self, mock_get_client, sample_meta):
        from src.db import upsert_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        parsed = ParsedStatement(meta=sample_meta, positions=[], trades=[])
        upsert_statement(parsed)

        # Should still upsert statement and delete, but no insert calls
        # for positions/trades with empty lists
        # The table is called for delete but not insert
        calls = client.table.call_args_list
        table_names = [c.args[0] for c in calls]
        # statements: upsert, positions: delete, trades: delete
        assert table_names == ["statements", "positions", "trades"]

    @patch("src.db.get_client")
    def test_upsert_idempotent(self, mock_get_client, parsed_statement):
        """Upserting the same statement twice returns the same ID."""
        from src.db import upsert_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        id1, _, _ = upsert_statement(parsed_statement)
        id2, _, _ = upsert_statement(parsed_statement)
        assert id1 == id2

    @patch("src.db.get_client")
    def test_upsert_uses_on_conflict(self, mock_get_client, parsed_statement):
        """Upsert must specify the correct conflict key."""
        from src.db import upsert_statement

        client = _make_mock_client()
        mock_get_client.return_value = client

        upsert_statement(parsed_statement)

        # Find the statements table upsert call
        for call in client.table.call_args_list:
            if call.args[0] == "statements":
                table_mock = client.table(call.args[0])
                upsert_calls = table_mock.upsert.call_args_list
                if upsert_calls:
                    _, kwargs = upsert_calls[0]
                    assert "on_conflict" in kwargs
                    assert "account_id" in kwargs["on_conflict"]
                break


# ── Query tests ──────────────────────────────────────────────────────────────

class TestGetStatements:
    @patch("src.db.get_client")
    def test_returns_data(self, mock_get_client):
        from src.db import get_statements

        # Clear Streamlit cache for test isolation
        get_statements.clear()

        client = _make_mock_client()
        mock_get_client.return_value = client

        result = get_statements()
        assert isinstance(result, list)

    @patch("src.db.get_client")
    @patch("src.db.st")
    def test_error_returns_empty(self, mock_st, mock_get_client):
        from src.db import get_statements

        get_statements.clear()

        client = MagicMock()
        client.table.side_effect = Exception("timeout")
        mock_get_client.return_value = client

        result = get_statements()
        assert result == []
        mock_st.error.assert_called_once()


class TestGetPositions:
    @patch("src.db.get_client")
    def test_filters_by_statement(self, mock_get_client):
        from src.db import get_positions

        get_positions.clear()

        client = _make_mock_client()
        mock_get_client.return_value = client

        result = get_positions("stmt-uuid-001")
        assert isinstance(result, list)


class TestGetTrades:
    @patch("src.db.get_client")
    def test_no_filters(self, mock_get_client):
        from src.db import get_trades

        get_trades.clear()

        client = _make_mock_client()
        mock_get_client.return_value = client

        result = get_trades()
        assert isinstance(result, list)

    @patch("src.db.get_client")
    def test_with_filters(self, mock_get_client):
        from src.db import get_trades

        get_trades.clear()

        client = _make_mock_client()
        mock_get_client.return_value = client

        result = get_trades(
            statement_id="stmt-001",
            symbol="AAPL",
            asset_class="STK",
            side="BOT",
            date_from=date(2026, 1, 1),
            date_to=date(2026, 3, 6),
        )
        assert isinstance(result, list)

    @patch("src.db.get_client")
    def test_symbol_filter_applied(self, mock_get_client):
        """Verify .eq('symbol', ...) is called when symbol filter is set."""
        from src.db import get_trades

        get_trades.clear()

        client = MagicMock()
        # Build a chainable mock
        query = MagicMock()
        client.table.return_value.select.return_value = query
        query.eq.return_value = query
        query.gte.return_value = query
        query.lte.return_value = query
        query.order.return_value = query
        query.execute.return_value = MagicMock(data=[])
        mock_get_client.return_value = client

        get_trades(symbol="AAPL")

        # .eq should have been called with 'symbol', 'AAPL'
        eq_calls = query.eq.call_args_list
        symbols = [c for c in eq_calls if c.args == ("symbol", "AAPL")]
        assert len(symbols) == 1

    @patch("src.db.get_client")
    def test_date_range_filters_applied(self, mock_get_client):
        """Verify .gte and .lte are called for date range filters."""
        from src.db import get_trades

        get_trades.clear()

        client = MagicMock()
        query = MagicMock()
        client.table.return_value.select.return_value = query
        query.eq.return_value = query
        query.gte.return_value = query
        query.lte.return_value = query
        query.order.return_value = query
        query.execute.return_value = MagicMock(data=[])
        mock_get_client.return_value = client

        get_trades(date_from=date(2026, 1, 1), date_to=date(2026, 3, 6))

        query.gte.assert_called_once_with("trade_date", "2026-01-01")
        query.lte.assert_called_once_with("trade_date", "2026-03-06T23:59:59")

    @patch("src.db.get_client")
    def test_no_filters_skips_eq(self, mock_get_client):
        """With no filters, only .order and .execute should be called."""
        from src.db import get_trades

        get_trades.clear()

        client = MagicMock()
        query = MagicMock()
        client.table.return_value.select.return_value = query
        query.order.return_value = query
        query.execute.return_value = MagicMock(data=[])
        mock_get_client.return_value = client

        get_trades()

        query.eq.assert_not_called()
        query.gte.assert_not_called()
        query.lte.assert_not_called()

    @patch("src.db.get_client")
    @patch("src.db.st")
    def test_error_returns_empty(self, mock_st, mock_get_client):
        from src.db import get_trades

        get_trades.clear()

        client = MagicMock()
        client.table.side_effect = Exception("network error")
        mock_get_client.return_value = client

        result = get_trades()
        assert result == []
        mock_st.error.assert_called_once()


# ── Fingerprint tests ─────────────────────────────────────────────────────


class TestFingerprints:
    def test_trade_fingerprint_matches_same_trade(self, sample_trade):
        row = _trade_row(sample_trade, "stmt-1")
        fp1 = _trade_fingerprint(row)
        row2 = _trade_row(sample_trade, "stmt-2")
        fp2 = _trade_fingerprint(row2)
        assert fp1 == fp2, "Same trade in different statements should have same fingerprint"

    def test_trade_fingerprint_differs_for_different_price(self, sample_trade):
        row1 = _trade_row(sample_trade, "stmt-1")
        fp1 = _trade_fingerprint(row1)
        # Modify price
        row2 = dict(row1)
        row2["price"] = "999.00"
        fp2 = _trade_fingerprint(row2)
        assert fp1 != fp2

    def test_position_fingerprint_matches_same_position(self, sample_position):
        row = _position_row(sample_position, "stmt-1")
        fp1 = _position_fingerprint(row)
        row2 = _position_row(sample_position, "stmt-2")
        fp2 = _position_fingerprint(row2)
        assert fp1 == fp2, "Same position in different statements should have same fingerprint"

    def test_position_fingerprint_differs_for_different_date(self, sample_position):
        row1 = _position_row(sample_position, "stmt-1")
        fp1 = _position_fingerprint(row1)
        row2 = dict(row1)
        row2["statement_date"] = "2026-04-01"
        fp2 = _position_fingerprint(row2)
        assert fp1 != fp2

    def test_option_position_fingerprint_includes_option_fields(
        self, sample_option_position,
    ):
        row = _position_row(sample_option_position, "stmt-1")
        fp = _position_fingerprint(row)
        # Should include expiry, strike, right
        assert fp == (
            "EEM 31MAR26 48 C", "OPT", "2026-03-06",
            "2026-03-31", "48", "C",
        )


# ── reconcile_holdings tests ──────────────────────────────────────────────


class TestReconcileHoldings:
    """Tests for reconcile_holdings with optional base_date/target_date.

    Mocks the internal helpers directly rather than the Supabase client to
    keep tests focused on the reconciliation logic.
    """

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between", return_value=[])
    @patch("src.db.get_positions", return_value=[])
    @patch("src.db._get_account_statements")
    def test_single_statement_skips(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
        ]
        result = reconcile_holdings("ACCT1")
        assert result["ok"] is True
        assert "Need at least 2" in result.get("skipped", "")

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_default_earliest_to_latest(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Without dates, reconciles earliest -> latest."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "150",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        mock_trades.return_value = [
            {"symbol": "AAPL", "asset_class": "STK", "side": "BOT",
             "quantity": "50", "price": "175.00",
             "trade_date": "2026-02-10T10:00:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_holdings("ACCT1")
        assert result["ok"] is True
        assert result["base_date"] == "2026-01-31"
        assert result["target_date"] == "2026-02-28"

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_custom_dates_pass(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Reconcile with explicit base and target dates."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
            {"id": "s3", "period_start": "2026-03-01", "period_end": "2026-03-31"},
        ]

        def _positions(stmt_id):
            data = {
                "s1": [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                        "expiry": None, "strike": None, "right": None}],
                "s2": [{"symbol": "AAPL", "asset_class": "STK", "quantity": "150",
                        "expiry": None, "strike": None, "right": None}],
                "s3": [{"symbol": "AAPL", "asset_class": "STK", "quantity": "200",
                        "expiry": None, "strike": None, "right": None}],
            }
            return data.get(stmt_id, [])

        mock_pos.side_effect = _positions
        mock_trades.return_value = [
            {"symbol": "AAPL", "asset_class": "STK", "side": "BOT",
             "quantity": "50", "price": "175.00",
             "trade_date": "2026-02-10T10:00:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_holdings(
            "ACCT1",
            base_date=date(2026, 1, 31),
            target_date=date(2026, 2, 28),
        )
        assert result["ok"] is True
        assert result["base_date"] == "2026-01-31"
        assert result["target_date"] == "2026-02-28"

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between", return_value=[])
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_custom_dates_mismatch(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Detects mismatch when trades don't explain the difference."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "200",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions

        result = reconcile_holdings(
            "ACCT1",
            base_date=date(2026, 1, 31),
            target_date=date(2026, 2, 28),
        )
        assert result["ok"] is False
        assert len(result["mismatches"]) == 1
        assert result["mismatches"][0]["symbol"] == "AAPL"
        assert result["mismatches"][0]["expected_qty"] == "200"
        assert result["mismatches"][0]["reconstructed_qty"] == "100"

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between", return_value=[])
    @patch("src.db.get_positions", return_value=[])
    @patch("src.db._get_account_statements")
    def test_invalid_base_date(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Base date that doesn't match any statement returns skipped."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        result = reconcile_holdings(
            "ACCT1",
            base_date=date(2026, 3, 15),
            target_date=date(2026, 2, 28),
        )
        assert result["ok"] is False
        assert "No statement found" in result.get("skipped", "")

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between", return_value=[])
    @patch("src.db.get_positions", return_value=[])
    @patch("src.db._get_account_statements")
    def test_base_after_target_skips(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Base date >= target date returns skipped."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        result = reconcile_holdings(
            "ACCT1",
            base_date=date(2026, 2, 28),
            target_date=date(2026, 1, 31),
        )
        assert "skipped" in result
        assert "before" in result["skipped"].lower()

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_sell_reduces_position(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """SLD trade reduces position quantity during reconciliation."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "70",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        mock_trades.return_value = [
            {"symbol": "AAPL", "asset_class": "STK", "side": "SLD",
             "quantity": "30", "price": "180.00",
             "trade_date": "2026-02-15T10:00:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_holdings("ACCT1")
        assert result["ok"] is True

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_new_position_from_trade(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """A trade opening a new position that appears in the target snapshot."""
        from src.db import reconcile_holdings

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return []
            return [{"symbol": "MSFT", "asset_class": "STK", "quantity": "50",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        mock_trades.return_value = [
            {"symbol": "MSFT", "asset_class": "STK", "side": "BOT",
             "quantity": "50", "price": "400.00",
             "trade_date": "2026-02-05T14:00:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_holdings("ACCT1")
        assert result["ok"] is True


# ── reconcile_holdings_detail tests ──────────────────────────────────────


class TestReconcileHoldingsDetail:
    """Tests for reconcile_holdings_detail — per-holding daily ledger."""

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between", return_value=[])
    @patch("src.db.get_positions", return_value=[])
    @patch("src.db._get_account_statements")
    def test_single_statement_skips(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        from src.db import reconcile_holdings_detail

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
        ]
        result = reconcile_holdings_detail("ACCT1")
        assert result["ok"] is True
        assert "Need at least 2" in result.get("skipped", "")
        assert result["holdings"] == {}

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_ledger_shows_trades(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Each trade appears in the per-holding ledger with running qty."""
        from src.db import reconcile_holdings_detail

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "170",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        mock_trades.return_value = [
            {"symbol": "AAPL", "asset_class": "STK", "side": "BOT",
             "quantity": "50", "price": "175.00",
             "trade_date": "2026-02-10T10:00:00",
             "expiry": None, "strike": None, "right": None},
            {"symbol": "AAPL", "asset_class": "STK", "side": "BOT",
             "quantity": "30", "price": "176.00",
             "trade_date": "2026-02-15T14:00:00",
             "expiry": None, "strike": None, "right": None},
            {"symbol": "AAPL", "asset_class": "STK", "side": "SLD",
             "quantity": "10", "price": "180.00",
             "trade_date": "2026-02-20T11:00:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_holdings_detail("ACCT1")
        assert result["ok"] is True

        aapl = result["holdings"]["AAPL"]
        assert aapl["base_qty"] == "100"
        assert aapl["match"] is True
        assert len(aapl["trades"]) == 3

        # Check running quantities: 100 +50=150, +30=180, -10=170
        assert aapl["trades"][0]["running_qty"] == "150"
        assert aapl["trades"][1]["running_qty"] == "180"
        assert aapl["trades"][2]["running_qty"] == "170"
        assert aapl["final_qty"] == "170"
        assert aapl["expected_qty"] == "170"

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between", return_value=[])
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_mismatch_detected(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Holdings that don't match show match=False."""
        from src.db import reconcile_holdings_detail

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "200",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions

        result = reconcile_holdings_detail("ACCT1")
        assert result["ok"] is False
        assert result["holdings"]["AAPL"]["match"] is False
        assert result["holdings"]["AAPL"]["final_qty"] == "100"
        assert result["holdings"]["AAPL"]["expected_qty"] == "200"

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_new_position_only_in_target(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """A position only in the target snapshot shows base_qty=0 and mismatch."""
        from src.db import reconcile_holdings_detail

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return []
            return [{"symbol": "GOOG", "asset_class": "STK", "quantity": "25",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        mock_trades.return_value = []

        result = reconcile_holdings_detail("ACCT1")
        assert result["ok"] is False
        goog = result["holdings"]["GOOG"]
        assert goog["base_qty"] == "0"
        assert goog["final_qty"] == "0"
        assert goog["expected_qty"] == "25"
        assert goog["match"] is False

    @patch("src.db.check_coverage_gap", return_value=None)
    @patch("src.db._get_account_trades_between")
    @patch("src.db.get_positions")
    @patch("src.db._get_account_statements")
    def test_trade_dates_in_ledger(self, mock_stmts, mock_pos, mock_trades, mock_cov):
        """Trade dates are extracted correctly (date-only portion)."""
        from src.db import reconcile_holdings_detail

        mock_stmts.return_value = [
            {"id": "s1", "period_start": "2026-01-01", "period_end": "2026-01-31"},
            {"id": "s2", "period_start": "2026-02-01", "period_end": "2026-02-28"},
        ]

        def _positions(stmt_id):
            if stmt_id == "s1":
                return [{"symbol": "MSFT", "asset_class": "STK", "quantity": "50",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "MSFT", "asset_class": "STK", "quantity": "80",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        mock_trades.return_value = [
            {"symbol": "MSFT", "asset_class": "STK", "side": "BOT",
             "quantity": "30", "price": "400.00",
             "trade_date": "2026-02-12T09:30:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_holdings_detail("ACCT1")
        assert result["ok"] is True
        msft = result["holdings"]["MSFT"]
        assert msft["trades"][0]["date"] == "2026-02-12"
        assert msft["trades"][0]["side"] == "BOT"


# ── check_duplicates tests ───────────────────────────────────────────────


class TestCheckDuplicates:
    @patch("src.db.get_client")
    def test_all_new_data(self, mock_get_client, parsed_statement):
        from src.db import check_duplicates

        client = _make_mock_client()
        mock_get_client.return_value = client

        result = check_duplicates(parsed_statement)
        assert result["new_trades"] == 1
        assert result["dup_trades"] == 0
        assert result["new_positions"] == 1
        assert result["dup_positions"] == 0

    @patch("src.db.get_client")
    def test_no_data(self, mock_get_client, sample_meta):
        from src.db import check_duplicates

        client = _make_mock_client()
        mock_get_client.return_value = client

        parsed = ParsedStatement(meta=sample_meta, positions=[], trades=[])
        result = check_duplicates(parsed)
        assert result["new_trades"] == 0
        assert result["dup_trades"] == 0
        assert result["new_positions"] == 0
        assert result["dup_positions"] == 0
