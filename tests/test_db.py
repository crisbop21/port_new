"""Tests for the Supabase database layer.

All tests mock the Supabase client — no real database needed.
"""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.db import (
    _metric_fingerprint,
    _metric_row,
    _position_fingerprint,
    _position_row,
    _ser,
    _trade_fingerprint,
    _trade_row,
)
from src.models import ParsedStatement, Position, StatementMeta, StockMetric, Trade


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


# ── reconcile_pair tests ──────────────────────────────────────────────────


class TestReconcilePair:
    """Tests for reconcile_pair — statement_date-driven reconciliation."""

    @patch("src.db.get_trades_between", return_value=[])
    @patch("src.db.get_positions_as_of")
    def test_exact_match(self, mock_pos, mock_trades):
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions
        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is True
        assert result["base_date"] == "2026-01-31"
        assert result["target_date"] == "2026-02-28"

    @patch("src.db.get_trades_between")
    @patch("src.db.get_positions_as_of")
    def test_buy_adds_quantity(self, mock_pos, mock_trades):
        """BOT trade increases position quantity."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
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

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is True
        aapl = result["holdings"]["AAPL"]
        assert aapl["base_qty"] == "100"
        assert aapl["reconstructed_qty"] == "150"
        assert aapl["expected_qty"] == "150"
        assert aapl["match"] is True

    @patch("src.db.get_trades_between")
    @patch("src.db.get_positions_as_of")
    def test_sell_reduces_quantity(self, mock_pos, mock_trades):
        """SLD trade reduces position quantity."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
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

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is True

    @patch("src.db.get_trades_between", return_value=[])
    @patch("src.db.get_positions_as_of")
    def test_mismatch_detected(self, mock_pos, mock_trades):
        """Detects mismatch when trades don't explain the difference."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
                return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "100",
                         "expiry": None, "strike": None, "right": None}]
            return [{"symbol": "AAPL", "asset_class": "STK", "quantity": "200",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is False
        aapl = result["holdings"]["AAPL"]
        assert aapl["reconstructed_qty"] == "100"
        assert aapl["expected_qty"] == "200"
        assert aapl["diff"] == "-100"
        assert aapl["match"] is False

    @patch("src.db.get_trades_between")
    @patch("src.db.get_positions_as_of")
    def test_new_position_from_trade(self, mock_pos, mock_trades):
        """A trade opening a new position that appears in the target snapshot."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
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

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is True

    @patch("src.db.get_trades_between", return_value=[])
    @patch("src.db.get_positions_as_of")
    def test_position_only_in_target_gap(self, mock_pos, mock_trades):
        """A position only in target with no trades is flagged as a gap."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
                return []
            return [{"symbol": "GOOG", "asset_class": "STK", "quantity": "25",
                     "expiry": None, "strike": None, "right": None}]

        mock_pos.side_effect = _positions

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is False
        goog = result["holdings"]["GOOG"]
        assert goog["base_qty"] == "0"
        assert goog["reconstructed_qty"] == "0"
        assert goog["expected_qty"] == "25"
        assert goog["match"] is False
        assert len(result["gaps"]["missing_from_reconstruction"]) == 1

    @patch("src.db.get_trades_between")
    @patch("src.db.get_positions_as_of")
    def test_ledger_shows_trades_with_running_qty(self, mock_pos, mock_trades):
        """Each trade appears in the per-holding ledger with running qty."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
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

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is True

        aapl = result["holdings"]["AAPL"]
        assert aapl["base_qty"] == "100"
        assert aapl["match"] is True
        assert len(aapl["trades"]) == 3

        # Check running quantities: 100 +50=150, +30=180, -10=170
        assert aapl["trades"][0]["running_qty"] == "150"
        assert aapl["trades"][1]["running_qty"] == "180"
        assert aapl["trades"][2]["running_qty"] == "170"
        assert aapl["reconstructed_qty"] == "170"
        assert aapl["expected_qty"] == "170"

    @patch("src.db.get_trades_between")
    @patch("src.db.get_positions_as_of")
    def test_trade_dates_in_ledger(self, mock_pos, mock_trades):
        """Trade dates are extracted correctly (date-only portion)."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
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

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is True
        msft = result["holdings"]["MSFT"]
        assert msft["trades"][0]["date"] == "2026-02-12"
        assert msft["trades"][0]["side"] == "BOT"

    @patch("src.db.get_trades_between")
    @patch("src.db.get_positions_as_of")
    def test_position_in_base_not_in_target_gap(self, mock_pos, mock_trades):
        """A position fully sold but still showing in reconstruction is flagged."""
        from src.db import reconcile_pair

        def _positions(account_id, as_of_date):
            if as_of_date == date(2026, 1, 31):
                return [{"symbol": "TSLA", "asset_class": "STK", "quantity": "50",
                         "expiry": None, "strike": None, "right": None}]
            return []  # TSLA gone from target

        mock_pos.side_effect = _positions
        # Only sold 30 of 50, so 20 remain in reconstruction but not in target
        mock_trades.return_value = [
            {"symbol": "TSLA", "asset_class": "STK", "side": "SLD",
             "quantity": "30", "price": "200.00",
             "trade_date": "2026-02-10T10:00:00",
             "expiry": None, "strike": None, "right": None},
        ]

        result = reconcile_pair("ACCT1", date(2026, 1, 31), date(2026, 2, 28))
        assert result["ok"] is False
        assert len(result["gaps"]["missing_from_target"]) == 1
        assert result["gaps"]["missing_from_target"][0]["symbol"] == "TSLA"


# ── reconcile_account tests ──────────────────────────────────────────────


class TestReconcileAccount:
    """Tests for reconcile_account — runs all consecutive pairs."""

    @patch("src.db.get_snapshot_dates")
    def test_single_snapshot_returns_empty(self, mock_dates):
        from src.db import reconcile_account

        mock_dates.return_value = [date(2026, 1, 31)]
        result = reconcile_account("ACCT1")
        assert result == []

    @patch("src.db.reconcile_pair")
    @patch("src.db.get_snapshot_dates")
    def test_two_snapshots_one_pair(self, mock_dates, mock_pair):
        from src.db import reconcile_account

        mock_dates.return_value = [date(2026, 1, 31), date(2026, 2, 28)]
        mock_pair.return_value = {"ok": True, "base_date": "2026-01-31", "target_date": "2026-02-28"}

        result = reconcile_account("ACCT1")
        assert len(result) == 1
        mock_pair.assert_called_once_with("ACCT1", date(2026, 1, 31), date(2026, 2, 28))

    @patch("src.db.reconcile_pair")
    @patch("src.db.get_snapshot_dates")
    def test_three_snapshots_two_pairs(self, mock_dates, mock_pair):
        from src.db import reconcile_account

        mock_dates.return_value = [
            date(2026, 1, 31), date(2026, 2, 28), date(2026, 3, 31),
        ]
        mock_pair.return_value = {"ok": True}

        result = reconcile_account("ACCT1")
        assert len(result) == 2
        assert mock_pair.call_count == 2


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


# ── Position dedup bug tests ─────────────────────────────────────────────────


class TestUpsertPositionsNotDedupedAcrossStatements:
    """Positions must always be stored per-statement, never skipped by
    cross-statement fingerprint dedup.

    Bug scenario:
    1. Statement A stores [AAPL, MSFT, GOOG] with statement_date=Mar 6
    2. Statement B (different period_start, same period_end) has [AAPL, MSFT, TSLA]
       - Old code: AAPL & MSFT fingerprints match Statement A → SKIPPED
       - Only TSLA is stored for Statement B
    3. Re-upload Statement A with corrected PDF [MSFT, GOOG]:
       - Deletes Statement A positions → AAPL gone from A
       - AAPL was never stored in B → AAPL LOST FOREVER
    """

    def _make_tracking_client(self, existing_stmt_ids=None):
        """Build a mock client that tracks inserted rows per table."""
        client = MagicMock()
        inserted = {"positions": [], "trades": []}

        def _mock_table(name):
            table = MagicMock()
            # upsert chain (for statements)
            table.upsert.return_value.execute.return_value = MagicMock(
                data=[{"id": "stmt-uuid-001"}]
            )
            # delete chain
            table.delete.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
            # insert chain — capture what's inserted
            def _capture_insert(rows):
                inserted[name].extend(rows)
                result_mock = MagicMock()
                result_mock.execute.return_value = MagicMock(data=[{"id": f"row-{i}"} for i in range(len(rows))])
                return result_mock
            table.insert.side_effect = _capture_insert
            # select chain (for fingerprint queries — return no existing data)
            select = table.select.return_value
            select.eq.return_value = select
            select.execute.return_value = MagicMock(data=existing_stmt_ids or [])
            return table

        client.table.side_effect = _mock_table
        return client, inserted

    @patch("src.db.get_client")
    def test_all_positions_inserted_even_with_matching_fingerprints(
        self, mock_get_client, sample_meta,
    ):
        """Every position from the PDF must be stored, regardless of whether
        another statement already has a position with the same fingerprint."""
        from src.db import upsert_statement

        client, inserted = self._make_tracking_client()
        mock_get_client.return_value = client

        # Two positions: AAPL and MSFT
        positions = [
            Position(
                symbol="AAPL", asset_class="STK", quantity=Decimal("100"),
                cost_basis=Decimal("15000"), market_price=Decimal("175"),
                market_value=Decimal("17500"), unrealized_pnl=Decimal("2500"),
                currency="USD", statement_date=date(2026, 3, 6),
            ),
            Position(
                symbol="MSFT", asset_class="STK", quantity=Decimal("50"),
                cost_basis=Decimal("20000"), market_price=Decimal("420"),
                market_value=Decimal("21000"), unrealized_pnl=Decimal("1000"),
                currency="USD", statement_date=date(2026, 3, 6),
            ),
        ]
        parsed = ParsedStatement(meta=sample_meta, positions=positions, trades=[])

        stmt_id, trades_skipped, positions_skipped = upsert_statement(parsed)

        # ALL positions must be inserted — none should be skipped
        assert positions_skipped == 0, (
            f"Expected 0 positions skipped, got {positions_skipped}. "
            "Cross-statement dedup must not skip positions."
        )
        assert len(inserted["positions"]) == 2, (
            f"Expected 2 positions inserted, got {len(inserted['positions'])}."
        )
        symbols_inserted = {r["symbol"] for r in inserted["positions"]}
        assert symbols_inserted == {"AAPL", "MSFT"}

    @patch("src.db._get_existing_position_fingerprints")
    @patch("src.db.get_client")
    def test_positions_not_skipped_when_fingerprint_exists_elsewhere(
        self, mock_get_client, mock_get_fps, sample_meta,
    ):
        """Even if _get_existing_position_fingerprints returns matching
        fingerprints from other statements, positions must still be inserted."""
        from src.db import upsert_statement

        client, inserted = self._make_tracking_client()
        mock_get_client.return_value = client

        # Simulate: another statement already has AAPL with same fingerprint
        mock_get_fps.return_value = {
            ("AAPL", "STK", "2026-03-06", None, None, None),
        }

        positions = [
            Position(
                symbol="AAPL", asset_class="STK", quantity=Decimal("100"),
                cost_basis=Decimal("15000"), market_price=Decimal("175"),
                market_value=Decimal("17500"), unrealized_pnl=Decimal("2500"),
                currency="USD", statement_date=date(2026, 3, 6),
            ),
            Position(
                symbol="GOOG", asset_class="STK", quantity=Decimal("25"),
                cost_basis=Decimal("5000"), market_price=Decimal("180"),
                market_value=Decimal("4500"), unrealized_pnl=Decimal("-500"),
                currency="USD", statement_date=date(2026, 3, 6),
            ),
        ]
        parsed = ParsedStatement(meta=sample_meta, positions=positions, trades=[])

        stmt_id, trades_skipped, positions_skipped = upsert_statement(parsed)

        # AAPL must NOT be skipped even though it exists in another statement
        assert positions_skipped == 0
        assert len(inserted["positions"]) == 2
        symbols_inserted = {r["symbol"] for r in inserted["positions"]}
        assert "AAPL" in symbols_inserted, (
            "AAPL was skipped due to cross-statement dedup — this causes holdings loss"
        )

    @patch("src.db.get_client")
    def test_upsert_return_value_positions_skipped_always_zero(
        self, mock_get_client, sample_meta,
    ):
        """positions_skipped in return value should always be 0 since we don't
        deduplicate positions across statements."""
        from src.db import upsert_statement

        client, _ = self._make_tracking_client()
        mock_get_client.return_value = client

        positions = [
            Position(
                symbol="AAPL", asset_class="STK", quantity=Decimal("100"),
                cost_basis=Decimal("15000"), market_price=Decimal("175"),
                market_value=Decimal("17500"), unrealized_pnl=Decimal("2500"),
                currency="USD", statement_date=date(2026, 3, 6),
            ),
        ]
        parsed = ParsedStatement(meta=sample_meta, positions=positions, trades=[])

        _, _, positions_skipped = upsert_statement(parsed)
        assert positions_skipped == 0


# ── Stock metric helpers ─────────────────────────────────────────────────────


@pytest.fixture
def sample_metric():
    return StockMetric(
        symbol="AAPL",
        metric_name="revenue",
        metric_value=Decimal("394328000000"),
        period_end=date(2024, 9, 28),
        source="SEC_EDGAR",
        cik="0000320193",
        filing_type="10-K",
    )


class TestMetricRow:
    def test_structure(self, sample_metric):
        row = _metric_row(sample_metric)
        assert row["symbol"] == "AAPL"
        assert row["metric_name"] == "revenue"
        assert row["metric_value"] == "394328000000"
        assert row["period_end"] == "2024-09-28"
        assert row["source"] == "SEC_EDGAR"
        assert row["cik"] == "0000320193"
        assert row["filing_type"] == "10-K"

    def test_decimal_precision(self):
        metric = StockMetric(
            symbol="MSFT",
            metric_name="eps_diluted",
            metric_value=Decimal("11.86"),
            period_end=date(2024, 6, 30),
            source="SEC_EDGAR",
            cik="0000789019",
            filing_type="10-K",
        )
        row = _metric_row(metric)
        assert row["metric_value"] == "11.86"


class TestMetricFingerprint:
    def test_same_metric_same_fingerprint(self, sample_metric):
        row = _metric_row(sample_metric)
        fp1 = _metric_fingerprint(row)
        fp2 = _metric_fingerprint(row)
        assert fp1 == fp2

    def test_different_period_different_fingerprint(self, sample_metric):
        row1 = _metric_row(sample_metric)
        metric2 = sample_metric.model_copy(update={"period_end": date(2023, 9, 30)})
        row2 = _metric_row(metric2)
        assert _metric_fingerprint(row1) != _metric_fingerprint(row2)

    def test_different_metric_name_different_fingerprint(self, sample_metric):
        row1 = _metric_row(sample_metric)
        metric2 = sample_metric.model_copy(update={"metric_name": "net_income"})
        row2 = _metric_row(metric2)
        assert _metric_fingerprint(row1) != _metric_fingerprint(row2)


class TestUpsertStockMetrics:
    @patch("src.db.get_client")
    def test_inserts_new_metrics(self, mock_get_client, sample_metric):
        from src.db import upsert_stock_metrics

        client = _make_mock_client()
        mock_get_client.return_value = client

        inserted, updated, errors = upsert_stock_metrics([sample_metric])
        assert inserted == 1
        assert updated == 0
        assert errors == []

    @patch("src.db.get_client")
    def test_empty_list_returns_zeros(self, mock_get_client):
        from src.db import upsert_stock_metrics

        inserted, updated, errors = upsert_stock_metrics([])
        assert inserted == 0
        assert updated == 0
        assert errors == []

    @patch("src.db.get_client")
    def test_detects_existing_as_update(self, mock_get_client, sample_metric):
        from src.db import upsert_stock_metrics

        client = MagicMock()
        existing_row = {"symbol": "AAPL", "metric_name": "revenue", "period_end": "2024-09-28"}

        def _mock_table(name):
            table = MagicMock()
            select = table.select.return_value
            select.eq.return_value = select
            # Return the existing metric so the fingerprint check finds it
            select.execute.return_value = MagicMock(data=[existing_row])
            # upsert chain
            table.upsert.return_value.execute.return_value = MagicMock(
                data=[{"id": "row-uuid-001"}]
            )
            return table

        client.table.side_effect = _mock_table
        mock_get_client.return_value = client

        inserted, updated, errors = upsert_stock_metrics([sample_metric])
        assert updated == 1
        assert inserted == 0
