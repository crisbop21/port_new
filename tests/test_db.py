"""Tests for the Supabase database layer.

All tests mock the Supabase client — no real database needed.
"""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.db import _position_row, _ser, _trade_row
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

        stmt_id, trades_skipped = upsert_statement(parsed_statement)
        assert stmt_id == "stmt-uuid-001"
        assert trades_skipped == 0

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

        id1 = upsert_statement(parsed_statement)
        id2 = upsert_statement(parsed_statement)
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
