from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.models import ParsedStatement, Position, StatementMeta, Trade


class TestStatementMeta:
    def test_valid(self):
        meta = StatementMeta(
            account_id="U1234567",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 31),
            base_currency="USD",
        )
        assert meta.account_id == "U1234567"


class TestPosition:
    def _stock(self, **overrides):
        defaults = dict(
            symbol="AAPL",
            asset_class="STK",
            quantity=Decimal("100"),
            cost_basis=Decimal("15000.00"),
            market_price=Decimal("175.50"),
            market_value=Decimal("17550.00"),
            unrealized_pnl=Decimal("2550.00"),
            currency="USD",
            statement_date=date(2024, 1, 31),
        )
        defaults.update(overrides)
        return Position(**defaults)

    def test_valid_stock(self):
        pos = self._stock()
        assert pos.quantity == Decimal("100")
        assert pos.expiry is None

    def test_valid_etf(self):
        pos = self._stock(symbol="SPY", asset_class="ETF")
        assert pos.asset_class == "ETF"

    def test_valid_option(self):
        pos = self._stock(
            symbol="AAPL 20240119 150.0 C",
            asset_class="OPT",
            expiry=date(2024, 1, 19),
            strike=Decimal("150.0"),
            right="C",
        )
        assert pos.right == "C"
        assert pos.strike == Decimal("150.0")

    def test_unsupported_asset_class_rejected(self):
        with pytest.raises(ValidationError, match="Unsupported asset class"):
            self._stock(asset_class="FUT")

    def test_option_missing_fields_rejected(self):
        with pytest.raises(ValidationError, match="missing expiry"):
            self._stock(asset_class="OPT")

    def test_decimal_precision_preserved(self):
        pos = self._stock(market_price=Decimal("175.123456"))
        assert pos.market_price == Decimal("175.123456")

    def test_decimal_zero_preserved(self):
        """Decimal('0.00') must not become 0 or None."""
        pos = self._stock(unrealized_pnl=Decimal("0.00"))
        assert pos.unrealized_pnl == Decimal("0.00")
        assert isinstance(pos.unrealized_pnl, Decimal)

    def test_decimal_negative_preserved(self):
        pos = self._stock(unrealized_pnl=Decimal("-458.08"))
        assert pos.unrealized_pnl == Decimal("-458.08")

    def test_decimal_large_value(self):
        pos = self._stock(market_value=Decimal("1234567.89"))
        assert pos.market_value == Decimal("1234567.89")

    def test_decimal_many_places(self):
        """Financial values must not be silently rounded."""
        pos = self._stock(market_price=Decimal("9.6783"))
        assert pos.market_price == Decimal("9.6783")

    def test_option_missing_strike_rejected(self):
        with pytest.raises(ValidationError):
            self._stock(asset_class="OPT", expiry=date(2024, 1, 19), right="C")

    def test_option_missing_right_rejected(self):
        with pytest.raises(ValidationError):
            self._stock(
                asset_class="OPT",
                expiry=date(2024, 1, 19),
                strike=Decimal("150"),
            )

    def test_invalid_right_rejected(self):
        with pytest.raises(ValidationError):
            self._stock(
                asset_class="OPT",
                expiry=date(2024, 1, 19),
                strike=Decimal("150"),
                right="X",
            )


class TestTrade:
    def _trade(self, **overrides):
        defaults = dict(
            trade_date=datetime(2024, 1, 15, 10, 30, 0),
            symbol="AAPL",
            asset_class="STK",
            side="BOT",
            quantity=Decimal("50"),
            price=Decimal("175.00"),
            proceeds=Decimal("-8750.00"),
            commission=Decimal("-1.00"),
            realized_pnl=Decimal("0"),
            currency="USD",
        )
        defaults.update(overrides)
        return Trade(**defaults)

    def test_valid_buy(self):
        t = self._trade()
        assert t.side == "BOT"

    def test_valid_sell(self):
        t = self._trade(side="SLD", proceeds=Decimal("9000.00"), realized_pnl=Decimal("250.00"))
        assert t.realized_pnl == Decimal("250.00")

    def test_invalid_side_rejected(self):
        with pytest.raises(ValidationError):
            self._trade(side="HOLD")

    def test_unsupported_asset_class_rejected(self):
        with pytest.raises(ValidationError, match="Unsupported asset class"):
            self._trade(asset_class="BOND")

    def test_option_trade(self):
        t = self._trade(
            symbol="AAPL 20240119 150.0 P",
            asset_class="OPT",
            expiry=date(2024, 1, 19),
            strike=Decimal("150.0"),
            right="P",
        )
        assert t.right == "P"

    def test_commission_precision(self):
        """Commission must preserve exact cents."""
        t = self._trade(commission=Decimal("-1.09"))
        assert t.commission == Decimal("-1.09")

    def test_proceeds_negative_for_buy(self):
        t = self._trade(proceeds=Decimal("-8750.00"))
        assert t.proceeds == Decimal("-8750.00")

    def test_realized_pnl_zero(self):
        t = self._trade(realized_pnl=Decimal("0"))
        assert t.realized_pnl == Decimal("0")
        assert isinstance(t.realized_pnl, Decimal)


class TestParsedStatement:
    def test_empty_statement(self):
        ps = ParsedStatement(
            meta=StatementMeta(
                account_id="U1234567",
                period_start=date(2024, 1, 1),
                period_end=date(2024, 1, 31),
                base_currency="USD",
            ),
        )
        assert ps.positions == []
        assert ps.trades == []
        assert ps.skipped_rows == []
