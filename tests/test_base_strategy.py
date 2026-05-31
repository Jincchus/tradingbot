import pytest
from collections import deque
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.database import Base
from db.models import Strategy, Trade
from datetime import datetime
from decimal import Decimal

@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e

@pytest.fixture
def concrete_strategy(db_engine):
    from strategies.base import BaseStrategy

    class TestStrategy(BaseStrategy):
        def select_symbols(self):
            return ["AAPL"]
        def on_bar(self, bar):
            pass

    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = TestStrategy(
            strategy_id=1, name="test",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()  # trading_client/trade_stream/engine를 run() 대신 여기서 mock으로 생성
    return s

def test_process_bar_appends_and_calls_on_bar(concrete_strategy):
    concrete_strategy._bar_buffer["AAPL"] = deque([100.0], maxlen=200)
    seen = {}
    concrete_strategy.on_bar = lambda bar: seen.setdefault("bar", bar)

    bar = MagicMock(); bar.symbol = "AAPL"; bar.close = 101.0
    concrete_strategy._process_bar(bar)

    assert list(concrete_strategy._bar_buffer["AAPL"]) == [100.0, 101.0]
    assert seen["bar"] is bar

def test_sync_state_loads_positions(concrete_strategy):
    mock_position = MagicMock()
    mock_position.symbol = "AAPL"
    concrete_strategy.trading_client.get_all_positions.return_value = [mock_position]
    concrete_strategy.trading_client.get_orders.return_value = []

    concrete_strategy.sync_state()

    assert "AAPL" in concrete_strategy._positions

def test_on_order_filled_updates_position_cache(concrete_strategy, db_engine):
    with Session(db_engine) as session:
        session.add(Strategy(
            id=1, name="t", strategy_type="test", alpaca_key="k", alpaca_secret="s",
            budget=Decimal("10000"), status="running", run_interval="1m",
        ))
        session.commit()

    buy = MagicMock()
    buy.symbol = "AAPL"; buy.side.value = "buy"; buy.filled_qty = "10"
    buy.filled_avg_price = "150"; buy.id = "o1"; buy.filled_at = datetime.utcnow()
    concrete_strategy.on_order_filled(buy)
    assert "AAPL" in concrete_strategy._positions

    sell = MagicMock()
    sell.symbol = "AAPL"; sell.side.value = "sell"; sell.filled_qty = "10"
    sell.filled_avg_price = "160"; sell.id = "o2"; sell.filled_at = datetime.utcnow()
    concrete_strategy.on_order_filled(sell)
    assert "AAPL" not in concrete_strategy._positions

def test_prefetch_bars_fills_buffer(concrete_strategy):
    mock_df = MagicMock()
    mock_df.__getitem__.return_value.tolist.return_value = [100.0] * 50

    with patch("strategies.base.StockHistoricalDataClient") as mock_hist:
        mock_hist.return_value.get_stock_bars.return_value.df = mock_df
        concrete_strategy._prefetch_bars(["AAPL"])

    assert "AAPL" in concrete_strategy._bar_buffer
    assert len(concrete_strategy._bar_buffer["AAPL"]) == 50

def test_on_order_filled_saves_trade(concrete_strategy, db_engine):
    with Session(db_engine) as session:
        session.add(Strategy(
            id=1, name="test", strategy_type="test", alpaca_key="k", alpaca_secret="s",
            budget=Decimal("10000"), status="running", run_interval="1m",
        ))
        session.commit()

    mock_order = MagicMock()
    mock_order.symbol = "AAPL"
    mock_order.side.value = "buy"
    mock_order.filled_qty = "10"
    mock_order.filled_avg_price = "150.00"
    mock_order.id = "order-abc"
    mock_order.filled_at = datetime.utcnow()

    concrete_strategy.on_order_filled(mock_order)

    with Session(db_engine) as session:
        trade = session.query(Trade).first()
        assert trade is not None
        assert trade.symbol == "AAPL"
        assert trade.alpaca_order_id == "order-abc"

def test_get_metrics_returns_account_info(concrete_strategy):
    mock_account = MagicMock()
    mock_account.equity = "10500.00"
    mock_account.cash = "5000.00"
    mock_account.buying_power = "10000.00"
    concrete_strategy.trading_client.get_account.return_value = mock_account

    metrics = concrete_strategy.get_metrics()

    assert metrics["equity"] == 10500.00
    assert metrics["cash"] == 5000.00
