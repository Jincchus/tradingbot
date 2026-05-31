import pytest
import pandas as pd
from collections import deque
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from db.database import Base

@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e

@pytest.fixture
def strategy(db_engine):
    from strategies.ma_crossover import MACrossoverStrategy
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = MACrossoverStrategy(
            strategy_id=1, name="ma_crossover",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()  # trading_client 등을 mock으로 생성
    return s

def _make_buffer(trend: str) -> deque:
    # SHORT_WINDOW=10, LONG_WINDOW=30 기준
    # 30개 100.0 + 마지막 1개로 크로스 생성
    closes = [100.0] * 30
    if trend == "golden_cross":
        closes.append(200.0)   # short_ma 급등 → golden cross
    elif trend == "death_cross":
        closes.append(0.0)     # short_ma 급락 → death cross
    else:
        closes.append(100.0)   # 변화 없음 → no cross
    return deque(closes, maxlen=200)

def test_select_symbols_returns_list(strategy):
    symbols = strategy.select_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0

def test_on_bar_buys_on_golden_cross(strategy):
    strategy._positions = {}
    strategy.trading_client.get_all_positions.return_value = []
    strategy._bar_buffer["AAPL"] = _make_buffer("golden_cross")

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 200.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.submit_order.assert_called_once()
    order_req = strategy.trading_client.submit_order.call_args[0][0]
    assert order_req.side.value == "buy"

def test_on_bar_sells_on_death_cross(strategy):
    mock_pos = MagicMock()
    mock_pos.symbol = "AAPL"
    strategy._positions = {"AAPL": mock_pos}
    strategy.trading_client.get_all_positions.return_value = [mock_pos]
    strategy._bar_buffer["AAPL"] = _make_buffer("death_cross")

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 0.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.close_position.assert_called_once_with("AAPL")

def test_on_bar_no_signal_no_order(strategy):
    strategy._positions = {}
    strategy.trading_client.get_all_positions.return_value = []
    strategy._bar_buffer["AAPL"] = _make_buffer("flat")

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 100.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.submit_order.assert_not_called()
    strategy.trading_client.close_position.assert_not_called()

def test_on_bar_skips_when_buffer_too_small(strategy):
    strategy._bar_buffer["AAPL"] = deque([100.0] * 10, maxlen=200)
    strategy.trading_client.get_all_positions.return_value = []

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 100.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.submit_order.assert_not_called()
