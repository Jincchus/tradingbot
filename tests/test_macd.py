import pytest
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
    from strategies.macd import MacdStrategy
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = MacdStrategy(
            strategy_id=1, name="macd",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()
    return s


def _make_buffer(trend: str) -> deque:
    # SLOW=26, SIGNAL=9 → 최소 35봉. 길게 횡보 후 마지막 봉에서 크로스 유도
    closes = [100.0] * 40
    if trend == "bull_cross":
        closes.append(130.0)   # 급등 → MACD가 Signal 상향 돌파
    elif trend == "bear_cross":
        closes.append(70.0)    # 급락 → MACD가 Signal 하향 돌파
    # flat: 그대로 횡보 → 크로스 없음
    return deque(closes, maxlen=200)


def test_select_symbols_returns_list(strategy):
    symbols = strategy.select_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0


def test_on_bar_buys_on_bull_cross(strategy):
    strategy._positions = {}
    strategy._bar_buffer["AAPL"] = _make_buffer("bull_cross")

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 130.0
    strategy.on_bar(bar)

    strategy.trading_client.submit_order.assert_called_once()
    order_req = strategy.trading_client.submit_order.call_args[0][0]
    assert order_req.side.value == "buy"


def test_on_bar_sells_on_bear_cross(strategy):
    pos = MagicMock()
    pos.symbol = "AAPL"
    strategy._positions = {"AAPL": pos}
    strategy._bar_buffer["AAPL"] = _make_buffer("bear_cross")

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 70.0
    strategy.on_bar(bar)

    strategy.trading_client.close_position.assert_called_once_with("AAPL")


def test_on_bar_no_signal_no_order(strategy):
    strategy._positions = {}
    strategy._bar_buffer["AAPL"] = _make_buffer("flat")

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 100.0
    strategy.on_bar(bar)

    strategy.trading_client.submit_order.assert_not_called()
    strategy.trading_client.close_position.assert_not_called()


def test_on_bar_skips_when_buffer_too_small(strategy):
    strategy._positions = {}
    strategy._bar_buffer["AAPL"] = deque([100.0] * 10, maxlen=200)

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 100.0
    strategy.on_bar(bar)

    strategy.trading_client.submit_order.assert_not_called()
