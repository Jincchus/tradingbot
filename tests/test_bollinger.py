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
    from strategies.bollinger import BollingerStrategy
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = BollingerStrategy(
            strategy_id=1, name="bollinger",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()
    return s


def test_select_symbols_returns_list(strategy):
    symbols = strategy.select_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0


def test_on_bar_buys_on_lower_band_touch(strategy):
    # 19봉 100 + 마지막 봉 급락 → 하단 밴드 이탈
    strategy._positions = {}
    strategy._bar_buffer["AAPL"] = deque([100.0] * 19 + [85.0], maxlen=200)

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 85.0
    strategy.on_bar(bar)

    strategy.trading_client.submit_order.assert_called_once()
    order_req = strategy.trading_client.submit_order.call_args[0][0]
    assert order_req.side.value == "buy"


def test_on_bar_sells_on_return_to_center(strategy):
    # 중심선(SMA20) 이상으로 회귀 → 청산
    pos = MagicMock()
    pos.symbol = "AAPL"
    strategy._positions = {"AAPL": pos}
    strategy._bar_buffer["AAPL"] = deque([100.0] * 19 + [105.0], maxlen=200)

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 105.0
    strategy.on_bar(bar)

    strategy.trading_client.close_position.assert_called_once_with("AAPL")


def test_on_bar_no_signal_no_order(strategy):
    # 밴드 내부(중심선 아래, 하단 위)에서 미보유 → 주문 없음
    strategy._positions = {}
    strategy._bar_buffer["AAPL"] = deque([98.0, 102.0] * 10, maxlen=200)

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 99.0
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
