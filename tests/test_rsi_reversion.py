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
    from strategies.rsi_reversion import RsiReversionStrategy
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = RsiReversionStrategy(
            strategy_id=1, name="rsi_reversion",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()
    return s


def _make_buffer(trend: str) -> deque:
    # RSI_PERIOD=14 → 종가 15개 필요
    if trend == "oversold":
        closes = [100.0 - i for i in range(15)]   # 단조 하락 → RSI≈0
    elif trend == "overbought":
        closes = [100.0 + i for i in range(15)]   # 단조 상승 → RSI≈100
    else:
        closes = [100.0 + (1 if i % 2 else -1) for i in range(15)]  # 진동 → RSI≈50
    return deque(closes, maxlen=200)


def test_select_symbols_returns_list(strategy):
    symbols = strategy.select_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0


def test_on_bar_buys_when_oversold(strategy):
    strategy._positions = {}
    strategy._bar_buffer["AAPL"] = _make_buffer("oversold")

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 86.0
    strategy.on_bar(bar)

    strategy.trading_client.submit_order.assert_called_once()
    order_req = strategy.trading_client.submit_order.call_args[0][0]
    assert order_req.side.value == "buy"


def test_on_bar_sells_when_overbought(strategy):
    pos = MagicMock()
    pos.symbol = "AAPL"
    strategy._positions = {"AAPL": pos}
    strategy._bar_buffer["AAPL"] = _make_buffer("overbought")

    bar = MagicMock()
    bar.symbol = "AAPL"
    bar.close = 114.0
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
