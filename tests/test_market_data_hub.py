import asyncio
import pytest
from unittest.mock import MagicMock


@pytest.fixture
def hub():
    from manager.market_data_hub import MarketDataHub
    h = MarketDataHub("datakey", "datasecret")
    h.stream = MagicMock()  # start() 대신 mock 스트림 주입
    return h


def test_add_strategy_subscribes_new_symbols(hub):
    q = MagicMock()
    hub.add_strategy(1, ["AAPL", "MSFT"], q)
    hub.stream.subscribe_bars.assert_called_once()
    args = hub.stream.subscribe_bars.call_args[0]
    assert args[0] == hub._on_bar          # 핸들러로 _on_bar 전달 (bound method는 == 비교)
    assert set(args[1:]) == {"AAPL", "MSFT"}


def test_add_second_strategy_same_symbol_no_resubscribe(hub):
    q1, q2 = MagicMock(), MagicMock()
    hub.add_strategy(1, ["AAPL"], q1)
    hub.stream.subscribe_bars.reset_mock()
    hub.add_strategy(2, ["AAPL"], q2)      # 이미 구독 중인 종목
    hub.stream.subscribe_bars.assert_not_called()


def test_on_bar_routes_to_all_subscribed_queues(hub):
    q1, q2 = MagicMock(), MagicMock()
    hub.add_strategy(1, ["AAPL"], q1)
    hub.add_strategy(2, ["AAPL"], q2)
    bar = MagicMock(); bar.symbol = "AAPL"

    asyncio.run(hub._on_bar(bar))

    q1.put_nowait.assert_called_once_with(bar)
    q2.put_nowait.assert_called_once_with(bar)


def test_on_bar_only_routes_to_symbol_subscribers(hub):
    q1, q2 = MagicMock(), MagicMock()
    hub.add_strategy(1, ["AAPL"], q1)
    hub.add_strategy(2, ["MSFT"], q2)
    bar = MagicMock(); bar.symbol = "AAPL"

    asyncio.run(hub._on_bar(bar))

    q1.put_nowait.assert_called_once_with(bar)
    q2.put_nowait.assert_not_called()


def test_remove_strategy_unsubscribes_orphan_symbol(hub):
    q1, q2 = MagicMock(), MagicMock()
    hub.add_strategy(1, ["AAPL", "MSFT"], q1)
    hub.add_strategy(2, ["AAPL"], q2)
    hub.remove_strategy(1)
    # MSFT는 이제 아무도 안 봄 → 구독 해지, AAPL은 q2가 봄 → 유지
    hub.stream.unsubscribe_bars.assert_called_once_with("MSFT")


def test_remove_strategy_keeps_shared_symbol(hub):
    q1, q2 = MagicMock(), MagicMock()
    hub.add_strategy(1, ["AAPL"], q1)
    hub.add_strategy(2, ["AAPL"], q2)
    hub.stream.unsubscribe_bars.reset_mock()
    hub.remove_strategy(1)
    hub.stream.unsubscribe_bars.assert_not_called()  # AAPL은 q2가 여전히 봄


def test_remove_strategy_stops_routing(hub):
    q1 = MagicMock()
    hub.add_strategy(1, ["AAPL"], q1)
    hub.remove_strategy(1)
    bar = MagicMock(); bar.symbol = "AAPL"
    asyncio.run(hub._on_bar(bar))
    q1.put_nowait.assert_not_called()
