import logging
import threading
from collections import defaultdict

from alpaca.data.live import StockDataStream

logger = logging.getLogger("market_data_hub")


class MarketDataHub:
    """시세 웹소켓 1개로 모든 전략의 종목을 구독하고, 받은 bar를 전략별 큐로 분배(팬아웃).

    무료 데이터 플랜의 '동시 시세 연결 1개' 한도를 우회하기 위한 허브. 매니저 프로세스
    내에서 백그라운드 스레드로 StockDataStream을 구동하며, 전략 프로세스와는
    multiprocessing.Queue로 bar를 주고받는다. 거래/체결은 전략별 키로 따로 처리되므로
    여기서는 시세만 다룬다 (시세용 키는 거래용 키와 분리된 별도 Alpaca 로그인).
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.stream: StockDataStream | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        # 종목 → {strategy_id: queue} 라우팅 테이블
        self._routes: dict[str, dict[int, object]] = defaultdict(dict)
        # strategy_id → 구독 종목 집합 (제거 시 정리용)
        self._strategy_symbols: dict[int, set[str]] = {}

    def start(self) -> None:
        self.stream = StockDataStream(self.api_key, self.api_secret)
        self._thread = threading.Thread(target=self.stream.run, daemon=True)
        self._thread.start()
        logger.info("market data hub started")

    def stop(self) -> None:
        if self.stream is not None:
            try:
                self.stream.stop()
            except Exception:
                logger.exception("hub stream stop failed")

    def add_strategy(self, strategy_id: int, symbols: list[str], queue) -> None:
        """전략의 종목들을 라우팅에 등록. 아무도 안 보던 종목만 새로 구독."""
        with self._lock:
            self._strategy_symbols[strategy_id] = set(symbols)
            new_symbols = []
            for sym in symbols:
                if not self._routes[sym]:  # 이 종목을 처음 보는 전략
                    new_symbols.append(sym)
                self._routes[sym][strategy_id] = queue
            if new_symbols and self.stream is not None:
                self.stream.subscribe_bars(self._on_bar, *new_symbols)
                logger.info(f"subscribed new symbols: {new_symbols}")

    def remove_strategy(self, strategy_id: int) -> None:
        """전략을 라우팅에서 제거. 아무도 안 보게 된 종목만 구독 해지."""
        with self._lock:
            symbols = self._strategy_symbols.pop(strategy_id, set())
            orphans = []
            for sym in symbols:
                self._routes[sym].pop(strategy_id, None)
                if not self._routes[sym]:
                    orphans.append(sym)
                    del self._routes[sym]
            if orphans and self.stream is not None:
                self.stream.unsubscribe_bars(*orphans)
                logger.info(f"unsubscribed orphan symbols: {orphans}")

    async def _on_bar(self, bar) -> None:
        """StockDataStream async 핸들러. 해당 종목 구독 전략 큐에 bar 분배."""
        with self._lock:
            targets = list(self._routes.get(bar.symbol, {}).items())
        for strategy_id, queue in targets:
            try:
                queue.put_nowait(bar)
            except Exception:
                logger.exception(f"failed to route bar to strategy {strategy_id}")
