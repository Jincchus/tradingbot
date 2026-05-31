import abc
import logging
import threading
from collections import deque
from datetime import datetime, timedelta

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from sqlalchemy.orm import Session

from db.database import create_engine_for_process
from db.models import Trade


def timeframe_for(run_interval: str) -> TimeFrame:
    """run_interval 문자열 → 프리페치용 Alpaca TimeFrame.

    실시간 스트림 bar는 Alpaca 한계상 항상 1분봉으로 들어오므로, 멀티분/일봉 의사결정은
    전략이 버퍼를 직접 집계해 처리한다. 분 미만(스캘핑)은 현 단계 범위 외(설계 9장).
    """
    return {
        "1m": TimeFrame(1, TimeFrameUnit.Minute),
        "5m": TimeFrame(5, TimeFrameUnit.Minute),
        "15m": TimeFrame(15, TimeFrameUnit.Minute),
        "1h": TimeFrame(1, TimeFrameUnit.Hour),
        "1d": TimeFrame(1, TimeFrameUnit.Day),
    }.get(run_interval, TimeFrame(1, TimeFrameUnit.Minute))


class BaseStrategy(abc.ABC):
    BUFFER_SIZE = 200

    def __init__(self, strategy_id: int, name: str, api_key: str, api_secret: str,
                 budget: float, run_interval: str, bar_queue=None):
        self.strategy_id = strategy_id
        self.name = name
        self.api_key = api_key
        self.api_secret = api_secret
        self.budget = budget
        self.run_interval = run_interval
        # 시세는 MarketDataHub(중앙 1연결)가 이 큐로 분배한다. 전략은 자체 시세 연결을 열지 않음.
        self.bar_queue = bar_queue
        self.logger = logging.getLogger(name)
        # 무거운 자원은 run()/_setup()에서 생성 (fork된 자식 프로세스에서만)
        self.trading_client: TradingClient | None = None
        self.trade_stream: TradingStream | None = None
        self.engine = None
        self._positions: dict = {}
        self._open_orders: dict = {}
        self._bar_buffer: dict[str, deque] = {}

    def _setup(self) -> None:
        """run() 진입(자식 프로세스) 후 호출. 부모에서 생성하면 소켓/커넥션 FD가 fork로 공유되어 깨짐.

        시세 스트림(StockDataStream)은 생성하지 않는다 — 시세는 허브에서 큐로 받는다.
        거래(TradingClient)와 체결(TradingStream)만 전략별 키로 생성한다.
        """
        self.trading_client = TradingClient(self.api_key, self.api_secret, paper=True)
        self.trade_stream = TradingStream(self.api_key, self.api_secret, paper=True)
        self.engine = create_engine_for_process()

    def sync_state(self) -> None:
        positions = self.trading_client.get_all_positions()
        open_orders = self.trading_client.get_orders(
            filter=GetOrdersRequest(status=QueryOrderStatus.OPEN)
        )
        self._positions = {p.symbol: p for p in positions}
        self._open_orders = {str(o.id): o for o in open_orders}

    def _prefetch_bars(self, symbols: list[str]) -> None:
        hist_client = StockHistoricalDataClient(self.api_key, self.api_secret)
        for symbol in symbols:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=timeframe_for(self.run_interval),
                start=datetime.utcnow() - timedelta(days=5),
                limit=self.BUFFER_SIZE,
            )
            df = hist_client.get_stock_bars(request).df
            self._bar_buffer[symbol] = deque(df["close"].tolist(), maxlen=self.BUFFER_SIZE)
            self.logger.info(f"Prefetched {len(self._bar_buffer[symbol])} bars for {symbol}")

    @abc.abstractmethod
    def select_symbols(self) -> list[str]:
        ...

    @abc.abstractmethod
    def on_bar(self, bar) -> None:
        """버퍼 + self._positions 캐시로 판단. ⚠️ 매 bar마다 REST(get_all_positions) 호출 금지."""
        ...

    def on_order_filled(self, order) -> None:
        with Session(self.engine) as session:
            session.add(Trade(
                strategy_id=self.strategy_id,
                symbol=order.symbol,
                side=order.side.value,
                qty=float(order.filled_qty),
                price=float(order.filled_avg_price),
                alpaca_order_id=str(order.id),
                filled_at=order.filled_at,
            ))
            session.commit()
        # 포지션 캐시 경량 갱신 (다음 bar 판단에 반영, REST 재호출 없이)
        if order.side.value == "buy":
            self._positions[order.symbol] = order
        else:
            self._positions.pop(order.symbol, None)

    def get_metrics(self) -> dict:
        account = self.trading_client.get_account()
        return {
            "equity": float(account.equity),
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
        }

    def _process_bar(self, bar) -> None:
        """허브 큐에서 받은 bar 처리 (동기). 예외가 소비 루프를 죽이지 않도록 격리."""
        try:
            if bar.symbol in self._bar_buffer:
                self._bar_buffer[bar.symbol].append(float(bar.close))
            self.on_bar(bar)
        except Exception:
            self.logger.exception(f"on_bar failed for {getattr(bar, 'symbol', '?')}")

    async def _trade_update_handler(self, data) -> None:
        try:
            if data.event in ("fill", "partial_fill"):
                self.on_order_filled(data.order)
        except Exception:
            self.logger.exception("trade update handler failed")

    def run(self) -> None:
        logging.basicConfig(
            filename=f"logs/{self.name}.log",
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
        self._setup()
        self.sync_state()
        symbols = self.select_symbols()
        self._prefetch_bars(symbols)

        # 체결 스트림(TradingStream)은 별도 스레드에서 자체 이벤트 루프로 구동
        self.trade_stream.subscribe_trade_updates(self._trade_update_handler)
        threading.Thread(target=self.trade_stream.run, daemon=True).start()

        # 시세는 허브가 bar_queue로 분배 → 큐 소비 (None sentinel 수신 시 종료)
        self.logger.info(f"consuming bars from hub queue for {symbols}")
        while True:
            bar = self.bar_queue.get()
            if bar is None:
                self.logger.info("received stop sentinel, exiting")
                break
            self._process_bar(bar)
