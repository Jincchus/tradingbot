import importlib
import inspect
import logging
import multiprocessing
import os
import statistics
import threading
from collections import defaultdict, deque
from datetime import datetime, date

from apscheduler.schedulers.background import BackgroundScheduler
from alpaca.trading.client import TradingClient
from sqlalchemy.orm import Session

from db.database import create_engine_for_process
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance
from db.watchlist import get_watchlist_symbols
from manager.market_data_hub import MarketDataHub

logger = logging.getLogger("manager")

MAX_RESTARTS = 3


class StrategyManager:
    def __init__(self):
        self.engine = create_engine_for_process()
        self.processes: dict[int, dict] = {}
        self.scheduler = BackgroundScheduler()
        self._lock = threading.RLock()  # processes 딕셔너리를 스케줄러/요청 스레드 동시 접근으로부터 보호
        # 시세는 거래용 키와 분리된 별도 Alpaca 로그인 키로 1개만 연결 (팬아웃 허브)
        self.hub = MarketDataHub(
            os.getenv("ALPACA_DATA_KEY", ""),
            os.getenv("ALPACA_DATA_SECRET", ""),
        )

    def start(self) -> None:
        self.hub.start()
        self.scheduler.add_job(self._monitor_crashes, "interval", seconds=30)
        self.scheduler.add_job(self.record_portfolio_history, "interval", minutes=5)
        # 장 마감 후(평일 16:05 ET) 일별 성과 기록 — 성과 비교의 핵심 데이터
        self.scheduler.add_job(
            self.record_daily_performance, "cron",
            day_of_week="mon-fri", hour=16, minute=5, timezone="America/New_York",
        )
        self.scheduler.start()
        with self._lock, Session(self.engine) as session:
            running = session.query(Strategy).filter(Strategy.status == "running").all()
            for strategy in running:
                self._launch_process(strategy)

    def stop(self) -> None:
        self.scheduler.shutdown(wait=False)
        with self._lock:
            for strategy_id in list(self.processes):
                self._teardown_process(strategy_id)
        self.hub.stop()

    def start_strategy(self, strategy_id: int) -> None:
        with self._lock:
            # 멱등성: 이미 살아있으면 중복 실행 금지 (좀비 프로세스 방지)
            existing = self.processes.get(strategy_id)
            if existing and existing["process"].is_alive():
                logger.info(f"strategy {strategy_id} already running, skip")
                return
            with Session(self.engine) as session:
                strategy = session.get(Strategy, strategy_id)
                strategy.status = "running"
                session.commit()
                session.refresh(strategy)
                self._launch_process(strategy)

    def stop_strategy(self, strategy_id: int) -> None:
        with self._lock:
            with Session(self.engine) as session:
                strategy = session.get(Strategy, strategy_id)
                strategy.status = "stopped"
                session.commit()
            self._teardown_process(strategy_id)

    def _teardown_process(self, strategy_id: int) -> None:
        """전략 프로세스 정리: 허브 라우팅 해제 → 큐 sentinel → 프로세스 종료. _lock 하에서 호출."""
        info = self.processes.get(strategy_id)
        if info is None:
            return
        self.hub.remove_strategy(strategy_id)
        q = info.get("queue")
        if q is not None:
            try:
                q.put_nowait(None)  # 소비 루프에 종료 신호
            except Exception:
                pass
        self._terminate_process(info["process"])
        del self.processes[strategy_id]

    def _terminate_process(self, proc) -> None:
        """SIGTERM → 5s 대기 → 미종료 시 SIGKILL. join으로 좀비(defunct) reap.

        전략 프로세스는 alpaca asyncio 웹소켓 루프가 SIGTERM을 즉시 처리하지 못해
        terminate()만으로는 종료가 보장되지 않으므로 SIGKILL 폴백이 필요하다.
        """
        proc.terminate()
        proc.join(timeout=5)
        if proc.is_alive():
            logger.warning(f"pid={proc.pid} did not stop on SIGTERM, sending SIGKILL")
            proc.kill()
            proc.join(timeout=3)

    def _launch_process(self, strategy: Strategy, restart_count: int = 0) -> None:
        # 항상 _lock 하에서 호출됨 (start/start_strategy/_monitor_crashes)
        cls = self._load_strategy_class(strategy.strategy_type)
        with Session(self.engine) as session:
            symbols = get_watchlist_symbols(session)
        bar_queue = multiprocessing.Queue()
        instance = cls(
            strategy_id=strategy.id,
            name=strategy.name,
            api_key=strategy.alpaca_key,
            api_secret=strategy.alpaca_secret,
            budget=float(strategy.budget),
            run_interval=strategy.run_interval,
            bar_queue=bar_queue,
            symbols=symbols,
            position_size=float(strategy.position_size),
        )
        # select_symbols는 매니저 프로세스에서도 호출되므로 무거운 자원에 의존하면 안 됨
        symbols = instance.select_symbols()
        proc = multiprocessing.Process(target=instance.run, daemon=True)
        proc.start()
        # restart_count는 재시작 시에도 보존되어야 함 (덮어쓰면 무한 재시작)
        self.processes[strategy.id] = {
            "process": proc, "restart_count": restart_count,
            "queue": bar_queue, "symbols": symbols,
        }
        # 허브에 종목 등록 → 허브가 시세 1연결로 받아 이 큐로 분배
        self.hub.add_strategy(strategy.id, symbols, bar_queue)
        logger.info(f"Launched {strategy.name} (type={strategy.strategy_type}) "
                    f"pid={proc.pid} symbols={symbols}")

    def _load_strategy_class(self, strategy_type: str):
        module = importlib.import_module(f"strategies.{strategy_type}")
        from strategies.base import BaseStrategy
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if issubclass(cls, BaseStrategy) and cls is not BaseStrategy:
                return cls
        raise ValueError(f"No BaseStrategy subclass in strategies/{strategy_type}.py")

    def _make_client(self, strategy) -> TradingClient:
        return TradingClient(strategy.alpaca_key, strategy.alpaca_secret, paper=True)

    def liquidate_strategy(self, strategy_id: int, symbol: str | None = None) -> None:
        """비상 청산: 봇을 멈추고(stopped) 포지션을 판다. symbol=None이면 전체 청산.

        봇이 켜진 채 강제 매도하면 포지션 캐시가 어긋나므로 항상 먼저 멈춘다.
        청산은 봇 프로세스가 아니라 매니저가 직접 주문 → 봇이 죽어있어도 작동.
        """
        with self._lock, Session(self.engine) as session:
            strategy = session.get(Strategy, strategy_id)
            if strategy is None:
                return
            strategy.status = "stopped"
            session.commit()
            self._teardown_process(strategy_id)
            try:
                client = self._make_client(strategy)
                if symbol is None:
                    client.close_all_positions(cancel_orders=True)
                else:
                    client.close_position(symbol)
            except Exception:
                logger.exception(f"liquidate failed strategy={strategy_id} symbol={symbol}")

    def liquidate_all(self) -> None:
        """폭락 비상 버튼: running 전략을 모두 청산 + 정지."""
        with self._lock, Session(self.engine) as session:
            ids = [s.id for s in session.query(Strategy)
                   .filter(Strategy.status == "running").all()]
        for sid in ids:
            self.liquidate_strategy(sid)

    def _monitor_crashes(self) -> None:
        with self._lock, Session(self.engine) as session:
            for strategy_id, info in list(self.processes.items()):
                if info["process"].is_alive():
                    continue
                strategy = session.get(Strategy, strategy_id)
                self.hub.remove_strategy(strategy_id)  # 죽은 전략의 라우팅 제거 (재시작 시 새로 등록)
                if info["restart_count"] < MAX_RESTARTS:
                    next_count = info["restart_count"] + 1
                    logger.warning(f"{strategy.name} crashed, restarting ({next_count}/{MAX_RESTARTS})")
                    self._launch_process(strategy, restart_count=next_count)  # 카운터 보존
                else:
                    logger.error(f"{strategy.name} failed {MAX_RESTARTS} times, marking failed")
                    strategy.status = "failed"
                    session.commit()
                    del self.processes[strategy_id]

    def record_portfolio_history(self) -> None:
        with Session(self.engine) as session:
            running = session.query(Strategy).filter(Strategy.status == "running").all()
            for strategy in running:
                try:  # 한 전략의 키 오류가 다른 전략 기록을 막지 않도록 격리
                    client = TradingClient(strategy.alpaca_key, strategy.alpaca_secret, paper=True)
                    account = client.get_account()
                    positions = client.get_all_positions()
                    unrealized_pnl = sum(float(p.unrealized_pl) for p in positions)
                    session.add(PortfolioHistory(
                        strategy_id=strategy.id,
                        timestamp=datetime.utcnow(),
                        equity=float(account.equity),
                        cash=float(account.cash),
                        unrealized_pnl=unrealized_pnl,
                    ))
                except Exception:
                    logger.exception(f"portfolio_history failed for strategy {strategy.id}")
            session.commit()

    def record_daily_performance(self) -> None:
        """장 마감 후 1회: 전략별 일별 수익률/승률/Sharpe/MDD 계산 → daily_performance upsert."""
        today = date.today()
        with Session(self.engine) as session:
            running = session.query(Strategy).filter(Strategy.status == "running").all()
            for strategy in running:
                try:
                    client = TradingClient(strategy.alpaca_key, strategy.alpaca_secret, paper=True)
                    equity = float(client.get_account().equity)

                    prev = (session.query(DailyPerformance)
                            .filter(DailyPerformance.strategy_id == strategy.id,
                                    DailyPerformance.date < today)
                            .order_by(DailyPerformance.date.desc()).first())
                    prev_value = float(prev.total_value) if prev else float(strategy.budget)
                    daily_return = (equity - prev_value) / prev_value if prev_value else 0.0

                    # Sharpe: 과거 일별 수익률 + 오늘치 (>=2개일 때만), 연율화(√252)
                    returns = [float(r.daily_return) for r in
                               session.query(DailyPerformance)
                               .filter(DailyPerformance.strategy_id == strategy.id,
                                       DailyPerformance.date < today).all()]
                    returns.append(daily_return)
                    sharpe = None
                    if len(returns) >= 2:
                        sd = statistics.pstdev(returns)
                        sharpe = (statistics.mean(returns) / sd * (252 ** 0.5)) if sd else None

                    # MDD: portfolio_history equity 고점 대비 현재 낙폭
                    equities = [float(p.equity) for p in
                                session.query(PortfolioHistory)
                                .filter(PortfolioHistory.strategy_id == strategy.id).all()]
                    equities.append(equity)
                    peak = max(equities)
                    drawdown = (peak - equity) / peak if peak else 0.0

                    win_rate = self._compute_win_rate(session, strategy.id)

                    row = (session.query(DailyPerformance)
                           .filter_by(strategy_id=strategy.id, date=today).first())
                    if row is None:
                        row = DailyPerformance(strategy_id=strategy.id, date=today)
                        session.add(row)
                    row.total_value = equity
                    row.daily_return = daily_return
                    row.win_rate = win_rate
                    row.sharpe_ratio = sharpe
                    row.drawdown = drawdown
                except Exception:
                    logger.exception(f"daily_performance failed for strategy {strategy.id}")
            session.commit()

    def _compute_win_rate(self, session: Session, strategy_id: int):
        """체결 내역을 종목별 FIFO로 매칭해 실현 승률 계산 (청산이 없으면 None)."""
        trades = (session.query(Trade)
                  .filter(Trade.strategy_id == strategy_id)
                  .order_by(Trade.filled_at).all())
        lots: dict[str, deque] = defaultdict(deque)  # symbol -> [ [qty, price], ... ] (매수 잔량)
        wins = total = 0
        for t in trades:
            qty, price = float(t.qty), float(t.price)
            if t.side == "buy":
                lots[t.symbol].append([qty, price])
            else:  # 매도 → FIFO 매수와 매칭해 실현
                remaining = qty
                while remaining > 1e-9 and lots[t.symbol]:
                    lot = lots[t.symbol][0]
                    take = min(remaining, lot[0])
                    total += 1
                    if price > lot[1]:
                        wins += 1
                    lot[0] -= take
                    remaining -= take
                    if lot[0] <= 1e-9:
                        lots[t.symbol].popleft()
        return (wins / total) if total else None
