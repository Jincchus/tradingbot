import importlib
import inspect
import logging
import multiprocessing
import statistics
import threading
from collections import defaultdict, deque
from datetime import datetime, date

from apscheduler.schedulers.background import BackgroundScheduler
from alpaca.trading.client import TradingClient
from sqlalchemy.orm import Session

from db.database import create_engine_for_process
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance

logger = logging.getLogger("manager")

MAX_RESTARTS = 3


class StrategyManager:
    def __init__(self):
        self.engine = create_engine_for_process()
        self.processes: dict[int, dict] = {}
        self.scheduler = BackgroundScheduler()
        self._lock = threading.RLock()  # processes 딕셔너리를 스케줄러/요청 스레드 동시 접근으로부터 보호

    def start(self) -> None:
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
            for info in self.processes.values():
                info["process"].terminate()

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
            if strategy_id in self.processes:
                self.processes[strategy_id]["process"].terminate()
                del self.processes[strategy_id]

    def _launch_process(self, strategy: Strategy, restart_count: int = 0) -> None:
        # 항상 _lock 하에서 호출됨 (start/start_strategy/_monitor_crashes)
        cls = self._load_strategy_class(strategy.strategy_type)
        instance = cls(
            strategy_id=strategy.id,
            name=strategy.name,
            api_key=strategy.alpaca_key,
            api_secret=strategy.alpaca_secret,
            budget=float(strategy.budget),
            run_interval=strategy.run_interval,
        )
        proc = multiprocessing.Process(target=instance.run, daemon=True)
        proc.start()
        # restart_count는 재시작 시에도 보존되어야 함 (덮어쓰면 무한 재시작)
        self.processes[strategy.id] = {"process": proc, "restart_count": restart_count}
        logger.info(f"Launched {strategy.name} (type={strategy.strategy_type}) pid={proc.pid}")

    def _load_strategy_class(self, strategy_type: str):
        module = importlib.import_module(f"strategies.{strategy_type}")
        from strategies.base import BaseStrategy
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if issubclass(cls, BaseStrategy) and cls is not BaseStrategy:
                return cls
        raise ValueError(f"No BaseStrategy subclass in strategies/{strategy_type}.py")

    def _monitor_crashes(self) -> None:
        with self._lock, Session(self.engine) as session:
            for strategy_id, info in list(self.processes.items()):
                if info["process"].is_alive():
                    continue
                strategy = session.get(Strategy, strategy_id)
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
