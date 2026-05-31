import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.database import Base
from db.models import Strategy, PortfolioHistory, DailyPerformance
from decimal import Decimal
from datetime import datetime

@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e

@pytest.fixture
def manager(db_engine):
    from manager.manager import StrategyManager
    with patch("manager.manager.create_engine_for_process", return_value=db_engine), \
         patch("manager.manager.MarketDataHub"):  # 시세 허브는 mock
        m = StrategyManager()
    return m

@pytest.fixture
def running_strategy(db_engine):
    with Session(db_engine) as session:
        s = Strategy(
            id=1, name="MA 크로스오버", strategy_type="ma_crossover",
            alpaca_key="key", alpaca_secret="secret",
            budget=Decimal("10000"), status="running", run_interval="1m",
        )
        session.add(s)
        session.commit()
        session.refresh(s)
        return s

def test_start_strategy_launches_process(manager, running_strategy, db_engine):
    with patch.object(manager, "_launch_process") as mock_launch, \
         patch.object(manager, "engine", db_engine):
        manager.start_strategy(running_strategy.id)

    mock_launch.assert_called_once()

def test_stop_strategy_terminates_process(manager, running_strategy, db_engine):
    mock_proc = MagicMock()
    manager.processes[1] = {"process": mock_proc, "restart_count": 0}

    with patch.object(manager, "engine", db_engine):
        manager.stop_strategy(running_strategy.id)

    mock_proc.terminate.assert_called_once()
    assert 1 not in manager.processes

def test_launch_process_registers_with_hub(manager, running_strategy, db_engine):
    with patch("manager.manager.multiprocessing.Process"), \
         patch.object(manager, "engine", db_engine):
        with Session(db_engine) as session:
            st = session.get(Strategy, 1)
            manager._launch_process(st)

    manager.hub.add_strategy.assert_called_once()
    sid, symbols, queue = manager.hub.add_strategy.call_args[0]
    assert sid == 1
    assert "AAPL" in symbols  # MACrossover WATCHLIST
    assert 1 in manager.processes and manager.processes[1]["queue"] is queue

def test_stop_strategy_removes_from_hub_and_sentinel(manager, running_strategy, db_engine):
    mock_proc = MagicMock()
    q = MagicMock()
    manager.processes[1] = {"process": mock_proc, "restart_count": 0, "queue": q}

    with patch.object(manager, "engine", db_engine):
        manager.stop_strategy(1)

    manager.hub.remove_strategy.assert_called_once_with(1)
    q.put_nowait.assert_called_once_with(None)  # 소비 루프 종료 sentinel
    mock_proc.terminate.assert_called_once()

def test_monitor_crashes_restarts_dead_process(manager, running_strategy, db_engine):
    dead_proc = MagicMock()
    dead_proc.is_alive.return_value = False
    manager.processes[1] = {"process": dead_proc, "restart_count": 0}

    with patch.object(manager, "_launch_process") as mock_launch, \
         patch.object(manager, "engine", db_engine):
        manager._monitor_crashes()

    mock_launch.assert_called_once()

def test_monitor_crashes_marks_failed_after_3(manager, running_strategy, db_engine):
    dead_proc = MagicMock()
    dead_proc.is_alive.return_value = False
    manager.processes[1] = {"process": dead_proc, "restart_count": 3}

    with patch.object(manager, "engine", db_engine):
        manager._monitor_crashes()

    with Session(db_engine) as session:
        s = session.get(Strategy, 1)
        assert s.status == "failed"
    assert 1 not in manager.processes

def test_record_portfolio_history_saves_to_db(manager, running_strategy, db_engine):
    mock_account = MagicMock()
    mock_account.equity = "10500.00"
    mock_account.cash = "5000.00"
    mock_position = MagicMock()
    mock_position.unrealized_pl = "500.00"

    with patch("manager.manager.TradingClient") as mock_client_cls, \
         patch.object(manager, "engine", db_engine):
        mock_client_cls.return_value.get_account.return_value = mock_account
        mock_client_cls.return_value.get_all_positions.return_value = [mock_position]
        manager.record_portfolio_history()

    with Session(db_engine) as session:
        records = session.query(PortfolioHistory).all()
        assert len(records) == 1
        assert float(records[0].equity) == 10500.00

def test_terminate_process_sigkill_when_alive(manager):
    proc = MagicMock()
    proc.is_alive.return_value = True  # SIGTERM 후에도 살아있는 상황
    manager._terminate_process(proc)
    proc.terminate.assert_called_once()
    proc.kill.assert_called_once()  # SIGKILL 폴백 호출돼야 함

def test_terminate_process_no_sigkill_when_dead(manager):
    proc = MagicMock()
    proc.is_alive.return_value = False  # SIGTERM으로 정상 종료
    manager._terminate_process(proc)
    proc.terminate.assert_called_once()
    proc.kill.assert_not_called()

def test_start_strategy_idempotent_when_alive(manager, running_strategy, db_engine):
    alive_proc = MagicMock()
    alive_proc.is_alive.return_value = True
    manager.processes[1] = {"process": alive_proc, "restart_count": 0}

    with patch.object(manager, "_launch_process") as mock_launch, \
         patch.object(manager, "engine", db_engine):
        manager.start_strategy(running_strategy.id)

    mock_launch.assert_not_called()  # 이미 실행 중 → 중복 실행 안 함

def test_monitor_crashes_preserves_restart_count(manager, running_strategy, db_engine):
    dead_proc = MagicMock()
    dead_proc.is_alive.return_value = False
    manager.processes[1] = {"process": dead_proc, "restart_count": 1}

    with patch.object(manager, "_launch_process") as mock_launch, \
         patch.object(manager, "engine", db_engine):
        manager._monitor_crashes()

    # 재시작 시 카운터가 0으로 리셋되지 않고 +1로 전달돼야 함
    assert mock_launch.call_args.kwargs["restart_count"] == 2

def test_record_daily_performance_saves_to_db(manager, running_strategy, db_engine):
    mock_account = MagicMock()
    mock_account.equity = "10200.00"

    with patch("manager.manager.TradingClient") as mock_client_cls, \
         patch.object(manager, "engine", db_engine):
        mock_client_cls.return_value.get_account.return_value = mock_account
        manager.record_daily_performance()

    with Session(db_engine) as session:
        rows = session.query(DailyPerformance).all()
        assert len(rows) == 1
        assert float(rows[0].total_value) == 10200.00
        # 첫날: 예산(10000) 대비 (10200-10000)/10000 = 0.02
        assert abs(float(rows[0].daily_return) - 0.02) < 1e-6
