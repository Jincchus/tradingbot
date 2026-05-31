import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from db.database import Base
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance
from decimal import Decimal
from datetime import datetime, date

@pytest.fixture
def db_engine():
    # TestClient는 앱을 별도 스레드에서 구동 → in-memory SQLite는 연결마다 DB가 분리됨.
    # StaticPool + check_same_thread=False 로 단일 연결을 공유해 동일 DB를 보게 함.
    e = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(e)
    return e

@pytest.fixture
def mock_mgr():
    return MagicMock()

@pytest.fixture
def client(db_engine, mock_mgr):
    from api.main import app, get_engine, get_manager
    app.dependency_overrides[get_engine] = lambda: db_engine
    app.dependency_overrides[get_manager] = lambda: mock_mgr
    with patch("api.main.StrategyManager", return_value=mock_mgr):
        with TestClient(app) as c:
            yield c
    app.dependency_overrides.clear()

@pytest.fixture
def seeded_db(db_engine):
    with Session(db_engine) as session:
        session.add(Strategy(
            id=1, name="MA 크로스오버", strategy_type="ma_crossover",
            alpaca_key="k", alpaca_secret="s",
            budget=Decimal("10000"), status="running", run_interval="1m",
        ))
        session.add(Trade(
            strategy_id=1, symbol="AAPL", side="buy",
            qty=Decimal("10"), price=Decimal("150"),
            alpaca_order_id="ord-1", filled_at=datetime.utcnow(),
        ))
        session.add(PortfolioHistory(
            strategy_id=1, timestamp=datetime.utcnow(),
            equity=Decimal("10500"), cash=Decimal("5000"), unrealized_pnl=Decimal("500"),
        ))
        session.add(DailyPerformance(
            strategy_id=1, date=date.today(),
            total_value=Decimal("10200"), daily_return=Decimal("0.02"),
        ))
        session.commit()

def test_list_strategies(client, seeded_db):
    resp = client.get("/strategies")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["name"] == "MA 크로스오버"

def test_get_performance(client, seeded_db):
    resp = client.get("/strategies/1/performance")
    assert resp.status_code == 200
    assert len(resp.json()) == 1

def test_get_portfolio(client, seeded_db):
    resp = client.get("/strategies/1/portfolio")
    assert resp.status_code == 200
    assert len(resp.json()) == 1

def test_get_trades(client, seeded_db):
    resp = client.get("/strategies/1/trades")
    assert resp.status_code == 200
    assert resp.json()[0]["symbol"] == "AAPL"

def test_get_strategy_not_found(client, seeded_db):
    resp = client.get("/strategies/999/trades")
    assert resp.status_code == 404

def test_start_strategy_calls_manager(client, seeded_db, mock_mgr):
    resp = client.post("/strategies/1/start")
    assert resp.status_code == 200
    mock_mgr.start_strategy.assert_called_once_with(1)

def test_stop_strategy_calls_manager(client, seeded_db, mock_mgr):
    resp = client.post("/strategies/1/stop")
    assert resp.status_code == 200
    mock_mgr.stop_strategy.assert_called_once_with(1)

def test_get_positions_returns_list(client, seeded_db):
    mock_pos = MagicMock()
    mock_pos.symbol = "AAPL"
    mock_pos.qty = "10"
    mock_pos.avg_entry_price = "182.50"
    mock_pos.current_price = "190.00"
    mock_pos.unrealized_pl = "75.00"
    mock_pos.unrealized_plpc = "0.0411"

    with patch("api.main.TradingClient") as mock_cls:
        mock_cls.return_value.get_all_positions.return_value = [mock_pos]
        resp = client.get("/strategies/1/positions")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "AAPL"
    # Pydantic v2 serializes Decimal at input precision as a string ("10"->"10", "75.00"->"75.00")
    assert data[0]["qty"] == "10"
    assert data[0]["unrealized_pl"] == "75.00"

def test_get_positions_strategy_not_found(client, seeded_db):
    resp = client.get("/strategies/999/positions")
    assert resp.status_code == 404
