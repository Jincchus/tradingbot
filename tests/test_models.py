import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.database import Base
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance
from datetime import datetime, date
from decimal import Decimal

@pytest.fixture
def engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e

def test_strategy_create(engine):
    with Session(engine) as session:
        s = Strategy(
            name="MA 크로스오버 v1",
            strategy_type="ma_crossover",
            alpaca_key="key",
            alpaca_secret="secret",
            budget=Decimal("10000.00"),
            status="stopped",
            run_interval="1m",
        )
        session.add(s)
        session.commit()
        assert s.id is not None

def test_trade_create(engine):
    with Session(engine) as session:
        s = Strategy(name="t", strategy_type="ma_crossover", alpaca_key="k",
                     alpaca_secret="s", budget=Decimal("10000"), status="stopped", run_interval="1m")
        session.add(s)
        session.flush()
        trade = Trade(
            strategy_id=s.id, symbol="AAPL", side="buy",
            qty=Decimal("10"), price=Decimal("150.00"),
            alpaca_order_id="order-123", filled_at=datetime.utcnow(),
        )
        session.add(trade)
        session.commit()
        assert trade.id is not None

def test_portfolio_history_create(engine):
    with Session(engine) as session:
        s = Strategy(name="t", strategy_type="ma_crossover", alpaca_key="k",
                     alpaca_secret="s", budget=Decimal("10000"), status="stopped", run_interval="1m")
        session.add(s)
        session.flush()
        ph = PortfolioHistory(
            strategy_id=s.id, timestamp=datetime.utcnow(),
            equity=Decimal("10500"), cash=Decimal("5000"), unrealized_pnl=Decimal("500"),
        )
        session.add(ph)
        session.commit()
        assert ph.id is not None

def test_daily_performance_create(engine):
    with Session(engine) as session:
        s = Strategy(name="t", strategy_type="ma_crossover", alpaca_key="k",
                     alpaca_secret="s", budget=Decimal("10000"), status="stopped", run_interval="1m")
        session.add(s)
        session.flush()
        dp = DailyPerformance(
            strategy_id=s.id, date=date.today(),
            total_value=Decimal("10200"), daily_return=Decimal("0.02"),
        )
        session.add(dp)
        session.commit()
        assert dp.id is not None
