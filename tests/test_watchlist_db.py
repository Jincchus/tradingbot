import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.database import Base
from db.watchlist import DEFAULT_WATCHLIST, get_watchlist_symbols, replace_watchlist


@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e


def test_empty_watchlist_returns_default(db_engine):
    with Session(db_engine) as session:
        assert get_watchlist_symbols(session) == DEFAULT_WATCHLIST


def test_replace_then_get_returns_saved(db_engine):
    with Session(db_engine) as session:
        replace_watchlist(session, ["TSLA", "AMD"])
        session.commit()
        assert get_watchlist_symbols(session) == ["TSLA", "AMD"]


def test_replace_overwrites_previous(db_engine):
    with Session(db_engine) as session:
        replace_watchlist(session, ["TSLA"])
        session.commit()
        replace_watchlist(session, ["AMD", "INTC"])
        session.commit()
        assert get_watchlist_symbols(session) == ["AMD", "INTC"]


def test_strategy_position_size_defaults_to_point_two(db_engine):
    from decimal import Decimal
    from db.models import Strategy
    with Session(db_engine) as session:
        s = Strategy(
            name="x", strategy_type="ma_crossover",
            alpaca_key="k", alpaca_secret="s",
            budget=Decimal("10000"), status="stopped", run_interval="1m",
        )
        session.add(s)
        session.commit()
        session.refresh(s)
        assert float(s.position_size) == 0.2
