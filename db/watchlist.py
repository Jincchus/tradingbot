"""감시 종목(공통) 조회/교체 헬퍼. 빈 표면 기본 5종목으로 폴백한다."""
from sqlalchemy.orm import Session

from db.models import Watchlist

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]


def get_watchlist_symbols(session: Session) -> list[str]:
    rows = session.query(Watchlist).order_by(Watchlist.id).all()
    return [r.symbol for r in rows] if rows else list(DEFAULT_WATCHLIST)


def replace_watchlist(session: Session, symbols: list[str]) -> None:
    """전체 교체. 호출자가 commit 한다."""
    session.query(Watchlist).delete()
    for sym in symbols:
        session.add(Watchlist(symbol=sym))
