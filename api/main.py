import os
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from alpaca.trading.client import TradingClient
from db.database import create_engine_for_process
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance
from api.schemas import (StrategyResponse, TradeResponse,
                         PortfolioHistoryResponse, DailyPerformanceResponse,
                         PositionResponse, WatchlistResponse, WatchlistUpdate,
                         StrategyUpdate)
from db.watchlist import get_watchlist_symbols
from manager.manager import StrategyManager

# Token authentication
_BOT_API_TOKEN = os.getenv("BOT_API_TOKEN", "")

def require_token(authorization: str = Header(default="")) -> None:
    """Validates Bearer token. If BOT_API_TOKEN is not set, auth is skipped (dev mode)."""
    if not _BOT_API_TOKEN:
        return
    if authorization != f"Bearer {_BOT_API_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


_manager: StrategyManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _manager
    _manager = StrategyManager()
    _manager.start()
    yield
    _manager.stop()


app = FastAPI(title="Trading Bot API", lifespan=lifespan)

# 기존 웹 프론트엔드가 브라우저에서 호출 → CORS 허용 필수 (미설정 시 전부 차단)
_cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine_for_process()
    return _engine


def get_manager() -> StrategyManager:
    return _manager


@app.get("/strategies", response_model=List[StrategyResponse], dependencies=[Depends(require_token)])
def list_strategies(engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        return session.query(Strategy).all()


@app.patch("/strategies/{id}", response_model=StrategyResponse, dependencies=[Depends(require_token)])
def update_strategy(id: int, body: StrategyUpdate, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        strategy = session.get(Strategy, id)
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        if body.position_size is not None:
            if not (0 < body.position_size <= 1):
                raise HTTPException(status_code=400, detail="position_size must be in (0, 1]")
            strategy.position_size = body.position_size
        session.commit()
        session.refresh(strategy)
        return strategy


@app.get("/strategies/{id}/performance", response_model=List[DailyPerformanceResponse], dependencies=[Depends(require_token)])
def get_performance(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
        return (session.query(DailyPerformance)
                .filter(DailyPerformance.strategy_id == id)
                .order_by(DailyPerformance.date).all())


@app.get("/strategies/{id}/portfolio", response_model=List[PortfolioHistoryResponse], dependencies=[Depends(require_token)])
def get_portfolio(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
        return (session.query(PortfolioHistory)
                .filter(PortfolioHistory.strategy_id == id)
                .order_by(PortfolioHistory.timestamp).all())


@app.get("/strategies/{id}/trades", response_model=List[TradeResponse], dependencies=[Depends(require_token)])
def get_trades(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
        return (session.query(Trade)
                .filter(Trade.strategy_id == id)
                .order_by(Trade.filled_at.desc()).all())


@app.get("/strategies/{id}/positions", response_model=List[PositionResponse], dependencies=[Depends(require_token)])
def get_positions(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        strategy = session.get(Strategy, id)
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        key, secret = strategy.alpaca_key, strategy.alpaca_secret
    try:
        client = TradingClient(key, secret, paper=True)
        positions = client.get_all_positions()
    except Exception:
        raise HTTPException(status_code=502, detail="Alpaca positions fetch failed")
    return [
        PositionResponse(
            symbol=p.symbol,
            qty=p.qty,
            avg_entry_price=p.avg_entry_price,
            current_price=getattr(p, "current_price", None),
            unrealized_pl=p.unrealized_pl,
            unrealized_plpc=p.unrealized_plpc,
        )
        for p in positions
    ]


@app.post("/strategies/{id}/start", dependencies=[Depends(require_token)])
def start_strategy(id: int, engine: Engine = Depends(get_engine),
                   mgr: StrategyManager = Depends(get_manager)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
    mgr.start_strategy(id)
    return {"message": "started"}


@app.post("/strategies/{id}/stop", dependencies=[Depends(require_token)])
def stop_strategy(id: int, engine: Engine = Depends(get_engine),
                  mgr: StrategyManager = Depends(get_manager)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
    mgr.stop_strategy(id)
    return {"message": "stopped"}


def validate_symbols(symbols: list[str]) -> list[str]:
    """거래 불가/존재하지 않는 종목 목록을 돌려준다(빈 리스트=모두 정상)."""
    key = os.getenv("ALPACA_KEY", "")
    secret = os.getenv("ALPACA_SECRET", "")
    if not key or not secret:
        raise HTTPException(status_code=503, detail="ALPACA_KEY not configured for validation")
    client = TradingClient(key, secret, paper=True)
    invalid: list[str] = []
    for sym in symbols:
        try:
            asset = client.get_asset(sym)
            if not (asset.tradable and asset.status.value == "active"):
                invalid.append(sym)
        except Exception:
            invalid.append(sym)
    return invalid


@app.get("/watchlist", response_model=WatchlistResponse, dependencies=[Depends(require_token)])
def get_watchlist(engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        return WatchlistResponse(symbols=get_watchlist_symbols(session))


@app.put("/watchlist", response_model=WatchlistResponse, dependencies=[Depends(require_token)])
def update_watchlist(body: WatchlistUpdate,
                     mgr: StrategyManager = Depends(get_manager)):
    symbols = [s.strip().upper() for s in body.symbols if s.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="watchlist must contain at least one symbol")
    invalid = validate_symbols(symbols)
    if invalid:
        raise HTTPException(status_code=400, detail=f"invalid or non-tradable symbols: {invalid}")
    mgr.apply_watchlist(symbols)
    return WatchlistResponse(symbols=symbols)


@app.post("/strategies/{id}/positions/{symbol}/close", dependencies=[Depends(require_token)])
def close_position(id: int, symbol: str, engine: Engine = Depends(get_engine),
                   mgr: StrategyManager = Depends(get_manager)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
    mgr.liquidate_strategy(id, symbol=symbol.upper())
    return {"message": "closed", "symbol": symbol.upper()}


@app.post("/strategies/{id}/liquidate", dependencies=[Depends(require_token)])
def liquidate_strategy(id: int, engine: Engine = Depends(get_engine),
                       mgr: StrategyManager = Depends(get_manager)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
    mgr.liquidate_strategy(id)
    return {"message": "liquidated"}


@app.post("/liquidate-all", dependencies=[Depends(require_token)])
def liquidate_all(mgr: StrategyManager = Depends(get_manager)):
    mgr.liquidate_all()
    return {"message": "all liquidated"}
