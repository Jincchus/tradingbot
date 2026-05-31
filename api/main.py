import os
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from db.database import create_engine_for_process
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance
from api.schemas import (StrategyResponse, TradeResponse,
                         PortfolioHistoryResponse, DailyPerformanceResponse)
from manager.manager import StrategyManager

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


@app.get("/strategies", response_model=List[StrategyResponse])
def list_strategies(engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        return session.query(Strategy).all()


@app.get("/strategies/{id}/performance", response_model=List[DailyPerformanceResponse])
def get_performance(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
        return (session.query(DailyPerformance)
                .filter(DailyPerformance.strategy_id == id)
                .order_by(DailyPerformance.date).all())


@app.get("/strategies/{id}/portfolio", response_model=List[PortfolioHistoryResponse])
def get_portfolio(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
        return (session.query(PortfolioHistory)
                .filter(PortfolioHistory.strategy_id == id)
                .order_by(PortfolioHistory.timestamp).all())


@app.get("/strategies/{id}/trades", response_model=List[TradeResponse])
def get_trades(id: int, engine: Engine = Depends(get_engine)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
        return (session.query(Trade)
                .filter(Trade.strategy_id == id)
                .order_by(Trade.filled_at.desc()).all())


@app.post("/strategies/{id}/start")
def start_strategy(id: int, engine: Engine = Depends(get_engine),
                   mgr: StrategyManager = Depends(get_manager)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
    mgr.start_strategy(id)
    return {"message": "started"}


@app.post("/strategies/{id}/stop")
def stop_strategy(id: int, engine: Engine = Depends(get_engine),
                  mgr: StrategyManager = Depends(get_manager)):
    with Session(engine) as session:
        if not session.get(Strategy, id):
            raise HTTPException(status_code=404, detail="Strategy not found")
    mgr.stop_strategy(id)
    return {"message": "stopped"}
