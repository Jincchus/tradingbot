from pydantic import BaseModel
from datetime import datetime, date
from decimal import Decimal
from typing import Optional


class StrategyResponse(BaseModel):
    id: int
    name: str
    strategy_type: str
    budget: Decimal
    status: str
    run_interval: str
    created_at: datetime

    model_config = {"from_attributes": True}


class TradeResponse(BaseModel):
    id: int
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    alpaca_order_id: str
    filled_at: datetime

    model_config = {"from_attributes": True}


class PortfolioHistoryResponse(BaseModel):
    id: int
    timestamp: datetime
    equity: Decimal
    cash: Decimal
    unrealized_pnl: Decimal

    model_config = {"from_attributes": True}


class DailyPerformanceResponse(BaseModel):
    id: int
    date: date
    total_value: Decimal
    daily_return: Decimal
    win_rate: Optional[Decimal]
    sharpe_ratio: Optional[Decimal]
    drawdown: Optional[Decimal]

    model_config = {"from_attributes": True}
