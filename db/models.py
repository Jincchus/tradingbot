from sqlalchemy import Integer, String, Numeric, DateTime, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from db.database import Base
from datetime import datetime, date
from decimal import Decimal
from typing import Optional


class Strategy(Base):
    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(100), nullable=False)
    alpaca_key: Mapped[str] = mapped_column(String(255), nullable=False)
    alpaca_secret: Mapped[str] = mapped_column(String(255), nullable=False)
    budget: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="stopped")
    run_interval: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    trades: Mapped[list["Trade"]] = relationship(back_populates="strategy")
    portfolio_history: Mapped[list["PortfolioHistory"]] = relationship(back_populates="strategy")
    daily_performance: Mapped[list["DailyPerformance"]] = relationship(back_populates="strategy")


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id", ondelete="CASCADE"))
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    qty: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    alpaca_order_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    strategy: Mapped["Strategy"] = relationship(back_populates="trades")


class PortfolioHistory(Base):
    __tablename__ = "portfolio_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id", ondelete="CASCADE"))
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    strategy: Mapped["Strategy"] = relationship(back_populates="portfolio_history")


class DailyPerformance(Base):
    __tablename__ = "daily_performance"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id", ondelete="CASCADE"))
    date: Mapped[date] = mapped_column(Date, nullable=False)
    total_value: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    daily_return: Mapped[Decimal] = mapped_column(Numeric(6, 4), nullable=False)
    win_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    sharpe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    drawdown: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))

    __table_args__ = (UniqueConstraint("strategy_id", "date"),)

    strategy: Mapped["Strategy"] = relationship(back_populates="daily_performance")
