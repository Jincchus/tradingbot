# Trading Bot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Alpaca 페이퍼 트레이딩으로 여러 전략을 독립 프로세스로 동시 실행하고 성과를 PostgreSQL에 기록, FastAPI로 웹 프론트엔드에 제공하는 트레이딩 봇 시스템 구축

**Architecture:** StrategyManager가 FastAPI lifespan 내 백그라운드 스레드로 실행. start/stop API 요청 시 Manager가 multiprocessing.Process를 직접 생성/종료 (DB 폴링 없음, 즉시 반영). 각 전략 프로세스는 시작 시 독립 DB Engine 생성 + REST API로 과거 데이터 백필 후 WebSocket 롤링 버퍼 방식으로 지표 계산. Ubuntu Linux 서버 배포 기준.

**Tech Stack:** Python 3.11+, alpaca-py, FastAPI, SQLAlchemy 2.x, psycopg2-binary, APScheduler, pandas, pytest, httpx

---

## 파일 구조

```
tradingbot/
├── db/
│   ├── __init__.py
│   ├── database.py        # create_engine 팩토리 (프로세스별 독립 호출)
│   ├── models.py          # Strategy, Trade, PortfolioHistory, DailyPerformance
│   └── init_db.py         # 테이블 생성 스크립트
├── strategies/
│   ├── __init__.py
│   ├── base.py            # BaseStrategy (추상 클래스 + _prefetch_bars + _bar_buffer)
│   └── ma_crossover.py    # 이동평균 크로스오버 예시 전략
├── manager/
│   ├── __init__.py
│   └── manager.py         # StrategyManager (start/stop/직접 프로세스 제어)
├── api/
│   ├── __init__.py
│   ├── schemas.py         # Pydantic 응답 모델
│   └── main.py            # FastAPI + lifespan(Manager 통합) + 엔드포인트
├── tests/
│   ├── test_models.py
│   ├── test_base_strategy.py
│   ├── test_ma_crossover.py
│   ├── test_manager.py
│   └── test_api.py
├── .env.example
├── requirements.txt
└── main.py                # 진입점 (uvicorn 실행만)
```

---

## Task 1: 프로젝트 환경 설정

**Files:**
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `.gitignore`
- Create: 각 패키지 `__init__.py`

- [ ] **Step 1: requirements.txt 작성**

```
alpaca-py>=0.20.0
fastapi>=0.110.0
uvicorn[standard]>=0.27.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.9
apscheduler>=3.10.4
pandas>=2.2.0
python-dotenv>=1.0.0
pytest>=8.0.0
httpx>=0.27.0
pytest-mock>=3.12.0
```

- [ ] **Step 2: .env.example 작성**

```
DATABASE_URL=postgresql://postgres:password@localhost:5432/tradingbot
# 웹 프론트엔드 origin (CORS 허용). 쉼표로 다중 지정.
CORS_ORIGINS=http://localhost:3000
```

- [ ] **Step 3: .gitignore 작성** (시크릿/빌드 산출물 커밋 방지)

```
.env
.venv/
__pycache__/
*.pyc
logs/*.log
.pytest_cache/
```

- [ ] **Step 4: 디렉토리 및 __init__.py 생성**

```bash
mkdir -p db strategies manager api tests logs
touch db/__init__.py strategies/__init__.py manager/__init__.py api/__init__.py tests/__init__.py logs/.gitkeep
```

- [ ] **Step 5: 패키지 설치**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- [ ] **Step 6: 설치 확인**

```bash
python -c "import alpaca; import fastapi; import sqlalchemy; print('OK')"
```

Expected: `OK`

- [ ] **Step 7: Commit** (`.env`은 절대 add 하지 않음 — `.gitignore`로 차단됨)

```bash
git init
git add .gitignore requirements.txt .env.example db/__init__.py strategies/__init__.py manager/__init__.py api/__init__.py tests/__init__.py logs/.gitkeep
git commit -m "Chore: initialize project structure and dependencies"
```

---

## Task 2: DB 레이어 (database.py + models.py)

**Files:**
- Create: `db/database.py`
- Create: `db/models.py`
- Create: `db/init_db.py`
- Test: `tests/test_models.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_models.py`:
```python
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
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
python -m pytest tests/test_models.py -v
```

Expected: `ImportError` (db/database.py, db/models.py 없음)

- [ ] **Step 3: db/database.py 작성**

```python
import os
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.orm import DeclarativeBase
from dotenv import load_dotenv

load_dotenv()

class Base(DeclarativeBase):
    pass

def create_engine_for_process():
    url = os.environ["DATABASE_URL"]
    return _create_engine(url, pool_pre_ping=True)
```

- [ ] **Step 4: db/models.py 작성**

```python
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
```

- [ ] **Step 5: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/test_models.py -v
```

Expected: `4 passed`

- [ ] **Step 6: db/init_db.py 작성**

```python
from db.database import Base, create_engine_for_process
from db import models  # noqa: F401

if __name__ == "__main__":
    engine = create_engine_for_process()
    Base.metadata.create_all(engine)
    print("Tables created.")
```

- [ ] **Step 7: Commit**

```bash
git add db/
git commit -m "Feat: add DB layer with SQLAlchemy models"
```

---

## Task 3: BaseStrategy 추상 클래스

**Files:**
- Create: `strategies/base.py`
- Test: `tests/test_base_strategy.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_base_strategy.py`:
```python
import pytest
from collections import deque
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.database import Base
from db.models import Strategy, Trade
from datetime import datetime
from decimal import Decimal

@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e

@pytest.fixture
def concrete_strategy(db_engine):
    from strategies.base import BaseStrategy

    class TestStrategy(BaseStrategy):
        def select_symbols(self):
            return ["AAPL"]
        def on_bar(self, bar):
            pass

    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.StockDataStream"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = TestStrategy(
            strategy_id=1, name="test",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()  # 클라이언트/Engine/Stream을 run() 대신 여기서 mock으로 생성
    return s

def test_sync_state_loads_positions(concrete_strategy):
    mock_position = MagicMock()
    mock_position.symbol = "AAPL"
    concrete_strategy.trading_client.get_all_positions.return_value = [mock_position]
    concrete_strategy.trading_client.get_orders.return_value = []

    concrete_strategy.sync_state()

    assert "AAPL" in concrete_strategy._positions

def test_on_order_filled_updates_position_cache(concrete_strategy, db_engine):
    with Session(db_engine) as session:
        session.add(Strategy(
            id=1, name="t", strategy_type="test", alpaca_key="k", alpaca_secret="s",
            budget=Decimal("10000"), status="running", run_interval="1m",
        ))
        session.commit()

    buy = MagicMock()
    buy.symbol = "AAPL"; buy.side.value = "buy"; buy.filled_qty = "10"
    buy.filled_avg_price = "150"; buy.id = "o1"; buy.filled_at = datetime.utcnow()
    concrete_strategy.on_order_filled(buy)
    assert "AAPL" in concrete_strategy._positions

    sell = MagicMock()
    sell.symbol = "AAPL"; sell.side.value = "sell"; sell.filled_qty = "10"
    sell.filled_avg_price = "160"; sell.id = "o2"; sell.filled_at = datetime.utcnow()
    concrete_strategy.on_order_filled(sell)
    assert "AAPL" not in concrete_strategy._positions

def test_prefetch_bars_fills_buffer(concrete_strategy):
    mock_df = MagicMock()
    mock_df.__getitem__.return_value.tolist.return_value = [100.0] * 50

    with patch("strategies.base.StockHistoricalDataClient") as mock_hist:
        mock_hist.return_value.get_stock_bars.return_value.df = mock_df
        concrete_strategy._prefetch_bars(["AAPL"])

    assert "AAPL" in concrete_strategy._bar_buffer
    assert len(concrete_strategy._bar_buffer["AAPL"]) == 50

def test_on_order_filled_saves_trade(concrete_strategy, db_engine):
    with Session(db_engine) as session:
        session.add(Strategy(
            id=1, name="test", strategy_type="test", alpaca_key="k", alpaca_secret="s",
            budget=Decimal("10000"), status="running", run_interval="1m",
        ))
        session.commit()

    mock_order = MagicMock()
    mock_order.symbol = "AAPL"
    mock_order.side.value = "buy"
    mock_order.filled_qty = "10"
    mock_order.filled_avg_price = "150.00"
    mock_order.id = "order-abc"
    mock_order.filled_at = datetime.utcnow()

    concrete_strategy.on_order_filled(mock_order)

    with Session(db_engine) as session:
        trade = session.query(Trade).first()
        assert trade is not None
        assert trade.symbol == "AAPL"
        assert trade.alpaca_order_id == "order-abc"

def test_get_metrics_returns_account_info(concrete_strategy):
    mock_account = MagicMock()
    mock_account.equity = "10500.00"
    mock_account.cash = "5000.00"
    mock_account.buying_power = "10000.00"
    concrete_strategy.trading_client.get_account.return_value = mock_account

    metrics = concrete_strategy.get_metrics()

    assert metrics["equity"] == 10500.00
    assert metrics["cash"] == 5000.00
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
python -m pytest tests/test_base_strategy.py -v
```

Expected: `ImportError` (strategies/base.py 없음)

- [ ] **Step 3: strategies/base.py 작성**

```python
import abc
import logging
import threading
from collections import deque
from datetime import datetime, timedelta

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.live import StockDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from sqlalchemy.orm import Session

from db.database import create_engine_for_process
from db.models import Trade


def timeframe_for(run_interval: str) -> TimeFrame:
    """run_interval 문자열 → 프리페치용 Alpaca TimeFrame.

    실시간 스트림 bar는 Alpaca 한계상 항상 1분봉으로 들어오므로, 멀티분/일봉 의사결정은
    전략이 버퍼를 직접 집계해 처리한다. 분 미만(스캘핑)은 현 단계 범위 외(설계 9장).
    """
    return {
        "1m": TimeFrame(1, TimeFrameUnit.Minute),
        "5m": TimeFrame(5, TimeFrameUnit.Minute),
        "15m": TimeFrame(15, TimeFrameUnit.Minute),
        "1h": TimeFrame(1, TimeFrameUnit.Hour),
        "1d": TimeFrame(1, TimeFrameUnit.Day),
    }.get(run_interval, TimeFrame(1, TimeFrameUnit.Minute))


class BaseStrategy(abc.ABC):
    BUFFER_SIZE = 200

    def __init__(self, strategy_id: int, name: str, api_key: str, api_secret: str,
                 budget: float, run_interval: str):
        self.strategy_id = strategy_id
        self.name = name
        self.api_key = api_key
        self.api_secret = api_secret
        self.budget = budget
        self.run_interval = run_interval
        self.logger = logging.getLogger(name)
        # 무거운 자원은 run()/_setup()에서 생성 (fork된 자식 프로세스에서만)
        self.trading_client: TradingClient | None = None
        self.data_stream: StockDataStream | None = None
        self.trade_stream: TradingStream | None = None
        self.engine = None
        self._positions: dict = {}
        self._open_orders: dict = {}
        self._bar_buffer: dict[str, deque] = {}

    def _setup(self) -> None:
        """run() 진입(자식 프로세스) 후 호출. 부모에서 생성하면 소켓/커넥션 FD가 fork로 공유되어 깨짐."""
        self.trading_client = TradingClient(self.api_key, self.api_secret, paper=True)
        self.data_stream = StockDataStream(self.api_key, self.api_secret)
        self.trade_stream = TradingStream(self.api_key, self.api_secret, paper=True)
        self.engine = create_engine_for_process()

    def sync_state(self) -> None:
        positions = self.trading_client.get_all_positions()
        open_orders = self.trading_client.get_orders(
            filter=GetOrdersRequest(status=QueryOrderStatus.OPEN)
        )
        self._positions = {p.symbol: p for p in positions}
        self._open_orders = {str(o.id): o for o in open_orders}

    def _prefetch_bars(self, symbols: list[str]) -> None:
        hist_client = StockHistoricalDataClient(self.api_key, self.api_secret)
        for symbol in symbols:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=timeframe_for(self.run_interval),
                start=datetime.utcnow() - timedelta(days=5),
                limit=self.BUFFER_SIZE,
            )
            df = hist_client.get_stock_bars(request).df
            self._bar_buffer[symbol] = deque(df["close"].tolist(), maxlen=self.BUFFER_SIZE)
            self.logger.info(f"Prefetched {len(self._bar_buffer[symbol])} bars for {symbol}")

    @abc.abstractmethod
    def select_symbols(self) -> list[str]:
        ...

    @abc.abstractmethod
    def on_bar(self, bar) -> None:
        """버퍼 + self._positions 캐시로 판단. ⚠️ 매 bar마다 REST(get_all_positions) 호출 금지."""
        ...

    def on_order_filled(self, order) -> None:
        with Session(self.engine) as session:
            session.add(Trade(
                strategy_id=self.strategy_id,
                symbol=order.symbol,
                side=order.side.value,
                qty=float(order.filled_qty),
                price=float(order.filled_avg_price),
                alpaca_order_id=str(order.id),
                filled_at=order.filled_at,
            ))
            session.commit()
        # 포지션 캐시 경량 갱신 (다음 bar 판단에 반영, REST 재호출 없이)
        if order.side.value == "buy":
            self._positions[order.symbol] = order
        else:
            self._positions.pop(order.symbol, None)

    def get_metrics(self) -> dict:
        account = self.trading_client.get_account()
        return {
            "equity": float(account.equity),
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
        }

    async def _bar_handler(self, bar) -> None:
        try:
            if bar.symbol in self._bar_buffer:
                self._bar_buffer[bar.symbol].append(float(bar.close))
            self.on_bar(bar)  # 핸들러 예외가 스트림을 죽이지 않도록 격리
        except Exception:
            self.logger.exception(f"on_bar failed for {getattr(bar, 'symbol', '?')}")

    async def _trade_update_handler(self, data) -> None:
        try:
            if data.event in ("fill", "partial_fill"):
                self.on_order_filled(data.order)
        except Exception:
            self.logger.exception("trade update handler failed")

    def run(self) -> None:
        logging.basicConfig(
            filename=f"logs/{self.name}.log",
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
        self._setup()
        self.sync_state()
        symbols = self.select_symbols()
        self._prefetch_bars(symbols)

        # 체결 스트림(TradingStream)은 별도 스레드에서 자체 이벤트 루프로 구동
        self.trade_stream.subscribe_trade_updates(self._trade_update_handler)
        threading.Thread(target=self.trade_stream.run, daemon=True).start()

        # 시세 스트림(StockDataStream)은 메인 스레드에서 구동 (블로킹)
        self.data_stream.subscribe_bars(self._bar_handler, *symbols)
        self.data_stream.run()
```

- [ ] **Step 4: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/test_base_strategy.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add strategies/base.py tests/test_base_strategy.py
git commit -m "Feat: add BaseStrategy (run()-scoped resources, TradingStream fills, position cache)"
```

---

## Task 4: MA 크로스오버 예시 전략

**Files:**
- Create: `strategies/ma_crossover.py`
- Test: `tests/test_ma_crossover.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_ma_crossover.py`:
```python
import pytest
import pandas as pd
from collections import deque
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from db.database import Base

@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    return e

@pytest.fixture
def strategy(db_engine):
    from strategies.ma_crossover import MACrossoverStrategy
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.StockDataStream"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=db_engine):
        s = MACrossoverStrategy(
            strategy_id=1, name="ma_crossover",
            api_key="key", api_secret="secret",
            budget=10000.0, run_interval="1m",
        )
        s._setup()  # trading_client 등을 mock으로 생성
    return s

def _make_buffer(trend: str) -> deque:
    # SHORT_WINDOW=10, LONG_WINDOW=30 기준
    # 30개 100.0 + 마지막 1개로 크로스 생성
    closes = [100.0] * 30
    if trend == "golden_cross":
        closes.append(200.0)   # short_ma 급등 → golden cross
    elif trend == "death_cross":
        closes.append(0.0)     # short_ma 급락 → death cross
    else:
        closes.append(100.0)   # 변화 없음 → no cross
    return deque(closes, maxlen=200)

def test_select_symbols_returns_list(strategy):
    symbols = strategy.select_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0

def test_on_bar_buys_on_golden_cross(strategy):
    strategy._positions = {}
    strategy.trading_client.get_all_positions.return_value = []
    strategy._bar_buffer["AAPL"] = _make_buffer("golden_cross")

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 200.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.submit_order.assert_called_once()
    order_req = strategy.trading_client.submit_order.call_args[0][0]
    assert order_req.side.value == "buy"

def test_on_bar_sells_on_death_cross(strategy):
    mock_pos = MagicMock()
    mock_pos.symbol = "AAPL"
    strategy._positions = {"AAPL": mock_pos}
    strategy.trading_client.get_all_positions.return_value = [mock_pos]
    strategy._bar_buffer["AAPL"] = _make_buffer("death_cross")

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 0.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.close_position.assert_called_once_with("AAPL")

def test_on_bar_no_signal_no_order(strategy):
    strategy._positions = {}
    strategy.trading_client.get_all_positions.return_value = []
    strategy._bar_buffer["AAPL"] = _make_buffer("flat")

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 100.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.submit_order.assert_not_called()
    strategy.trading_client.close_position.assert_not_called()

def test_on_bar_skips_when_buffer_too_small(strategy):
    strategy._bar_buffer["AAPL"] = deque([100.0] * 10, maxlen=200)
    strategy.trading_client.get_all_positions.return_value = []

    mock_bar = MagicMock()
    mock_bar.symbol = "AAPL"
    mock_bar.close = 100.0
    strategy.on_bar(mock_bar)

    strategy.trading_client.submit_order.assert_not_called()
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
python -m pytest tests/test_ma_crossover.py -v
```

Expected: `ImportError` (strategies/ma_crossover.py 없음)

- [ ] **Step 3: strategies/ma_crossover.py 작성**

```python
import pandas as pd
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.base import BaseStrategy


class MACrossoverStrategy(BaseStrategy):
    SHORT_WINDOW = 10
    LONG_WINDOW = 30
    WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]
    POSITION_SIZE = 0.2

    def select_symbols(self) -> list[str]:
        return self.WATCHLIST

    def on_bar(self, bar) -> None:
        symbol = bar.symbol
        buffer = self._bar_buffer.get(symbol)
        if buffer is None or len(buffer) < self.LONG_WINDOW + 1:
            return

        closes = pd.Series(list(buffer))
        short_ma = closes.rolling(self.SHORT_WINDOW).mean()
        long_ma = closes.rolling(self.LONG_WINDOW).mean()

        is_golden_cross = (short_ma.iloc[-1] > long_ma.iloc[-1] and
                           short_ma.iloc[-2] <= long_ma.iloc[-2])
        is_death_cross = (short_ma.iloc[-1] < long_ma.iloc[-1] and
                          short_ma.iloc[-2] >= long_ma.iloc[-2])

        # 보유 여부는 캐시로 판단 (매 bar REST 호출 금지 — rate limit 방지).
        # 캐시는 sync_state()와 체결 이벤트(on_order_filled)로 갱신됨.
        has_position = symbol in self._positions

        if is_golden_cross and not has_position:
            qty = int(self.budget * self.POSITION_SIZE / float(bar.close))
            if qty > 0:
                self.trading_client.submit_order(
                    MarketOrderRequest(
                        symbol=symbol, qty=qty,
                        side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
                    )
                )
                self.logger.info(f"BUY {qty} {symbol} @ {bar.close}")

        elif is_death_cross and has_position:
            self.trading_client.close_position(symbol)
            self.logger.info(f"SELL all {symbol}")
```

- [ ] **Step 4: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/test_ma_crossover.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add strategies/ma_crossover.py tests/test_ma_crossover.py
git commit -m "Feat: add MA crossover strategy with rolling buffer"
```

---

## Task 5: Strategy Manager

**Files:**
- Create: `manager/manager.py`
- Test: `tests/test_manager.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_manager.py`:
```python
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
    with patch("manager.manager.create_engine_for_process", return_value=db_engine):
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
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
python -m pytest tests/test_manager.py -v
```

Expected: `ImportError` (manager/manager.py 없음)

- [ ] **Step 3: manager/manager.py 작성**

```python
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
```

- [ ] **Step 4: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/test_manager.py -v
```

Expected: `8 passed`

- [ ] **Step 5: Commit**

```bash
git add manager/manager.py tests/test_manager.py
git commit -m "Feat: add StrategyManager (lock, restart-count, idempotency, daily_performance)"
```

---

## Task 6: FastAPI 서버

**Files:**
- Create: `api/schemas.py`
- Create: `api/main.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_api.py`:
```python
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.database import Base
from db.models import Strategy, Trade, PortfolioHistory, DailyPerformance
from decimal import Decimal
from datetime import datetime, date

@pytest.fixture
def db_engine():
    e = create_engine("sqlite:///:memory:")
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
```

- [ ] **Step 2: 테스트 실행 (실패 확인)**

```bash
python -m pytest tests/test_api.py -v
```

Expected: `ImportError` (api/schemas.py, api/main.py 없음)

- [ ] **Step 3: api/schemas.py 작성**

```python
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
```

- [ ] **Step 4: api/main.py 작성**

```python
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
```

- [ ] **Step 5: 테스트 실행 (통과 확인)**

```bash
python -m pytest tests/test_api.py -v
```

Expected: `7 passed`

- [ ] **Step 6: Commit**

```bash
git add api/schemas.py api/main.py tests/test_api.py
git commit -m "Feat: add FastAPI with lifespan-managed StrategyManager"
```

---

## Task 7: 진입점 및 통합 확인

**Files:**
- Create: `main.py`

- [ ] **Step 1: 전체 테스트 통과 확인**

```bash
python -m pytest tests/ -v --tb=short
```

Expected: 전체 테스트 통과

- [ ] **Step 2: PostgreSQL DB 테이블 생성**

```bash
cp .env.example .env
# .env 파일에 실제 DB 연결 정보 입력 후:
python db/init_db.py
```

Expected: `Tables created.`

- [ ] **Step 3: 첫 번째 전략 DB에 등록**

```python
# 터미널에서 python 실행 (한 번만)
from db.database import create_engine_for_process
from db.models import Strategy
from sqlalchemy.orm import Session
from decimal import Decimal

engine = create_engine_for_process()
with Session(engine) as session:
    session.add(Strategy(
        name="MA 크로스오버 v1",
        strategy_type="ma_crossover",
        alpaca_key="YOUR_PAPER_API_KEY",
        alpaca_secret="YOUR_PAPER_SECRET",
        budget=Decimal("10000"),
        status="running",
        run_interval="1m",
    ))
    session.commit()
    print("Strategy registered.")
```

- [ ] **Step 4: main.py 작성**

```python
import uvicorn

if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)
```

- [ ] **Step 5: 실행 확인**

```bash
python main.py
```

Expected:
- StrategyManager 시작 로그 출력
- FastAPI 서버가 `http://localhost:8000` 에서 실행
- `curl http://localhost:8000/strategies` → 전략 목록 JSON 반환

- [ ] **Step 6: API 문서 확인**

브라우저에서 `http://localhost:8000/docs` 접속 → 6개 엔드포인트 확인

- [ ] **Step 7: Commit**

```bash
git add main.py
git commit -m "Feat: add entry point"
```

---

## 전체 테스트 실행

```bash
python -m pytest tests/ -v --tb=short
```

Expected: 전체 테스트 통과

---

## 새 전략 추가 방법

1. `strategies/` 폴더에 `my_strategy.py` 파일 생성
2. `BaseStrategy`를 상속하고 `select_symbols()`, `on_bar()` 구현
3. DB에 전략 등록 (`strategy_type="my_strategy"`, `name="표시할 이름"`)
4. `POST /strategies/{id}/start` 호출 → 즉시 프로세스 실행 (재시작 불필요)
