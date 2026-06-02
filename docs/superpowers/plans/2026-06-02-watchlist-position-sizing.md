# 감시 종목·포지션 크기 설정화 + 비상 청산 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 봇 4개의 감시 종목(공통)과 포지션 크기(봇별)를 코드 하드코딩에서 런타임 API 변경으로 바꾸고, 폭락 대비 비상 수동 청산 기능을 추가한다.

**Architecture:** 감시 종목은 신규 `watchlist` 표(공통)에 저장하고 봇이 기동 시 읽는다(빈 표→기본 5종목 폴백). 포지션 크기는 `strategies` 표에 `position_size` 컬럼을 추가해 봇별로 둔다. 종목/포지션 변경과 청산은 모두 API에서 받아 `StrategyManager`가 프로세스 제어·알파카 직접 주문으로 처리한다.

**Tech Stack:** Python, FastAPI, SQLAlchemy(ORM), Postgres(운영)/SQLite(테스트), alpaca-py, pytest, multiprocessing.

---

## 파일 구조

- `db/models.py` — `Watchlist` 모델 추가, `Strategy.position_size` 컬럼 추가
- `db/watchlist.py` *(신규)* — `DEFAULT_WATCHLIST`, `get_watchlist_symbols`, `replace_watchlist`
- `strategies/base.py` — 생성자에 `symbols`/`position_size` 주입, `select_symbols` 구체화
- `strategies/ma_crossover.py`, `rsi_reversion.py`, `macd.py`, `bollinger.py` — 클래스 상수 `WATCHLIST`/`POSITION_SIZE`·`select_symbols` 제거, `self.position_size` 사용
- `manager/manager.py` — `_launch_process`가 watchlist·position_size 사용, `liquidate_strategy`/`liquidate_all`/`apply_watchlist` 추가
- `api/schemas.py` — `WatchlistResponse`/`WatchlistUpdate`/`StrategyUpdate`, `StrategyResponse.position_size`
- `api/main.py` — `GET`/`PUT /watchlist`, `PATCH /strategies/{id}`, 비상 청산 3종, `validate_symbols`
- `scripts/migrate_add_position_size.py` *(신규)* — 기존 DB용 컬럼 추가 마이그레이션
- 테스트: `tests/test_watchlist_db.py`*(신규)*, 기존 `tests/test_manager.py`·`tests/test_api.py`·`tests/test_base_strategy.py` 확장

---

### Task 1: `watchlist` DB 모델 + 헬퍼 모듈

**Files:**
- Modify: `db/models.py`
- Create: `db/watchlist.py`
- Test: `tests/test_watchlist_db.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_watchlist_db.py`

```python
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
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_watchlist_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'db.watchlist'`

- [ ] **Step 3: 모델 추가** — `db/models.py`의 `Trade` 클래스 정의 위(또는 `Strategy` 아래)에 추가

```python
class Watchlist(Base):
    __tablename__ = "watchlist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
```

- [ ] **Step 4: 헬퍼 모듈 작성** — `db/watchlist.py`

```python
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
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/test_watchlist_db.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: 커밋**

```bash
git add db/models.py db/watchlist.py tests/test_watchlist_db.py
git commit -m "feat: add watchlist table and symbol helper with default fallback"
```

---

### Task 2: `strategies` 표에 `position_size` 컬럼 + 마이그레이션

**Files:**
- Modify: `db/models.py:9-24` (`Strategy` 클래스)
- Create: `scripts/migrate_add_position_size.py`
- Test: `tests/test_watchlist_db.py` (컬럼 기본값 테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_watchlist_db.py` 끝에 추가

```python
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
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_watchlist_db.py::test_strategy_position_size_defaults_to_point_two -v`
Expected: FAIL — `AttributeError: ... has no attribute 'position_size'` 또는 컬럼 없음

- [ ] **Step 3: 컬럼 추가** — `db/models.py` `Strategy` 클래스의 `run_interval` 줄 아래에 추가

```python
    position_size: Mapped[Decimal] = mapped_column(Numeric(4, 3), nullable=False, default=Decimal("0.2"))
```

- [ ] **Step 4: 마이그레이션 스크립트 작성** — `scripts/migrate_add_position_size.py`

```python
"""기존 운영 DB의 strategies 표에 position_size 컬럼을 추가하는 일회성 마이그레이션.

신규 watchlist 표는 `python -m db.init_db`(create_all)로 생성된다.
이 스크립트는 기존 표에 컬럼을 더하는 것만 담당한다 (create_all은 컬럼 추가를 못 함).

사용: python -m scripts.migrate_add_position_size
"""
from sqlalchemy import text

from db.database import create_engine_for_process


def main() -> None:
    engine = create_engine_for_process()
    with engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS "
            "position_size NUMERIC(4,3) NOT NULL DEFAULT 0.2"
        ))
    print("position_size column ensured.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/test_watchlist_db.py -v`
Expected: PASS (4 passed)

- [ ] **Step 6: 커밋**

```bash
git add db/models.py scripts/migrate_add_position_size.py tests/test_watchlist_db.py
git commit -m "feat: add per-strategy position_size column with migration"
```

---

### Task 3: `BaseStrategy`에 `symbols`/`position_size` 주입

**Files:**
- Modify: `strategies/base.py:38-55` (생성자), `:88-90` (`select_symbols`)
- Test: `tests/test_base_strategy.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_base_strategy.py` 끝에 추가

```python
def test_injected_symbols_and_position_size_used():
    from unittest.mock import patch
    from sqlalchemy import create_engine
    from db.database import Base
    from strategies.ma_crossover import MACrossoverStrategy
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=e):
        s = MACrossoverStrategy(
            strategy_id=1, name="t", api_key="k", api_secret="s",
            budget=10000.0, run_interval="1m",
            symbols=["TSLA", "AMD"], position_size=0.1,
        )
    assert s.select_symbols() == ["TSLA", "AMD"]
    assert s.position_size == 0.1


def test_missing_symbols_falls_back_to_default():
    from unittest.mock import patch
    from sqlalchemy import create_engine
    from db.database import Base
    from db.watchlist import DEFAULT_WATCHLIST
    from strategies.ma_crossover import MACrossoverStrategy
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    with patch("strategies.base.TradingClient"), \
         patch("strategies.base.TradingStream"), \
         patch("strategies.base.create_engine_for_process", return_value=e):
        s = MACrossoverStrategy(
            strategy_id=1, name="t", api_key="k", api_secret="s",
            budget=10000.0, run_interval="1m",
        )
    assert s.select_symbols() == DEFAULT_WATCHLIST
    assert s.position_size == 0.2
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_base_strategy.py::test_injected_symbols_and_position_size_used -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'symbols'`

- [ ] **Step 3: 생성자 수정** — `strategies/base.py` 상단 import에 추가

```python
from db.watchlist import DEFAULT_WATCHLIST
```

그리고 `__init__` 시그니처와 본문 수정 (`bar_queue=None` 다음에 인자 추가, `self.bar_queue` 줄 근처에 속성 추가):

```python
    def __init__(self, strategy_id: int, name: str, api_key: str, api_secret: str,
                 budget: float, run_interval: str, bar_queue=None,
                 symbols=None, position_size=None):
        self.strategy_id = strategy_id
        self.name = name
        self.api_key = api_key
        self.api_secret = api_secret
        self.budget = budget
        self.run_interval = run_interval
        # 시세는 MarketDataHub(중앙 1연결)가 이 큐로 분배한다. 전략은 자체 시세 연결을 열지 않음.
        self.bar_queue = bar_queue
        # 감시 종목/포지션 크기는 매니저가 DB에서 읽어 주입. 미주입 시 안전 폴백.
        self.symbols = list(symbols) if symbols else list(DEFAULT_WATCHLIST)
        self.position_size = position_size if position_size else 0.2
        self.logger = logging.getLogger(name)
```

- [ ] **Step 4: `select_symbols` 구체화** — `strategies/base.py`의 추상 `select_symbols`를 구체 메서드로 교체

```python
    def select_symbols(self) -> list[str]:
        return self.symbols
```

(`@abc.abstractmethod` 데코레이터와 `...` 본문을 제거. `on_bar`의 추상 선언은 그대로 둔다.)

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/test_base_strategy.py -v`
Expected: PASS (신규 2건 포함 전부 통과)

- [ ] **Step 6: 커밋**

```bash
git add strategies/base.py tests/test_base_strategy.py
git commit -m "feat: inject symbols and position_size into BaseStrategy"
```

---

### Task 4: 4개 전략에서 하드코딩 제거, `self.position_size` 사용

**Files:**
- Modify: `strategies/ma_crossover.py`, `strategies/rsi_reversion.py`, `strategies/macd.py`, `strategies/bollinger.py`
- Test: 기존 `tests/test_ma_crossover.py`, `test_rsi_reversion.py`, `test_macd.py`, `test_bollinger.py` (변경 없이 통과해야 함)

- [ ] **Step 1: 기존 전략 테스트가 통과 상태인지 먼저 확인 (베이스라인)**

Run: `pytest tests/test_ma_crossover.py tests/test_rsi_reversion.py tests/test_macd.py tests/test_bollinger.py -v`
Expected: PASS (현재 코드 기준)

- [ ] **Step 2: `ma_crossover.py` 수정** — `WATCHLIST`, `POSITION_SIZE`, `select_symbols` 제거하고 매수 수량에서 `self.position_size` 사용

클래스 본문 상단을 다음으로:

```python
class MACrossoverStrategy(BaseStrategy):
    SHORT_WINDOW = 10
    LONG_WINDOW = 30

    def on_bar(self, bar) -> None:
```

매수 수량 줄을 변경:

```python
            qty = int(self.budget * self.position_size / float(bar.close))
```

(`WATCHLIST = [...]`, `POSITION_SIZE = 0.2`, `def select_symbols` 블록 삭제. `import pandas as pd` 등 나머지는 유지.)

- [ ] **Step 3: `rsi_reversion.py` 수정** — 동일하게 `WATCHLIST`/`POSITION_SIZE`/`select_symbols` 제거, 매수 수량 줄 변경

```python
            qty = int(self.budget * self.position_size / float(bar.close))
```

- [ ] **Step 4: `macd.py` 수정** — 동일하게 제거, 매수 수량 줄 변경

```python
            qty = int(self.budget * self.position_size / float(bar.close))
```

- [ ] **Step 5: `bollinger.py` 수정** — 동일하게 제거. 단, 볼린저는 `close` 지역변수를 쓰므로 매수 수량 줄을 변경

```python
                qty = int(self.budget * self.position_size / close)
```

- [ ] **Step 6: 전략 테스트 통과 확인 (기존 테스트 무변경 통과 = 회귀 없음)**

Run: `pytest tests/test_ma_crossover.py tests/test_rsi_reversion.py tests/test_macd.py tests/test_bollinger.py -v`
Expected: PASS — 기존 테스트는 `position_size` 미주입 → 0.2 폴백 → 수량 계산 동일

- [ ] **Step 7: 커밋**

```bash
git add strategies/ma_crossover.py strategies/rsi_reversion.py strategies/macd.py strategies/bollinger.py
git commit -m "refactor: strategies read symbols/position_size from base instead of hardcoded"
```

---

### Task 5: 매니저 `_launch_process`가 watchlist·position_size 사용

**Files:**
- Modify: `manager/manager.py:108-133` (`_launch_process`), 상단 import
- Test: `tests/test_manager.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_manager.py`에 추가

```python
def test_launch_process_uses_db_watchlist(manager, running_strategy, db_engine):
    from sqlalchemy.orm import Session
    from db.watchlist import replace_watchlist
    with Session(db_engine) as session:
        replace_watchlist(session, ["TSLA", "AMD"])
        session.commit()

    with patch("manager.manager.multiprocessing.Process"), \
         patch.object(manager, "engine", db_engine):
        with Session(db_engine) as session:
            st = session.get(Strategy, 1)
            manager._launch_process(st)

    _sid, symbols, _q = manager.hub.add_strategy.call_args[0]
    assert symbols == ["TSLA", "AMD"]
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_manager.py::test_launch_process_uses_db_watchlist -v`
Expected: FAIL — symbols는 기본 5종목(`["AAPL", ...]`)이라 `["TSLA","AMD"]`와 불일치

- [ ] **Step 3: import 추가** — `manager/manager.py` 상단

```python
from db.watchlist import get_watchlist_symbols
```

- [ ] **Step 4: `_launch_process` 수정** — `cls = self._load_strategy_class(...)` 다음에 watchlist 조회를 추가하고, 인스턴스 생성에 `symbols`/`position_size` 전달

```python
    def _launch_process(self, strategy: Strategy, restart_count: int = 0) -> None:
        # 항상 _lock 하에서 호출됨 (start/start_strategy/_monitor_crashes)
        cls = self._load_strategy_class(strategy.strategy_type)
        with Session(self.engine) as session:
            symbols = get_watchlist_symbols(session)
        bar_queue = multiprocessing.Queue()
        instance = cls(
            strategy_id=strategy.id,
            name=strategy.name,
            api_key=strategy.alpaca_key,
            api_secret=strategy.alpaca_secret,
            budget=float(strategy.budget),
            run_interval=strategy.run_interval,
            bar_queue=bar_queue,
            symbols=symbols,
            position_size=float(strategy.position_size),
        )
        # select_symbols는 매니저 프로세스에서도 호출되므로 무거운 자원에 의존하면 안 됨
        symbols = instance.select_symbols()
        proc = multiprocessing.Process(target=instance.run, daemon=True)
        proc.start()
        # restart_count는 재시작 시에도 보존되어야 함 (덮어쓰면 무한 재시작)
        self.processes[strategy.id] = {
            "process": proc, "restart_count": restart_count,
            "queue": bar_queue, "symbols": symbols,
        }
        # 허브에 종목 등록 → 허브가 시세 1연결로 받아 이 큐로 분배
        self.hub.add_strategy(strategy.id, symbols, bar_queue)
        logger.info(f"Launched {strategy.name} (type={strategy.strategy_type}) "
                    f"pid={proc.pid} symbols={symbols}")
```

- [ ] **Step 5: 테스트 통과 확인 (신규 + 기존 매니저 테스트 회귀 없음)**

Run: `pytest tests/test_manager.py -v`
Expected: PASS — 기존 `test_launch_process_registers_with_hub`도 watchlist 비어있어 기본값에 `AAPL` 포함 → 통과

- [ ] **Step 6: 커밋**

```bash
git add manager/manager.py tests/test_manager.py
git commit -m "feat: manager launches strategies with DB watchlist and position_size"
```

---

### Task 6: 매니저 비상 청산 (`liquidate_strategy` / `liquidate_all`)

**Files:**
- Modify: `manager/manager.py` (메서드 추가)
- Test: `tests/test_manager.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_manager.py`에 추가

```python
def test_liquidate_strategy_stops_and_closes_symbol(manager, running_strategy, db_engine):
    mock_proc = MagicMock()
    manager.processes[1] = {"process": mock_proc, "restart_count": 0, "queue": MagicMock()}

    with patch("manager.manager.TradingClient") as mock_cls, \
         patch.object(manager, "engine", db_engine):
        manager.liquidate_strategy(1, symbol="AAPL")

    mock_cls.return_value.close_position.assert_called_once_with("AAPL")
    mock_proc.terminate.assert_called_once()  # 봇 멈춤
    with Session(db_engine) as session:
        assert session.get(Strategy, 1).status == "stopped"


def test_liquidate_strategy_closes_all_when_no_symbol(manager, running_strategy, db_engine):
    with patch("manager.manager.TradingClient") as mock_cls, \
         patch.object(manager, "engine", db_engine):
        manager.liquidate_strategy(1)

    mock_cls.return_value.close_all_positions.assert_called_once_with(cancel_orders=True)


def test_liquidate_all_liquidates_running(manager, running_strategy, db_engine):
    with patch.object(manager, "liquidate_strategy") as mock_liq, \
         patch.object(manager, "engine", db_engine):
        manager.liquidate_all()

    mock_liq.assert_called_once_with(1)
```

(참고: `tests/test_manager.py` 상단에 `from sqlalchemy.orm import Session`이 이미 import 되어 있음.)

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_manager.py::test_liquidate_strategy_stops_and_closes_symbol -v`
Expected: FAIL — `AttributeError: 'StrategyManager' object has no attribute 'liquidate_strategy'`

- [ ] **Step 3: 메서드 추가** — `manager/manager.py`의 `_load_strategy_class` 아래(또는 클래스 내 적당한 위치)에 추가

```python
    def _make_client(self, strategy) -> TradingClient:
        return TradingClient(strategy.alpaca_key, strategy.alpaca_secret, paper=True)

    def liquidate_strategy(self, strategy_id: int, symbol: str | None = None) -> None:
        """비상 청산: 봇을 멈추고(stopped) 포지션을 판다. symbol=None이면 전체 청산.

        봇이 켜진 채 강제 매도하면 포지션 캐시가 어긋나므로 항상 먼저 멈춘다.
        청산은 봇 프로세스가 아니라 매니저가 직접 주문 → 봇이 죽어있어도 작동.
        """
        with self._lock, Session(self.engine) as session:
            strategy = session.get(Strategy, strategy_id)
            if strategy is None:
                return
            strategy.status = "stopped"
            session.commit()
            self._teardown_process(strategy_id)
            try:
                client = self._make_client(strategy)
                if symbol is None:
                    client.close_all_positions(cancel_orders=True)
                else:
                    client.close_position(symbol)
            except Exception:
                logger.exception(f"liquidate failed strategy={strategy_id} symbol={symbol}")

    def liquidate_all(self) -> None:
        """폭락 비상 버튼: running 전략을 모두 청산 + 정지."""
        with self._lock, Session(self.engine) as session:
            ids = [s.id for s in session.query(Strategy)
                   .filter(Strategy.status == "running").all()]
        for sid in ids:
            self.liquidate_strategy(sid)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/test_manager.py -v`
Expected: PASS (신규 3건 포함)

- [ ] **Step 5: 커밋**

```bash
git add manager/manager.py tests/test_manager.py
git commit -m "feat: add manager liquidate_strategy and liquidate_all (stop + sell)"
```

---

### Task 7: 매니저 `apply_watchlist` (검증 통과 후 적용)

**Files:**
- Modify: `manager/manager.py` (메서드 추가), 상단 import
- Test: `tests/test_manager.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_manager.py`에 추가

```python
def test_apply_watchlist_saves_and_restarts_running(manager, running_strategy, db_engine):
    from db.watchlist import get_watchlist_symbols
    alive = MagicMock()
    alive.is_alive.return_value = True
    manager.processes[1] = {"process": alive, "restart_count": 0, "queue": MagicMock()}

    with patch.object(manager, "_teardown_process") as mock_teardown, \
         patch.object(manager, "_launch_process") as mock_launch, \
         patch("manager.manager.TradingClient"), \
         patch.object(manager, "engine", db_engine):
        manager.apply_watchlist(["TSLA", "AMD"])

    with Session(db_engine) as session:
        assert get_watchlist_symbols(session) == ["TSLA", "AMD"]
    mock_teardown.assert_called_once_with(1)   # 돌던 봇 정지
    mock_launch.assert_called_once()           # 새 종목으로 재시작


def test_apply_watchlist_liquidates_removed_symbols(manager, running_strategy, db_engine):
    from db.watchlist import replace_watchlist
    with Session(db_engine) as session:
        replace_watchlist(session, ["AAPL", "MSFT"])
        session.commit()

    held = MagicMock()
    held.symbol = "AAPL"  # 봇이 AAPL 보유 중

    with patch.object(manager, "_teardown_process"), \
         patch.object(manager, "_launch_process"), \
         patch("manager.manager.TradingClient") as mock_cls, \
         patch.object(manager, "engine", db_engine):
        mock_cls.return_value.get_all_positions.return_value = [held]
        manager.apply_watchlist(["MSFT"])  # AAPL 제거

    mock_cls.return_value.close_position.assert_called_once_with("AAPL")
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_manager.py::test_apply_watchlist_saves_and_restarts_running -v`
Expected: FAIL — `AttributeError: ... has no attribute 'apply_watchlist'`

- [ ] **Step 3: import 보강** — `manager/manager.py` 상단의 watchlist import를 확장

```python
from db.watchlist import get_watchlist_symbols, replace_watchlist
```

- [ ] **Step 4: 메서드 추가** — `manager/manager.py` `liquidate_all` 아래에 추가

```python
    def apply_watchlist(self, symbols: list[str]) -> None:
        """검증 통과한 새 종목 목록 적용: 돌던 봇 정지 → 빠진 종목 청산 → 저장 → 재시작.

        호출 전 API가 알파카 검증을 끝낸 상태여야 한다.
        """
        with self._lock, Session(self.engine) as session:
            removed = set(get_watchlist_symbols(session)) - set(symbols)
            running_ids = [sid for sid, info in list(self.processes.items())
                           if info["process"].is_alive()]

            # 1) 돌던 봇 정지
            for sid in running_ids:
                st = session.get(Strategy, sid)
                if st:
                    st.status = "stopped"
                self._teardown_process(sid)
            session.commit()

            # 2) 빠진 종목을 보유한 모든 전략에서 청산 (봇별 격리)
            if removed:
                for strategy in session.query(Strategy).all():
                    try:
                        client = self._make_client(strategy)
                        held = {p.symbol for p in client.get_all_positions()}
                        for sym in removed & held:
                            client.close_position(sym)
                    except Exception:
                        logger.exception(f"watchlist liquidation failed strategy={strategy.id}")

            # 3) 새 목록 저장
            replace_watchlist(session, symbols)
            session.commit()

            # 4) 직전에 돌던 봇 재시작 (새 종목 반영)
            for sid in running_ids:
                st = session.get(Strategy, sid)
                if st:
                    st.status = "running"
                    session.commit()
                    self._launch_process(st)
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/test_manager.py -v`
Expected: PASS (신규 2건 포함)

- [ ] **Step 6: 커밋**

```bash
git add manager/manager.py tests/test_manager.py
git commit -m "feat: add manager apply_watchlist (stop, liquidate removed, save, restart)"
```

---

### Task 8: API 스키마 추가

**Files:**
- Modify: `api/schemas.py`
- Test: (Task 9~11에서 엔드포인트와 함께 검증)

- [ ] **Step 1: 스키마 추가** — `api/schemas.py`

`StrategyResponse`에 `position_size` 필드 추가 (`run_interval` 아래):

```python
    position_size: Decimal
```

파일 끝에 새 스키마 추가:

```python
class WatchlistResponse(BaseModel):
    symbols: list[str]


class WatchlistUpdate(BaseModel):
    symbols: list[str]


class StrategyUpdate(BaseModel):
    position_size: Optional[Decimal] = None
```

- [ ] **Step 2: import 호환 확인 (구문 오류 없음 확인)**

Run: `python -c "import api.schemas"`
Expected: 출력 없음(에러 없음)

- [ ] **Step 3: 커밋**

```bash
git add api/schemas.py
git commit -m "feat: add watchlist and strategy-update API schemas"
```

---

### Task 9: API `GET`/`PUT /watchlist` + 알파카 검증

**Files:**
- Modify: `api/main.py` (import, `validate_symbols`, 엔드포인트 2개)
- Test: `tests/test_api.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_api.py`에 추가

```python
def test_get_watchlist_returns_default_when_empty(client, seeded_db):
    resp = client.get("/watchlist")
    assert resp.status_code == 200
    assert resp.json()["symbols"] == ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]


def test_put_watchlist_validates_and_applies(client, seeded_db, mock_mgr):
    with patch("api.main.TradingClient") as mock_cls:
        asset = MagicMock()
        asset.tradable = True
        asset.status.value = "active"
        mock_cls.return_value.get_asset.return_value = asset
        resp = client.put("/watchlist", json={"symbols": ["tsla", "amd"]})

    assert resp.status_code == 200
    assert resp.json()["symbols"] == ["TSLA", "AMD"]   # 대문자 정규화
    mock_mgr.apply_watchlist.assert_called_once_with(["TSLA", "AMD"])


def test_put_watchlist_rejects_invalid_symbol(client, seeded_db, mock_mgr):
    with patch("api.main.TradingClient") as mock_cls:
        asset = MagicMock()
        asset.tradable = False           # 거래 불가
        asset.status.value = "inactive"
        mock_cls.return_value.get_asset.return_value = asset
        resp = client.put("/watchlist", json={"symbols": ["BADX"]})

    assert resp.status_code == 400
    mock_mgr.apply_watchlist.assert_not_called()   # 저장/적용 안 함


def test_put_watchlist_rejects_empty(client, seeded_db, mock_mgr):
    resp = client.put("/watchlist", json={"symbols": []})
    assert resp.status_code == 400
    mock_mgr.apply_watchlist.assert_not_called()
```

(검증용 `ALPACA_KEY` 환경변수: 테스트는 `api.main.TradingClient`를 mock 하므로 실제 키 불필요. 단, `validate_symbols`가 키 미설정 시 503을 내지 않도록 테스트 상단에서 보장한다 — 다음 줄을 파일 맨 위 `os.environ.setdefault("BOT_API_TOKEN", "")` 아래에 추가:)

```python
os.environ.setdefault("ALPACA_KEY", "test")
os.environ.setdefault("ALPACA_SECRET", "test")
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_api.py::test_get_watchlist_returns_default_when_empty -v`
Expected: FAIL — 404 (라우트 없음)

- [ ] **Step 3: import 추가** — `api/main.py` 상단

```python
from db.watchlist import get_watchlist_symbols
from api.schemas import (StrategyResponse, TradeResponse,
                         PortfolioHistoryResponse, DailyPerformanceResponse,
                         PositionResponse, WatchlistResponse, WatchlistUpdate,
                         StrategyUpdate)
```

(기존 `from api.schemas import (...)` 줄을 위 내용으로 교체.)

- [ ] **Step 4: 검증 헬퍼 + 엔드포인트 추가** — `api/main.py`의 `stop_strategy` 엔드포인트 아래에 추가

```python
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
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/test_api.py -v`
Expected: PASS (신규 4건 포함, 기존 회귀 없음)

- [ ] **Step 6: 커밋**

```bash
git add api/main.py tests/test_api.py
git commit -m "feat: add GET/PUT /watchlist with Alpaca symbol validation"
```

---

### Task 10: API `PATCH /strategies/{id}` (포지션 크기 변경)

**Files:**
- Modify: `api/main.py` (엔드포인트 추가)
- Test: `tests/test_api.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_api.py`에 추가

```python
def test_patch_strategy_updates_position_size(client, seeded_db):
    resp = client.patch("/strategies/1", json={"position_size": 0.1})
    assert resp.status_code == 200
    assert float(resp.json()["position_size"]) == 0.1


def test_patch_strategy_rejects_out_of_range(client, seeded_db):
    resp = client.patch("/strategies/1", json={"position_size": 1.5})
    assert resp.status_code == 400


def test_patch_strategy_not_found(client, seeded_db):
    resp = client.patch("/strategies/999", json={"position_size": 0.1})
    assert resp.status_code == 404
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_api.py::test_patch_strategy_updates_position_size -v`
Expected: FAIL — 405 Method Not Allowed (PATCH 라우트 없음)

- [ ] **Step 3: 엔드포인트 추가** — `api/main.py`의 `list_strategies` 아래에 추가

```python
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
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/test_api.py -v`
Expected: PASS (신규 3건 포함)

- [ ] **Step 5: 커밋**

```bash
git add api/main.py tests/test_api.py
git commit -m "feat: add PATCH /strategies/{id} to update position_size"
```

---

### Task 11: API 비상 청산 엔드포인트 3종

**Files:**
- Modify: `api/main.py` (엔드포인트 3개)
- Test: `tests/test_api.py`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_api.py`에 추가

```python
def test_close_one_position_calls_manager(client, seeded_db, mock_mgr):
    resp = client.post("/strategies/1/positions/aapl/close")
    assert resp.status_code == 200
    mock_mgr.liquidate_strategy.assert_called_once_with(1, symbol="AAPL")


def test_liquidate_strategy_calls_manager(client, seeded_db, mock_mgr):
    resp = client.post("/strategies/1/liquidate")
    assert resp.status_code == 200
    mock_mgr.liquidate_strategy.assert_called_once_with(1)


def test_liquidate_all_calls_manager(client, seeded_db, mock_mgr):
    resp = client.post("/liquidate-all")
    assert resp.status_code == 200
    mock_mgr.liquidate_all.assert_called_once_with()


def test_close_one_position_strategy_not_found(client, seeded_db, mock_mgr):
    resp = client.post("/strategies/999/positions/AAPL/close")
    assert resp.status_code == 404
    mock_mgr.liquidate_strategy.assert_not_called()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/test_api.py::test_liquidate_all_calls_manager -v`
Expected: FAIL — 404 (라우트 없음)

- [ ] **Step 3: 엔드포인트 추가** — `api/main.py` 끝에 추가

```python
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
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/test_api.py -v`
Expected: PASS (신규 4건 포함)

- [ ] **Step 5: 커밋**

```bash
git add api/main.py tests/test_api.py
git commit -m "feat: add emergency liquidation endpoints (symbol/strategy/all)"
```

---

### Task 12: 종목 변경 문서화 + 전체 테스트 + register 스크립트 보강

**Files:**
- Modify: `scripts/register_strategy.py` (`--position-size` 인자)
- Modify: `docs/strategies.md` (감시 종목·포지션 크기 설정 안내 추가)

- [ ] **Step 1: `register_strategy.py`에 `--position-size` 추가** — argparse 인자와 Strategy 생성에 반영

argparse 인자 추가 (`--interval` 아래):

```python
    parser.add_argument("--position-size", type=Decimal, default=Decimal("0.2"),
                        help="종목당 예산 비중 (0~1, 기본 0.2)")
```

`Strategy(...)` 생성에 추가 (`run_interval=args.interval,` 아래):

```python
            position_size=args.position_size,
```

- [ ] **Step 2: `docs/strategies.md` 갱신** — 문서 상단 "공통 포지션 크기" 줄을 다음으로 교체

```markdown
공통 감시 종목: DB `watchlist` 표에서 관리 (비어있으면 기본 `AAPL, MSFT, NVDA, TSLA, GOOGL`). `GET`/`PUT /watchlist`로 조회·변경하며, 변경 시 빠진 종목은 자동 청산되고 돌던 봇은 자동 재시작된다.
포지션 크기: 봇별 `strategies.position_size` (기본 0.2 = 예산의 20%). `PATCH /strategies/{id}`로 변경.
비상 청산: `POST /strategies/{id}/positions/{symbol}/close`(종목 1개), `POST /strategies/{id}/liquidate`(봇 전체), `POST /liquidate-all`(전체) — 모두 해당 봇을 멈추고 매도한다.
```

- [ ] **Step 3: 전체 테스트 스위트 실행**

Run: `pytest -v`
Expected: PASS (전부 통과)

- [ ] **Step 4: 커밋**

```bash
git add scripts/register_strategy.py docs/strategies.md
git commit -m "docs: document watchlist/position_size config; add register --position-size"
```

---

## 배포 메모 (운영 DB 적용 순서)

구현 완료 후 운영 환경에서 1회 실행:

1. `python -m db.init_db` — 신규 `watchlist` 표 생성 (create_all은 기존 표 보존, 누락 표만 생성)
2. `python -m scripts.migrate_add_position_size` — 기존 `strategies` 표에 `position_size` 컬럼 추가
3. API 재시작

마이그레이션 전까지는 watchlist 표가 비어 기본 5종목·position_size 0.2로 동작하므로 **현재 거래 동작과 동일**하다.
```
