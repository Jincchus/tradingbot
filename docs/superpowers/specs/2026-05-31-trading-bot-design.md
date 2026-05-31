# Trading Bot 설계 문서

**날짜:** 2026-05-31  
**목표:** Alpaca 페이퍼 트레이딩으로 여러 전략을 동시에 실행하고 성과를 비교하여 최적 전략을 실투자에 적용

---

## 1. 시스템 개요

- 언어: Python 3.11+
- API: Alpaca (페이퍼 트레이딩 계정 전략별 1개씩)
- DB: PostgreSQL
- API 서버: FastAPI (Strategy Manager를 내장)
- 프로세스 모델: 전략마다 독립 Python 프로세스
- 배포: Ubuntu Linux 서버 (기존 웹 프로젝트와 동일 서버)

---

## 2. 아키텍처

```
┌──────────────────────────────────────────────────────────────┐
│                       FastAPI 프로세스                          │
│                                                                │
│  ┌──────────────────────────────────────────────────────┐    │
│  │              Strategy Manager (스레드)                 │    │
│  │  - 프로세스 크래시 감시 (30s) / portfolio (5min)       │    │
│  │  - daily_performance (장마감 16:05 ET)                 │    │
│  └──────────┬──────────────┬────────────────────────────┘    │
│             │              │                                   │
│  ┌──────────▼──────────────────────────────────┐  POST /start │
│  │   MarketDataHub (스레드)                      │  POST /stop  │
│  │   시세용 키로 StockDataStream 1개 연결         │              │
│  │   종목→전략큐 라우팅 (팬아웃)                  │              │
│  └───┬───────────┬───────────┬──────────────────┘              │
│      │ bar_queue │ bar_queue │ bar_queue (multiprocessing.Queue)│
└──────┼───────────┼───────────┼─────────────────────────────────┘
       │           │           │
  ┌────▼───┐  ┌────▼───┐  ┌────▼───┐
  │Strategy│  │Strategy│  │Strategy│  ... (독립 프로세스, 거래만 담당)
  │  A     │  │  B     │  │  C     │
  └────┬───┘  └────┬───┘  └────┬───┘
       │ 거래/체결  │           │  (TradingClient/TradingStream, 전략별 키)
  ┌────▼───┐  ┌────▼───┐  ┌────▼───┐
  │Alpaca  │  │Alpaca  │  │Alpaca  │
  │Paper#1 │  │Paper#2 │  │Paper#3 │   ← 거래용 (계정별 독립)
  └────┬───┘  └────┬───┘  └────┬───┘
       └───────────┼───────────┘
                   ▼
           ┌───────────────┐
           │  PostgreSQL   │ → 기존 웹 프론트엔드 (REST API)
           └───────────────┘
```

**핵심 결정:**
- Manager / MarketDataHub는 FastAPI와 같은 프로세스 내 백그라운드 스레드로 실행
- start/stop API 요청 시 Manager가 직접 subprocess를 생성/종료 (DB 폴링 없음, 즉시 반영)
- **시세/거래 키 분리 (무료 데이터 플랜의 동시 시세 연결 1개 한도 우회):**
  - **시세**: `MarketDataHub`가 **시세용 별도 로그인 키**(`.env`의 `ALPACA_DATA_KEY/SECRET`)로 `StockDataStream` **1개**만 연결 → 받은 bar를 `multiprocessing.Queue`로 각 전략에 분배(팬아웃)
  - **거래/체결**: 각 전략 프로세스가 **전략별 거래 키**(DB `strategies.alpaca_key/secret`)로 `TradingClient`/`TradingStream` 독립 연결 (체결은 계정별이라 한도 무관)
- 각 전략 subprocess는 **`run()` 진입 후** 독립적으로 DB Engine / 거래 클라이언트를 생성 (부모 공유 금지)
  - ⚠️ `__init__`에서 생성하면 안 됨 — Manager(부모) 프로세스에서 만들어진 소켓·커넥션 풀 FD가 fork로 자식과 공유되어 SQLAlchemy/웹소켓이 깨짐
  - 전략은 자체 `StockDataStream`을 열지 않고 `bar_queue`에서 시세를 소비
- `self.processes` 딕셔너리는 APScheduler 스레드와 API 요청 스레드가 동시에 접근하므로 **`threading.Lock`으로 보호**
- ⚠️ MarketDataHub는 단일 장애점 — 허브가 죽으면 전 전략 시세가 끊김 (허브가 매니저=FastAPI 프로세스와 운명을 같이하므로 시스템 다운과 동일 범위)

---

## 3. 핵심 컴포넌트

### BaseStrategy (공통 인터페이스)

모든 전략이 상속하는 추상 클래스. 시작 시 과거 데이터를 백필하여 롤링 버퍼를 채운 뒤 WebSocket 수신.
**무거운 자원(DB Engine, TradingClient, StockDataStream, TradingStream)은 `run()` 안에서 생성**한다.

```python
class BaseStrategy:
    BUFFER_SIZE = 200

    def select_symbols(self) -> list[str]: ...      # 종목 선별
    def on_bar(self, bar) -> None: ...              # bar 수신 시 (버퍼+캐시된 포지션으로 판단)
    def on_order_filled(self, order) -> None: ...   # 체결 이벤트 → trades 테이블 저장 + 포지션 캐시 갱신
    def get_metrics(self) -> dict: ...              # 성과 지표
    def sync_state(self) -> None: ...               # Alpaca 잔고/미체결 주문 → self._positions 캐시
    def _prefetch_bars(self, symbols) -> None: ...  # 시작 시 1회 REST API로 과거 데이터 로드
    def _setup(self) -> None: ...                   # run() 진입 시 클라이언트/Engine/Stream 생성
    def run(self) -> None: ...                      # _setup → sync_state → prefetch → 체결스트림(스레드) + bar_queue 소비
    # self._positions: dict[str, position]          # 보유 포지션 캐시 (REST 재호출 금지, fill/sync로 갱신)
    # self._bar_buffer: dict[str, deque]            # 종목별 close 가격 롤링 버퍼
```

**체결 기록(중요):** 주문 체결은 시세 스트림(`StockDataStream`)이 아니라 **`TradingStream`(trade updates 웹소켓)** 으로 들어온다.
`run()`에서 `TradingStream.subscribe_trade_updates(handler)` 를 등록하고, `fill`/`partial_fill` 이벤트 수신 시 `on_order_filled()`를
호출해 `trades` 테이블에 저장하고 `self._positions` 캐시를 갱신한다. 시세는 `MarketDataHub`가 `bar_queue`로 분배하므로,
전략은 거래 스트림(별도 스레드)과 `bar_queue` 소비 루프(메인)만 구동한다 — 자체 `StockDataStream`은 열지 않는다.

**rate limit 방지:** `on_bar`는 매 bar마다 REST(`get_all_positions`)를 호출하지 않는다. 보유 여부는 `self._positions` 캐시로 판단하고,
캐시는 시작 시 `sync_state()`와 이후 체결 이벤트로만 갱신한다. (Alpaca REST 분당 200회 제한 대응)

**매매 주기(`run_interval`):** `run_interval`은 `_prefetch_bars`/`subscribe_bars`의 TimeFrame과 의사결정 주기를 결정한다.
- `1m`/`5m`/`15m`/`1h`/`1d` → 해당 TimeFrame 분/시/일봉 구독, bar 수신마다 판단
- 분 미만(스캘핑)은 Alpaca 분봉 한계상 `subscribe_quotes`/`subscribe_trades` 기반이 필요 → **현 단계 제외, 추후 검토**(9장)

### Strategy Manager

FastAPI lifespan 안에서 백그라운드 스레드로 실행.

- `start()` — FastAPI 시작 시 호출: 스케줄러 시작 + DB에서 status=running인 전략 복원
- `stop()` — FastAPI 종료 시 호출: 스케줄러 정지 + 전략 프로세스 전부 terminate
- `start_strategy(strategy_id)` — POST /start 요청 시 직접 호출: DB 상태 변경 + 프로세스 즉시 시작
  - **멱등성**: 이미 `self.processes`에 있고 살아있으면 중복 실행하지 않음 (좀비 프로세스 방지)
- `stop_strategy(strategy_id)` — POST /stop 요청 시 직접 호출: DB 상태 변경 + 프로세스 즉시 종료
- `_monitor_crashes()` — 30초마다 실행: 크래시된 프로세스 감지 → 최대 3회 재시작, 초과 시 `status=failed`
  - ⚠️ 재시작 카운터는 `_launch_process`가 덮어쓰지 않도록 **별도 보존**해야 함 (안 그러면 카운터가 0으로 리셋되어 무한 재시작)
- `record_portfolio_history()` — 5분마다 실행: 각 계정 자산 조회 → `portfolio_history` 기록
  - 전략별 `try/except` 로 격리 (한 전략의 키 오류가 다른 전략 기록을 막지 않도록)
- `record_daily_performance()` — **장 마감 후(16:05 ET) 1회 실행**: 전략별 일별 성과 계산 → `daily_performance` 기록
  - `daily_return`(전일 대비 equity 변화율), `win_rate`(실현 익절/전체 청산 비율), `sharpe_ratio`(일별 수익률 표준편차 기반), `drawdown`(고점 대비 낙폭)
  - **이 잡이 없으면 핵심 목표인 "전략 성과 비교"가 동작하지 않음**
- 모든 `self.processes` 접근은 `self._lock`(threading.Lock)으로 보호
- `_launch_process` 시 전략용 `bar_queue` 생성 → 전략에 전달 + `hub.add_strategy(id, symbols, queue)` 등록
- `stop`/크래시/failed 시 `hub.remove_strategy(id)` 로 라우팅 해제 (+ 큐에 None sentinel)

### MarketDataHub (팬아웃 시세 허브)

매니저 프로세스 내 백그라운드 스레드. **시세용 키 1개**로 `StockDataStream` 하나만 연결하고, 모든 전략이 보는 종목의 합집합을 구독해 받은 bar를 전략별 `multiprocessing.Queue`로 분배한다. 무료 데이터 플랜의 동시 시세 연결 1개 한도를 우회하는 핵심 장치.

- `start()` / `stop()` — 시세 스트림 스레드 시작/정지
- `add_strategy(strategy_id, symbols, queue)` — 종목→큐 라우팅 등록. **아무도 안 보던 종목만** `subscribe_bars`
- `remove_strategy(strategy_id)` — 라우팅 해제. **아무도 안 보게 된 종목만** `unsubscribe_bars`
- `_on_bar(bar)` — async 핸들러. 해당 종목 구독 전략 큐들에 `put_nowait`
- 라우팅 테이블은 `threading.RLock`으로 보호 (스트림 스레드 ↔ 매니저 스레드 동시 접근)
- ⚠️ `select_symbols()`는 매니저 프로세스에서도 호출되므로 무거운 자원(거래 클라이언트 등)에 의존하면 안 됨

### FastAPI 엔드포인트

```
GET  /strategies                      전략 목록 + 현재 상태
GET  /strategies/{id}/performance     일별 성과 지표
GET  /strategies/{id}/portfolio       자산 변화 이력 (차팅용)
GET  /strategies/{id}/trades          체결 내역
POST /strategies/{id}/start           Manager.start_strategy() 직접 호출
POST /strategies/{id}/stop            Manager.stop_strategy() 직접 호출
```

**CORS:** 기존 웹 프론트엔드가 브라우저에서 호출하므로 `CORSMiddleware`로 프론트 origin을 허용해야 한다 (미설정 시 모든 요청 차단).

---

## 4. 데이터 흐름

```
0. MarketDataHub → 시세용 키로 StockDataStream 1개 연결 (전 전략 공용, 매니저 프로세스)
1. 전략 run() → _setup() → DB Engine/TradingClient/TradingStream 생성 (시세 스트림은 안 만듦)
2.            → sync_state() → Alpaca 잔고/미체결 주문 → self._positions 캐시
3.            → select_symbols() → 감시 종목 (매니저가 hub.add_strategy로 종목 등록 → 허브가 구독)
4.            → _prefetch_bars() → REST API로 과거 N개 bar 로드 → _bar_buffer 채움
5. MarketDataHub → 실시간 bar 수신 → 해당 종목 구독 전략들의 bar_queue로 분배(put)
6. 전략: bar_queue.get() → _process_bar() → _bar_buffer append + on_bar (캐시로 보유 판단, REST 금지)
7. 신호 발생 → Alpaca 주문 제출 (전략별 거래 키)
8. TradingStream → fill/partial_fill 이벤트 수신 → on_order_filled() → trades 저장 + _positions 캐시 갱신
9. Manager(스레드) → 5분마다 계정 자산 조회 → portfolio_history 기록
10. Manager(스레드) → 장 마감 후(16:05 ET) record_daily_performance() → daily_performance 기록
```

---

## 5. 에러 처리

- 자식 프로세스(전략)는 `run()` 진입 후 독립적으로 DB Engine/Session/클라이언트 생성 (부모 객체 fork 공유 금지)
- 프로세스 crash → `_monitor_crashes()`가 최대 3회 재시작, 초과 시 status=failed (재시작 카운터 보존 필수)
- 프로세스 종료(stop) → SIGTERM 후 5s 대기, 미종료 시 SIGKILL + join (alpaca asyncio 웹소켓 루프가 SIGTERM을 즉시 처리하지 못해 terminate만으로는 종료/좀비 reap이 안 됨)
- **데이터 연결 한도(실측)**: 무료 데이터 플랜은 같은 Alpaca 로그인 산하 전체에서 실시간 시세 웹소켓 **동시 1개**만 허용. 여러 전략을 독립 데이터 연결로 동시 실행하려면 → 데이터 플랜 업그레이드 / 시세 1개 연결 후 전략에 팬아웃 / 전략별 별도 로그인 중 택일 (10장 참조)
- Alpaca WebSocket 연결 끊김 → alpaca-py 내장 재연결 로직 (지수 백오프)
- `on_bar` 핸들러 내부 예외는 try/except로 격리 (한 bar 처리 오류가 스트림 전체를 죽이지 않도록)
- `record_portfolio_history`/`record_daily_performance`는 전략별 try/except로 격리
- 장 마감(16:00 ET) 자동 감지 → 전략 설정에 따라 포지션 전량 청산 또는 유지 (추후 구현)
- 프로세스별 독립 로그 파일 (`logs/{strategy_name}.log`)
- **데이터 피드 제약**: 무료 페이퍼 계정의 실시간 시세는 SIP가 아닌 **IEX 피드**(거래량 적은 종목은 분봉이 듬성, 최근 데이터 일부 지연). 백테스트 기대치와 차이가 날 수 있음

---

## 6. 종목 선별

- 전략 시작 시 `select_symbols()` 1회 실행
- 선별 기준은 각 전략 구현에 완전 위임

---

## 7. PostgreSQL 스키마

```sql
CREATE TABLE strategies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,              -- 표시용 이름 (예: "MA 크로스오버 - 공격형")
    strategy_type VARCHAR(100) NOT NULL,     -- 파일명과 매핑 (예: "ma_crossover")
    alpaca_key VARCHAR(255) NOT NULL,
    alpaca_secret VARCHAR(255) NOT NULL,
    budget NUMERIC(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,             -- 'running', 'stopped', 'failed'
    run_interval VARCHAR(20) NOT NULL,       -- '1m', '5m', '1d' 등
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES strategies(id) ON DELETE CASCADE,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,               -- 'buy', 'sell'
    qty NUMERIC(12, 4) NOT NULL,
    price NUMERIC(12, 4) NOT NULL,
    alpaca_order_id VARCHAR(100) NOT NULL UNIQUE,
    filled_at TIMESTAMP NOT NULL
);

CREATE TABLE portfolio_history (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES strategies(id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    equity NUMERIC(12, 2) NOT NULL,
    cash NUMERIC(12, 2) NOT NULL,
    unrealized_pnl NUMERIC(12, 2) NOT NULL
);

CREATE TABLE daily_performance (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES strategies(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    total_value NUMERIC(12, 2) NOT NULL,
    daily_return NUMERIC(6, 4) NOT NULL,
    win_rate NUMERIC(5, 2),
    sharpe_ratio NUMERIC(5, 2),
    drawdown NUMERIC(5, 2),
    UNIQUE (strategy_id, date)
);
```

---

## 8. 프로젝트 구조

```
tradingbot/
├── strategies/
│   ├── base.py              # BaseStrategy (추상 클래스 + 공통 로직)
│   └── ma_crossover.py      # 이동평균 크로스오버 예시 전략
├── manager/
│   ├── manager.py           # StrategyManager (lifespan 스레드)
│   └── market_data_hub.py   # MarketDataHub (시세 1연결 → 전략 큐 팬아웃)
├── api/
│   ├── schemas.py           # Pydantic 응답 모델
│   └── main.py              # FastAPI 앱 + lifespan + 엔드포인트
├── db/
│   ├── database.py          # create_engine 팩토리
│   ├── models.py            # SQLAlchemy ORM 모델
│   └── init_db.py           # 테이블 생성 스크립트
├── tests/
├── logs/
├── .gitignore               # .env / .venv / logs / __pycache__ 제외 (시크릿 커밋 방지)
├── .env.example
├── requirements.txt
└── main.py                  # 진입점 (uvicorn 실행만)
```

---

## 9. 추후 추가 검토 필요

현재 단계(페이퍼 트레이딩)에서는 제외했으나, 실투자 전환 또는 운영 안정화 시점에 재검토 권장.

| 항목 | 내용 |
|------|------|
| Panic Button | `POST /strategies/{id}/panic` — 미체결 주문 전량 취소 + 보유 주식 시장가 청산 |
| 일일 손실 한도 | 하루 손실이 예산의 X% 초과 시 자동 거래 중지 → `BaseStrategy`에 공통 적용 |
| 장 마감 포지션 처리 | 16:00 ET 감지 → 전략 설정에 따라 청산 또는 유지 (timezone-aware 스케줄링 필요) |
| Rate Limit 정교화 | Alpaca REST 분당 200회 제한 대응 — 전략별 Rate Limiter 내장 |
| 분 미만 스캘핑 | `run_interval` < 1m은 분봉 스트림으로 불가 → `subscribe_quotes`/`subscribe_trades` 기반 별도 처리 필요 |
| 멀티분/일봉 집계 | 실시간 스트림은 1분봉 고정 → 5m/15m/1h/1d 의사결정은 버퍼 재집계 로직 추가 검토 |
| 시크릿 암호화 | `alpaca_key/secret` DB 평문 저장 → 실투자 전 KMS/암호화 컬럼 적용 |
| systemd 데몬화 | 서버 재시작 시 자동 복구 (Ubuntu 배포 운영화) |

---

## 10. 기술 스택

| 역할 | 선택 |
|------|------|
| 언어 | Python 3.11+ |
| 트레이딩 API | alpaca-py (StockDataStream=시세, TradingStream=체결, TradingClient=주문/조회) |
| API 서버 | FastAPI + uvicorn (+ CORSMiddleware) |
| DB ORM | SQLAlchemy 2.x + psycopg2-binary |
| DB | PostgreSQL |
| 프로세스 관리 | multiprocessing |
| 스케줄링 | APScheduler (BackgroundScheduler) |
| 환경 설정 | python-dotenv |
