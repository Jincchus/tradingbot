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
┌──────────────────────────────────────────────────────┐
│                  FastAPI 프로세스                     │
│                                                      │
│  ┌──────────────────────────────────────────────┐   │
│  │           Strategy Manager (스레드)           │   │
│  │  - 프로세스 크래시 감시 (30s 주기)             │   │
│  │  - portfolio_history 기록 (5min 주기)          │   │
│  └──────────┬──────────────┬─────────────────────┘  │
│             │              │                         │
│  POST /start│    POST /stop│  ← API 요청 시 직접 제어│
└─────────────┼──────────────┼─────────────────────────┘
              │              │
        ┌─────▼──┐     ┌─────▼──┐     ┌────────┐
        │Strategy│     │Strategy│     │Strategy│  ...
        │  A     │     │  B     │     │  C     │
        │(process│     │(process│     │(process│
        └────┬───┘     └────┬───┘     └────┬───┘
             │              │              │
        ┌────▼──┐      ┌────▼──┐      ┌────▼──┐
        │Alpaca │      │Alpaca │      │Alpaca │
        │Paper#1│      │Paper#2│      │Paper#3│
        └────┬──┘      └────┬──┘      └────┬──┘
             └──────────────┼──────────────┘
                            ▼
                    ┌───────────────┐
                    │  PostgreSQL   │
                    └───────┬───────┘
                            ▼
                    기존 웹 프론트엔드 (REST API)
```

**핵심 결정:**
- Manager는 FastAPI와 같은 프로세스 내 백그라운드 스레드로 실행
- start/stop API 요청 시 Manager가 직접 subprocess를 생성/종료 (DB 폴링 없음, 즉시 반영)
- 각 전략 subprocess는 시작 시 독립적으로 DB Engine 생성 (부모 공유 금지)

---

## 3. 핵심 컴포넌트

### BaseStrategy (공통 인터페이스)

모든 전략이 상속하는 추상 클래스. 시작 시 과거 데이터를 백필하여 롤링 버퍼를 채운 뒤 WebSocket 수신.

```python
class BaseStrategy:
    BUFFER_SIZE = 200

    def select_symbols(self) -> list[str]: ...      # 종목 선별
    def on_bar(self, bar) -> None: ...              # bar 수신 시 (버퍼에서 지표 계산)
    def on_order_filled(self, order) -> None: ...   # 체결 → DB 저장
    def get_metrics(self) -> dict: ...              # 성과 지표
    def sync_state(self) -> None: ...               # Alpaca 잔고/미체결 주문 동기화
    def _prefetch_bars(self, symbols) -> None: ...  # 시작 시 1회 REST API로 과거 데이터 로드
    # self._bar_buffer: dict[str, deque]           # 종목별 close 가격 롤링 버퍼
```

### Strategy Manager

FastAPI lifespan 안에서 백그라운드 스레드로 실행.

- `start()` — FastAPI 시작 시 호출: 스케줄러 시작 + DB에서 status=running인 전략 복원
- `stop()` — FastAPI 종료 시 호출: 스케줄러 정지 + 전략 프로세스 전부 terminate
- `start_strategy(strategy_id)` — POST /start 요청 시 직접 호출: DB 상태 변경 + 프로세스 즉시 시작
- `stop_strategy(strategy_id)` — POST /stop 요청 시 직접 호출: DB 상태 변경 + 프로세스 즉시 종료
- `_monitor_crashes()` — 30초마다 실행: 크래시된 프로세스 감지 → 최대 3회 재시작
- `record_portfolio_history()` — 5분마다 실행: 각 계정 자산 조회 → DB 기록

### FastAPI 엔드포인트

```
GET  /strategies                      전략 목록 + 현재 상태
GET  /strategies/{id}/performance     일별 성과 지표
GET  /strategies/{id}/portfolio       자산 변화 이력 (차팅용)
GET  /strategies/{id}/trades          체결 내역
POST /strategies/{id}/start           Manager.start_strategy() 직접 호출
POST /strategies/{id}/stop            Manager.stop_strategy() 직접 호출
```

---

## 4. 데이터 흐름

```
1. 전략 시작 → sync_state() → Alpaca에서 잔고/미체결 주문 동기화
2.           → select_symbols() → 감시 종목 결정
3.           → _prefetch_bars() → REST API로 과거 N개 bar 로드 → _bar_buffer 채움
4. Alpaca WebSocket 연결 → 실시간 bar 수신 → _bar_buffer에 append
5. on_bar() → 버퍼의 close 가격으로 지표 계산 → 매매 신호 판단
6. 신호 발생 → Alpaca 주문 → 체결 → on_order_filled() → trades 테이블 저장
7. Manager(스레드) → 5분마다 계정 자산 조회 → portfolio_history 기록
8. Manager(스레드) → 장 마감 후 일별 지표 계산 → daily_performance 기록
```

---

## 5. 에러 처리

- 자식 프로세스(전략)는 시작 시 독립적으로 DB Engine/Session 생성 (부모 객체 공유 금지)
- 프로세스 crash → `_monitor_crashes()`가 최대 3회 재시작, 초과 시 status=failed
- Alpaca WebSocket 연결 끊김 → alpaca-py 내장 재연결 로직 (지수 백오프)
- 장 마감(16:00 ET) 자동 감지 → 전략 설정에 따라 포지션 전량 청산 또는 유지 (추후 구현)
- 프로세스별 독립 로그 파일 (`logs/{strategy_name}.log`)

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
│   └── manager.py           # StrategyManager (lifespan 스레드)
├── api/
│   ├── schemas.py           # Pydantic 응답 모델
│   └── main.py              # FastAPI 앱 + lifespan + 엔드포인트
├── db/
│   ├── database.py          # create_engine 팩토리
│   ├── models.py            # SQLAlchemy ORM 모델
│   └── init_db.py           # 테이블 생성 스크립트
├── tests/
├── logs/
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
| systemd 데몬화 | 서버 재시작 시 자동 복구 (Ubuntu 배포 운영화) |

---

## 10. 기술 스택

| 역할 | 선택 |
|------|------|
| 언어 | Python 3.11+ |
| 트레이딩 API | alpaca-py |
| API 서버 | FastAPI + uvicorn |
| DB ORM | SQLAlchemy 2.x + psycopg2-binary |
| DB | PostgreSQL |
| 프로세스 관리 | multiprocessing |
| 스케줄링 | APScheduler (BackgroundScheduler) |
| 환경 설정 | python-dotenv |
