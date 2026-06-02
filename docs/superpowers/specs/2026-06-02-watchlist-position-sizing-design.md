# 설계: 감시 종목·포지션 크기 설정화 + 비상 청산

작성일: 2026-06-02

## 배경 / 목적

현재 4개 봇(v1~v4)은 감시 종목과 포지션 크기가 **코드에 하드코딩**되어 있다.

- 감시 종목: 각 전략 클래스의 `WATCHLIST = ["AAPL","MSFT","NVDA","TSLA","GOOGL"]`
- 포지션 크기: 각 전략 클래스의 `POSITION_SIZE = 0.2` (예산의 20%)

종목/비중을 바꾸려면 코드를 수정해야 한다. 이를 **런타임에 API로 변경 가능**하게 만들고,
폭락 등 비상 상황에 대비해 **사람이 직접 포지션을 청산**할 수 있는 기능을 추가한다.

비매수 동작(분할 매수/물타기 등)은 이번 범위에서 제외한다.

## 결정 사항 (확정)

| 항목 | 결정 |
|---|---|
| 감시 종목 범위 | **4개 봇 공통** (한 목록 공유) |
| 종목 변경 방식 | **API** (`GET`/`PUT /watchlist`) |
| 종목 변경 적용 | **자동 적용** — 돌아가던 봇 자동 재시작 |
| 종목 검증 | **켬** — 저장 전 알파카로 거래 가능 여부 확인 |
| 종목 제거 시 | **자동 청산** — 빠진 종목 보유분은 자동 매도 |
| 포지션 크기(%) | **봇마다 따로** (예산이 이미 봇별이라 일관) |
| 포지션 크기 변경 | **API** (`PATCH /strategies/{id}`), 적용은 봇 재시작 시 |
| 비상 수동 청산 | 3종류(종목1개 / 봇전체 / 전체), 전부 **"멈추고 팔기"** |

## 아키텍처

### A. 감시 종목 (공통, DB `watchlist` 표)

- 신규 테이블 `watchlist` (종목 1개당 1행). 기존 `strategies` 표는 건드리지 않는다.
  - 컬럼: `id`(PK), `symbol`(unique, not null), `created_at`
- **빈 목록 → 기본값 폴백**: 표가 비어있으면 기본 5종목(`AAPL, MSFT, NVDA, TSLA, GOOGL`)을 사용.
  기본값은 `BaseStrategy.DEFAULT_WATCHLIST` 상수로 둔다. → 마이그레이션 직후에도 **현재 동작 그대로 유지**.
- 봇은 켜질 때 이 표를 읽어 감시 종목을 정한다.
  - `StrategyManager._launch_process`가 DB에서 watchlist를 읽어 `symbols`로 전달.
  - `BaseStrategy.__init__(..., symbols=None)`: `self.symbols = symbols or self.DEFAULT_WATCHLIST`.
  - `BaseStrategy.select_symbols()`는 `self.symbols`를 반환 (각 전략의 `WATCHLIST` 오버라이드 제거,
    대신 `DEFAULT_WATCHLIST` 폴백으로 통일).

### B. 포지션 크기(%) (봇별, `strategies` 표 컬럼 추가)

- `strategies` 표에 `position_size NUMERIC(4,3) NOT NULL DEFAULT 0.2` 컬럼 추가.
- **마이그레이션 필요** (기존 DB): `ALTER TABLE strategies ADD COLUMN position_size NUMERIC(4,3) NOT NULL DEFAULT 0.2;`
  - `create_all`은 기존 표에 컬럼을 추가하지 않으므로, 일회성 마이그레이션 스크립트/구문을 제공한다.
  - 기존 행은 DEFAULT로 0.2 채워짐.
- 봇은 클래스 상수 `POSITION_SIZE` 대신 자기 `position_size`를 읽어 매수 수량 계산.
  - `BaseStrategy.__init__(..., position_size=None)`: `self.position_size = position_size or 0.2`.
  - 각 전략의 매수 계산을 `self.budget * self.position_size / price`로 변경 (클래스 상수 `POSITION_SIZE` 제거).

### C. 비상 수동 청산 (3종류, 전부 "멈추고 팔기")

청산은 **관리자(API/매니저 프로세스)가 알파카에 직접 주문**한다. 봇 프로세스 생존 여부와 무관 → 비상시 신뢰성.
각 봇의 키(`strategies.alpaca_key/secret`)로 `TradingClient(paper=True)` 생성 후 청산.

- `POST /strategies/{id}/positions/{symbol}/close` — 특정 봇의 **종목 1개** 청산
  - 봇 stop → 해당 종목 `close_position(symbol)`
- `POST /strategies/{id}/liquidate` — 특정 봇 **전체** 청산
  - 봇 stop → `close_all_positions(cancel_orders=True)`
- `POST /liquidate-all` — **모든 봇** 청산 (폭락 비상 버튼)
  - 각 running 봇에 대해 stop → 전체 청산
- 셋 다 **해당 봇을 `stopped` 상태로 전환**(매니저 `stop_strategy`). 재가동은 수동 `start`.
  - 이유: 봇이 켜진 채 강제 매도하면 봇의 포지션 캐시가 어긋나(여전히 보유로 인식) 재매수/매도 로직이 꼬임.
    종목 1개 청산도 동일하게 그 봇 전체를 멈추는 것으로 일관 처리한다.

### B-api. 종목 변경 API 처리 순서 (`PUT /watchlist`)

요청 body: `{"symbols": ["AAPL", "MSFT", ...]}`

1. **검증**: 각 종목을 알파카에서 조회(`ALPACA_KEY`/`ALPACA_SECRET` 환경변수 사용),
   `status == active` 및 `tradable == true` 확인. 하나라도 실패 → `400`, 저장 안 함, 문제 종목 안내.
2. **자동 청산**: 기존 목록 대비 **빠진 종목**을, 그 종목 보유 중인 모든 봇에서 청산.
   - 순서: 봇 stop → 빠진 종목 청산 → (4단계에서 재시작).
3. **저장**: `watchlist` 표를 새 목록으로 교체.
4. **자동 재시작**: 직전에 running이던 봇들을 다시 start → 새 목록 반영.
   - 자동 청산으로 멈춘 봇과, 변경 전 running이던 봇을 재가동.

## 데이터 흐름

```
[사람] --PUT /watchlist--> [API]
   1. 알파카 검증 (ALPACA_KEY)
   2. 빠진 종목 보유 봇 stop + 청산
   3. watchlist 표 교체
   4. running 봇 재시작
       --> [Manager._launch_process] DB watchlist 읽음
       --> [BaseStrategy] symbols 주입 --> select_symbols() --> hub 구독
```

## 에러 처리

- 검증 실패: `400 Bad Request`, 어떤 종목이 거부됐는지 본문에 명시. **저장/청산/재시작 모두 미수행**.
- 알파카 청산 실패: 봇별로 격리(한 봇 실패가 다른 봇 청산을 막지 않음), 실패는 로깅 + 응답에 부분 실패 표시.
- 빈 목록 저장 시도: 거부(`400`) — 빈 목록이면 폴백이 발동해 의도와 어긋남. 최소 1종목 요구.
- `ALPACA_KEY` 미설정: `PUT /watchlist` 검증 불가 → `500`/`503`로 명확히 안내.

## 테스트 (TDD)

- `watchlist` 모델/표 생성, 빈 목록→기본값 폴백.
- `PUT /watchlist`: 정상 교체 / 잘못된 종목 거부(저장 안 됨) / 빈 목록 거부 / 빠진 종목 자동청산 호출 / running 봇 재시작 호출.
- `GET /watchlist`: 저장값 반환, 비었을 때 기본값 반환.
- `BaseStrategy`: 주입된 `symbols`·`position_size` 사용, 미주입 시 폴백.
- 각 전략 매수 수량이 `position_size` 반영.
- 비상 청산 3종 엔드포인트: 봇 stop + 알맞은 close 호출, 존재하지 않는 id/symbol 처리.
- `PATCH /strategies/{id}`: position_size 갱신.
- 알파카 호출은 목(mock) 처리 (페이퍼 계정·외부 의존 격리).

## 범위 밖 (이번 제외)

- 분할 매수 / 물타기 / 가격 기반 스케일인.
- 동적 종목 스크리닝 (봇이 시장 훑어 자동 선정).
- 종목/포지션 변경을 위한 프론트엔드 UI (API만 제공; UI는 후속).
- 손절/익절(stop-loss/take-profit) 자동화 (별도 작업).
```
