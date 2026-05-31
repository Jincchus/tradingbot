"""전략을 DB에 등록한다. Alpaca 키는 환경변수로만 받아 출력에 노출하지 않는다.

사용 예:
    ALPACA_KEY=... ALPACA_SECRET=... \
    python -m scripts.register_strategy \
        --name "MA 크로스오버 v1" --type ma_crossover --budget 10000 --interval 1m

등록 후 status는 'stopped'. 구동은 POST /strategies/{id}/start 로 한다.
"""
import argparse
import os
from decimal import Decimal

from sqlalchemy.orm import Session

from db.database import create_engine_for_process
from db.models import Strategy


def main() -> None:
    parser = argparse.ArgumentParser(description="Register a trading strategy")
    parser.add_argument("--name", required=True, help="표시용 이름")
    parser.add_argument("--type", required=True, help="strategies/<type>.py 파일명")
    parser.add_argument("--budget", type=Decimal, default=Decimal("10000"))
    parser.add_argument("--interval", default="1m", help="1m / 5m / 15m / 1h / 1d")
    args = parser.parse_args()

    try:
        api_key = os.environ["ALPACA_KEY"]
        api_secret = os.environ["ALPACA_SECRET"]
    except KeyError as e:
        raise SystemExit(f"환경변수 {e} 가 필요합니다 (ALPACA_KEY, ALPACA_SECRET)")

    engine = create_engine_for_process()
    with Session(engine) as session:
        strategy = Strategy(
            name=args.name,
            strategy_type=args.type,
            alpaca_key=api_key,
            alpaca_secret=api_secret,
            budget=args.budget,
            status="stopped",
            run_interval=args.interval,
        )
        session.add(strategy)
        session.commit()
        session.refresh(strategy)
        # 키는 출력하지 않는다
        print(f"Registered strategy id={strategy.id} name={strategy.name} "
              f"type={strategy.strategy_type} interval={strategy.run_interval} status=stopped")


if __name__ == "__main__":
    main()
