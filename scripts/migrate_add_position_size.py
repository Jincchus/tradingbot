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
