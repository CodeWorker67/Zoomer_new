"""
Таблица wheel_fake_recent_wins — сохранённые фейковые записи ленты колеса.

Запуск из корня проекта:
  python -m config_bd.migrate_wheel_fake_recent_wins
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from sqlalchemy import text

from config_bd.models import engine


async def migrate() -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS wheel_fake_recent_wins (
                    id SERIAL PRIMARY KEY,
                    prize_id VARCHAR(64) NOT NULL,
                    name_initial VARCHAR(8) NOT NULL,
                    mask_stars INTEGER NOT NULL DEFAULT 4,
                    won_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_wheel_fake_recent_wins_won_at
                ON wheel_fake_recent_wins (won_at DESC)
                """
            )
        )
    print("OK: wheel_fake_recent_wins")


if __name__ == "__main__":
    asyncio.run(migrate())
