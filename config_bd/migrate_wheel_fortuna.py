"""
Таблица wheel_fortuna — попытки и призы колеса фортуны.

Запуск из корня проекта:
  python -m config_bd.migrate_wheel_fortuna
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
                CREATE TABLE IF NOT EXISTS wheel_fortuna (
                    user_id BIGINT PRIMARY KEY,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    rotation_number INTEGER NOT NULL DEFAULT 0,
                    discount_10 INTEGER NOT NULL DEFAULT 0,
                    discount_30 INTEGER NOT NULL DEFAULT 0,
                    discount_50 INTEGER NOT NULL DEFAULT 0,
                    partner_wheel_batches_credited INTEGER NOT NULL DEFAULT 0,
                    history TEXT,
                    pending_prize_id VARCHAR(64),
                    pending_since TIMESTAMP,
                    username VARCHAR(255),
                    full_name VARCHAR(512),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
        )
    print("OK: wheel_fortuna")


if __name__ == "__main__":
    asyncio.run(migrate())
