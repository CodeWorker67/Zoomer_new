"""
Миграция таблицы users — поле билетиков розыгрыша:
- tickets INTEGER DEFAULT 0

Запуск из корня проекта (нужны переменные .env для Postgres):
  python -m config_bd.migrate_users_tickets

  Либо напрямую:
  python config_bd/migrate_users_tickets.py
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
            text("ALTER TABLE users ADD COLUMN IF NOT EXISTS tickets INTEGER DEFAULT 0")
        )
        await conn.execute(
            text("UPDATE users SET tickets = 0 WHERE tickets IS NULL")
        )

    print("OK: users tickets.")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
