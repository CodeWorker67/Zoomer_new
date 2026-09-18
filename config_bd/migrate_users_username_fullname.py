"""
Миграция таблицы users — Telegram-профиль:
- username VARCHAR(255) NULL
- fullname VARCHAR(255) NULL

Запуск из корня проекта (нужны переменные .env для Postgres):
  python -m config_bd.migrate_users_username_fullname

  Либо напрямую:
  python config_bd/migrate_users_username_fullname.py
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
            text("ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(255)")
        )
        await conn.execute(
            text("ALTER TABLE users ADD COLUMN IF NOT EXISTS fullname VARCHAR(255)")
        )

    print("OK: users.username, users.fullname.")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
