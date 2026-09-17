"""
Колонки username, full_name в wheel_fortuna.

  python -m config_bd.migrate_wheel_fortuna_profile
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
            text("ALTER TABLE wheel_fortuna ADD COLUMN IF NOT EXISTS username VARCHAR(255)")
        )
        await conn.execute(
            text("ALTER TABLE wheel_fortuna ADD COLUMN IF NOT EXISTS full_name VARCHAR(512)")
        )
    print("OK: wheel_fortuna.username, wheel_fortuna.full_name")


if __name__ == "__main__":
    asyncio.run(migrate())
