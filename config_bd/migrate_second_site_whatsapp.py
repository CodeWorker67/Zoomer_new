"""
Добавляет second_site.whatsapp_id (Meta WhatsApp ID пользователя для входа через WA-бота).

Запуск из корня проекта:
  python -m config_bd.migrate_second_site_whatsapp
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
                ALTER TABLE second_site
                ADD COLUMN IF NOT EXISTS whatsapp_id VARCHAR(32) NULL
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ix_second_site_whatsapp_id
                ON second_site (whatsapp_id)
                WHERE whatsapp_id IS NOT NULL
                """
            )
        )
    print("OK: second_site.whatsapp_id добавлен (или уже существовал).")


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
