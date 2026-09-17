"""reservation_id в wheel_discount_redemptions — nullable (активация без резерва)."""
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
                "ALTER TABLE wheel_discount_redemptions "
                "ALTER COLUMN reservation_id DROP NOT NULL"
            )
        )
    print("OK: wheel_discount_redemptions.reservation_id nullable")


if __name__ == "__main__":
    asyncio.run(migrate())
