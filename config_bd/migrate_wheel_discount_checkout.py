"""
Активная скидка при оплате (списание при выборе, без резерва).

  python -m config_bd.migrate_wheel_discount_checkout
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
                CREATE TABLE IF NOT EXISTS wheel_discount_checkout (
                    user_id BIGINT PRIMARY KEY,
                    kind VARCHAR(16) NOT NULL,
                    product_key VARCHAR(32) NOT NULL,
                    percent INTEGER NOT NULL,
                    base_rub INTEGER NOT NULL,
                    final_rub INTEGER NOT NULL,
                    base_stars INTEGER NOT NULL DEFAULT 0,
                    final_stars INTEGER NOT NULL DEFAULT 0,
                    expires_at TIMESTAMP NOT NULL,
                    activated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        )
    print("OK: wheel_discount_checkout")
