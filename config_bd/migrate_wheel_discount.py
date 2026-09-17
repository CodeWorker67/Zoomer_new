"""
Резервы и ledger списания скидок колеса.

  python -m config_bd.migrate_wheel_discount
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
                CREATE TABLE IF NOT EXISTS wheel_discount_reservations (
                    id SERIAL PRIMARY KEY,
                    token VARCHAR(32) NOT NULL UNIQUE,
                    user_id BIGINT NOT NULL,
                    percent INTEGER NOT NULL,
                    kind VARCHAR(16) NOT NULL,
                    product_key VARCHAR(32) NOT NULL,
                    base_rub INTEGER NOT NULL,
                    final_rub INTEGER NOT NULL,
                    base_stars INTEGER NOT NULL DEFAULT 0,
                    final_stars INTEGER NOT NULL DEFAULT 0,
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    consumed_at TIMESTAMP NULL,
                    payment_ref VARCHAR(128) NULL
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_wheel_discount_res_user_status
                ON wheel_discount_reservations (user_id, status)
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS wheel_discount_redemptions (
                    id SERIAL PRIMARY KEY,
                    reservation_id INTEGER NOT NULL REFERENCES wheel_discount_reservations(id),
                    user_id BIGINT NOT NULL,
                    percent INTEGER NOT NULL,
                    kind VARCHAR(16) NOT NULL,
                    product_key VARCHAR(32) NOT NULL,
                    payment_ref VARCHAR(128) NOT NULL UNIQUE,
                    payload TEXT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        )
    print("OK: wheel_discount_reservations, wheel_discount_redemptions")
