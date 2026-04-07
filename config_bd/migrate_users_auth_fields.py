"""
Миграция таблицы users:
- email: тип TEXT, UNIQUE (несколько NULL допускаются в PostgreSQL)
- password_hash: TEXT NULL
- linked_telegram_id: BIGINT NULL (как user_id; INTEGER слишмал для типичных Telegram ID)

Запуск из корня проекта (нужны переменные .env для Postgres):
  python -m config_bd.migrate_users_auth_fields
"""
from __future__ import annotations

import asyncio
import sys

from sqlalchemy import text

from config_bd.models import LinkingCodes, PasswordResetCodes, engine


async def migrate() -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "ALTER TABLE users ALTER COLUMN email TYPE TEXT USING email::text"
            )
        )

        dup = await conn.execute(
            text(
                """
                SELECT email, COUNT(*) AS cnt
                FROM users
                WHERE email IS NOT NULL
                GROUP BY email
                HAVING COUNT(*) > 1
                """
            )
        )
        rows = dup.fetchall()
        if rows:
            print(
                "Error: duplicate non-null emails. Fix duplicates and re-run.",
                file=sys.stderr,
            )
            for r in rows:
                print(f"  email={r[0]!r} rows={r[1]}", file=sys.stderr)
            raise SystemExit(1)

        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email ON users (email)"
            )
        )

        await conn.execute(
            text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS linked_telegram_id BIGINT"
            )
        )

        def _create_auth_tables(sync_conn):
            PasswordResetCodes.__table__.create(sync_conn, checkfirst=True)
            LinkingCodes.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_auth_tables)
        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_linking_codes_code ON linking_codes (code)"
            )
        )

    print(
        "OK: users email (TEXT+UNIQUE), password_hash, linked_telegram_id; "
        "linking_codes, password_reset_codes."
    )


def main() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
