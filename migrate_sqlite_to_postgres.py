"""
Перенос данных из SQLite (sqlite3.db) в PostgreSQL по переменным из .env (POSTGRES_*).

Поддерживаются оба варианта схемы users в SQLite:
  — старая: Id, User_id, Is_pay_null, Is_tarif, Is_admin, has_discount, Create_user, …
  — уже как в PG: id, user_id, in_panel, is_connect, in_chanel, reserve_field, …

Остальные таблицы копируются по совпадению имён колонок (как у SQLAlchemy).

Перед первым переносом создайте таблицы в PostgreSQL (один запуск бота / main.py с create_tables).

Запуск из корня проекта:
  pip install -r requirements.txt
  python migrate_sqlite_to_postgres.py --truncate

  --dry-run       только посчитать строки, без записи в PG
  --sqlite PATH   путь к файлу SQLite (по умолчанию ./sqlite3.db)
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
from dotenv import load_dotenv
import os

ROOT = Path(__file__).resolve().parent

# Порядок: сначала users, затем остальные (логические ссылки на user_id).
TABLES_ORDER: List[str] = [
    "users",
    "gifts",
    "payments",
    "payments_cards",
    "payments_platega_crypto",
    "payments_stars",
    "payments_cryptobot",
    "white_counter",
    "online",
]

SERIAL_TABLES: List[Tuple[str, str]] = [
    ("payments", "id"),
    ("payments_cards", "id"),
    ("payments_platega_crypto", "id"),
    ("payments_stars", "id"),
    ("payments_cryptobot", "id"),
    ("white_counter", "id"),
    ("online", "online_id"),
    ("users", "id"),
]


def pg_connect():
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    db = os.environ.get("POSTGRES_DB")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    if not user or not password or not db:
        raise SystemExit("Задайте POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB в .env")
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
    )


def sqlite_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return [row[1] for row in cur.fetchall()]


def as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    try:
        return bool(int(v))
    except (TypeError, ValueError):
        return default


def as_datetime(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    if isinstance(v, str):
        s = v.strip()
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(s[:26], fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def as_date(v: Any) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    dt = as_datetime(str(v))
    return dt.date() if dt else None


def pick(raw: Dict[str, Any], *names: str) -> Any:
    for n in names:
        if n in raw:
            return raw[n]
    return None


def row_to_dict(columns: Sequence[str], row: Tuple[Any, ...]) -> Dict[str, Any]:
    return dict(zip(columns, row))


def user_sqlite_to_pg(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Старая (PascalCase) и новая (snake_case) схема users в SQLite."""
    rid = pick(raw, "id", "Id")
    uid = pick(raw, "user_id", "User_id")
    if uid is None:
        raise ValueError("user row without user_id")

    return {
        "id": int(rid) if rid is not None else None,
        "user_id": int(uid),
        "ref": pick(raw, "ref", "Ref"),
        "is_delete": as_bool(pick(raw, "is_delete", "Is_delete"), False),
        "in_panel": as_bool(
            pick(raw, "in_panel", "Is_pay_null", "is_pay_null"), False
        ),
        "is_connect": as_bool(
            pick(raw, "is_connect", "Is_tarif", "is_tarif"), False
        ),
        "create_user": as_datetime(pick(raw, "create_user", "Create_user"))
        or datetime.now(),
        "in_chanel": as_bool(
            pick(raw, "in_chanel", "Is_admin", "is_admin"), False
        ),
        "reserve_field": as_bool(
            pick(raw, "reserve_field", "has_discount"), False
        ),
        "subscription_end_date": as_datetime(
            pick(raw, "subscription_end_date")
        ),
        "white_subscription_end_date": as_datetime(
            pick(raw, "white_subscription_end_date")
        ),
        "last_notification_date": as_date(pick(raw, "last_notification_date")),
        "last_broadcast_status": pick(raw, "last_broadcast_status"),
        "last_broadcast_date": as_datetime(pick(raw, "last_broadcast_date")),
        "stamp": pick(raw, "stamp") or "",
        "ttclid": pick(raw, "ttclid"),
        "subscribtion": pick(raw, "subscribtion"),
        "white_subscription": pick(raw, "white_subscription"),
        "email": pick(raw, "email"),
        "password": pick(raw, "password"),
        "activation_pass": pick(raw, "activation_pass"),
        "field_str_1": pick(raw, "field_str_1"),
        "field_str_2": pick(raw, "field_str_2"),
        "field_str_3": pick(raw, "field_str_3"),
        "field_bool_1": as_bool(pick(raw, "field_bool_1"), False),
        "field_bool_2": as_bool(pick(raw, "field_bool_2"), False),
        "field_bool_3": as_bool(pick(raw, "field_bool_3"), False),
    }


def users_table_name(sl: sqlite3.Connection) -> str:
    cur = sl.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)='users'"
    )
    row = cur.fetchone()
    if not row:
        raise SystemExit("В SQLite нет таблицы users")
    return row[0]


USER_PG_COLS: List[str] = [
    "id",
    "user_id",
    "ref",
    "is_delete",
    "in_panel",
    "is_connect",
    "create_user",
    "in_chanel",
    "reserve_field",
    "subscription_end_date",
    "white_subscription_end_date",
    "last_notification_date",
    "last_broadcast_status",
    "last_broadcast_date",
    "stamp",
    "ttclid",
    "subscribtion",
    "white_subscription",
    "email",
    "password",
    "activation_pass",
    "field_str_1",
    "field_str_2",
    "field_str_3",
    "field_bool_1",
    "field_bool_2",
    "field_bool_3",
]


def migrate_users(
    sl: sqlite3.Connection, pg, dry_run: bool
) -> int:
    tname = users_table_name(sl)
    cols = sqlite_columns(sl, tname)
    if not cols:
        raise SystemExit("В SQLite нет колонок у таблицы users")
    cur = sl.execute(f'SELECT * FROM "{tname}"')
    rows = cur.fetchall()
    n = 0
    placeholders = ", ".join("%s" for _ in USER_PG_COLS)
    col_list = ", ".join(USER_PG_COLS)
    sql_ins = f"INSERT INTO users ({col_list}) VALUES ({placeholders})"

    if dry_run:
        return len(rows)

    with pg.cursor() as c:
        for row in rows:
            d = row_to_dict(cols, row)
            pgd = user_sqlite_to_pg(d)
            vals = tuple(pgd[k] for k in USER_PG_COLS)
            c.execute(sql_ins, vals)
            n += 1
    return n


def table_exists_sqlite(sl: sqlite3.Connection, table: str) -> bool:
    cur = sl.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


# Колонки в PG (как в models)
TABLE_PG_COLUMNS: Dict[str, List[str]] = {
    "gifts": [
        "gift_id",
        "giver_id",
        "duration",
        "recepient_id",
        "white_flag",
        "flag",
    ],
    "payments": [
        "id",
        "user_id",
        "amount",
        "time_created",
        "is_gift",
        "status",
        "transaction_id",
        "payload",
    ],
    "payments_cards": [
        "id",
        "user_id",
        "amount",
        "time_created",
        "is_gift",
        "status",
        "transaction_id",
        "payload",
    ],
    "payments_platega_crypto": [
        "id",
        "user_id",
        "amount",
        "time_created",
        "is_gift",
        "status",
        "transaction_id",
        "payload",
    ],
    "payments_stars": [
        "id",
        "user_id",
        "amount",
        "time_created",
        "is_gift",
        "status",
        "payload",
    ],
    "payments_cryptobot": [
        "id",
        "user_id",
        "amount",
        "currency",
        "time_created",
        "is_gift",
        "status",
        "invoice_id",
        "payload",
    ],
    "white_counter": ["id", "user_id", "time_created"],
    "online": [
        "online_id",
        "online_date",
        "users_panel",
        "users_active",
        "users_pay",
        "users_trial",
    ],
}


def normalize_sqlite_row(
    table: str, col: str, val: Any
) -> Any:
    if val is None:
        return None
    if table == "payments_cryptobot" and col == "amount":
        return float(val)
    if col.endswith("_date") or col in ("time_created", "create_user"):
        if "notification" in col:
            return as_date(val)
        return as_datetime(val)
    if col in (
        "is_delete",
        "in_panel",
        "is_connect",
        "in_chanel",
        "reserve_field",
        "is_gift",
        "white_flag",
        "flag",
    ):
        return as_bool(val, False)
    if col in ("duration", "users_panel", "users_active", "users_pay", "users_trial"):
        return int(val)
    if col == "amount":
        return int(val)
    if col in ("user_id", "giver_id", "recepient_id", "id", "online_id"):
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def migrate_generic_table_coerce(
    sl: sqlite3.Connection,
    pg,
    table: str,
    pg_columns: List[str],
    dry_run: bool,
) -> int:
    if not table_exists_sqlite(sl, table):
        return 0
    sl_cols = sqlite_columns(sl, table)
    common = [c for c in pg_columns if c in sl_cols]
    if not common:
        print(f"  skip {table}: нет общих колонок")
        return 0

    col_sql = ", ".join(f'"{c}"' for c in common)
    cur = sl.execute(f'SELECT {col_sql} FROM "{table}"')
    rows = cur.fetchall()
    if dry_run:
        return len(rows)

    placeholders = ", ".join("%s" for _ in common)
    ins = f'INSERT INTO "{table}" ({", ".join(common)}) VALUES ({placeholders})'
    with pg.cursor() as c:
        for row in rows:
            out = [
                normalize_sqlite_row(table, common[i], row[i])
                for i in range(len(common))
            ]
            c.execute(ins, tuple(out))
    return len(rows)


def truncate_pg(pg, dry_run: bool) -> None:
    if dry_run:
        print("dry-run: пропуск TRUNCATE")
        return
    lst = ", ".join(f'"{t}"' for t in TABLES_ORDER)
    with pg.cursor() as c:
        c.execute(f"TRUNCATE TABLE {lst} RESTART IDENTITY CASCADE")
    pg.commit()
    print("PostgreSQL: TRUNCATE ... RESTART IDENTITY CASCADE выполнен.")


def sync_sequences(pg, dry_run: bool) -> None:
    if dry_run:
        return
    with pg.cursor() as c:
        for table, col in SERIAL_TABLES:
            c.execute(f'SELECT MAX("{col}") FROM "{table}"')
            row = c.fetchone()
            mx = row[0] if row else None
            if mx is None:
                continue
            c.execute(
                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, true)",
                (table, col, int(mx)),
            )
    pg.commit()


def main() -> None:
    load_dotenv(ROOT / ".env")
    ap = argparse.ArgumentParser(description="SQLite → PostgreSQL")
    ap.add_argument(
        "--sqlite",
        type=Path,
        default=ROOT / "sqlite3.db",
        help="Путь к sqlite3.db",
    )
    ap.add_argument(
        "--truncate",
        action="store_true",
        help="Очистить таблицы в PostgreSQL перед загрузкой (рекомендуется)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Только посчитать строки, без записи в PG",
    )
    args = ap.parse_args()
    path: Path = args.sqlite
    if not path.is_file():
        raise SystemExit(f"Файл SQLite не найден: {path}")

    sl = sqlite3.connect(path)
    sl.row_factory = None
    try:
        pg = pg_connect()
        pg.autocommit = False
        try:
            if args.truncate:
                truncate_pg(pg, args.dry_run)
            elif not args.dry_run:
                print(
                    "Внимание: без --truncate возможны дубликаты (UNIQUE). "
                    "Обычно нужен --truncate при первой миграции."
                )

            total = 0
            nu = migrate_users(sl, pg, args.dry_run)
            print(f"users: {nu} строк")
            total += nu

            for t in TABLES_ORDER[1:]:
                cols = TABLE_PG_COLUMNS.get(t)
                if not cols:
                    continue
                n = migrate_generic_table_coerce(sl, pg, t, cols, args.dry_run)
                if n:
                    print(f"{t}: {n} строк")
                total += n

            if not args.dry_run:
                pg.commit()
                sync_sequences(pg, False)
                print(f"Готово. Всего записей: {total}")
            else:
                print(f"dry-run: всего строк к копированию: {total}")
        except Exception:
            pg.rollback()
            raise
        finally:
            pg.close()
    finally:
        sl.close()


if __name__ == "__main__":
    main()
