"""Периодическая выгрузка статистики по меткам RA_ в Google Sheets."""
from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func, select

from config import GOOGLE_PATH_ZOOMER_RA, GOOGLE_SERVICE_ACCOUNT_FILE
from config_bd.models import AsyncSessionLocal, Users
from logging_config import logger
from wl_traffic.service import is_forever_end_date

_SHEET_HEADERS = [
    "ключ",
    "пользователи",
    "оплатившие",
    "действующие триалы",
    "действующие триалы, которые подключили впн",
    "все триалы",
    "все триалы, которые подключали впн",
]


def _subscription_active_at(sub_end: Optional[datetime], *, at: datetime) -> bool:
    if sub_end is None:
        return False
    if is_forever_end_date(sub_end):
        return True
    if sub_end.tzinfo is None:
        aware = sub_end.replace(tzinfo=timezone.utc)
    else:
        aware = sub_end.astimezone(timezone.utc)
    return aware > at


def _spreadsheet_id_from_env(value: str) -> str:
    value = value.strip()
    if "/d/" in value:
        return value.split("/d/", 1)[1].split("/", 1)[0]
    return value


@dataclass
class _StampAgg:
    users: int = 0
    paid_users: int = 0
    active_trials: int = 0
    active_trials_connect: int = 0
    all_trials: int = 0
    all_trials_connect: int = 0


RA_EXPORT_XLSX_HEADERS: Tuple[str, ...] = (
    "ключ",
    "пользователи",
    "оплатившие",
    "действующие триалы",
    "действующие триалы, которые подключили впн",
)


def _aggregate_ra_stamps(
    rows,
    *,
    export_moment: datetime,
) -> Dict[str, _StampAgg]:
    by_stamp: Dict[str, _StampAgg] = defaultdict(_StampAgg)
    for _user_id, stamp, reserve_field, in_panel, is_connect, sub_end in rows:
        key = (stamp or "").strip()
        if not key or "ra_" not in key.lower():
            continue
        agg = by_stamp[key]
        agg.users += 1
        if reserve_field:
            agg.paid_users += 1

        if in_panel:
            agg.all_trials += 1
            if is_connect:
                agg.all_trials_connect += 1

        if (
            in_panel
            and not reserve_field
            and _subscription_active_at(sub_end, at=export_moment)
        ):
            agg.active_trials += 1
            if is_connect:
                agg.active_trials_connect += 1

    return by_stamp


async def _fetch_ra_stamp_user_rows(
    *,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
):
    async with AsyncSessionLocal() as session:
        stmt = select(
            Users.user_id,
            Users.stamp,
            Users.reserve_field,
            Users.in_panel,
            Users.is_connect,
            Users.subscription_end_date,
        ).where(
            # В ILIKE «_» — один любой символ; нужна подстрока «ra_», не «ra» + символ.
            func.strpos(func.lower(Users.stamp), "ra_") > 0,
        )
        if period_start is not None:
            stmt = stmt.where(Users.create_user >= period_start)
        if period_end is not None:
            stmt = stmt.where(Users.create_user < period_end)
        return (await session.execute(stmt)).all()


def _rows_from_by_stamp(
    by_stamp: Dict[str, _StampAgg],
    *,
    headers: List[str],
    include_totals: bool,
) -> List[List]:
    out: List[List] = [headers]
    totals = _StampAgg()
    for stamp in sorted(by_stamp.keys(), key=str.casefold):
        a = by_stamp[stamp]
        if len(headers) == len(_SHEET_HEADERS):
            out.append(
                [
                    stamp,
                    a.users,
                    a.paid_users,
                    a.active_trials,
                    a.active_trials_connect,
                    a.all_trials,
                    a.all_trials_connect,
                ]
            )
        else:
            out.append(
                [
                    stamp,
                    a.users,
                    a.paid_users,
                    a.active_trials,
                    a.active_trials_connect,
                ]
            )
        totals.users += a.users
        totals.paid_users += a.paid_users
        totals.active_trials += a.active_trials
        totals.active_trials_connect += a.active_trials_connect
        totals.all_trials += a.all_trials
        totals.all_trials_connect += a.all_trials_connect

    if include_totals and by_stamp:
        if len(headers) == len(_SHEET_HEADERS):
            out.append(
                [
                    "ИТОГО",
                    totals.users,
                    totals.paid_users,
                    totals.active_trials,
                    totals.active_trials_connect,
                    totals.all_trials,
                    totals.all_trials_connect,
                ]
            )
        else:
            out.append(
                [
                    "ИТОГО",
                    totals.users,
                    totals.paid_users,
                    totals.active_trials,
                    totals.active_trials_connect,
                ]
            )
    return out


async def collect_ra_export_xlsx_rows(
    *,
    period_start: datetime,
    period_end: datetime,
) -> List[List]:
    """Строки для /export_ra: фильтр create_user в [period_start, period_end)."""
    rows = await _fetch_ra_stamp_user_rows(
        period_start=period_start,
        period_end=period_end,
    )
    export_moment = datetime.now(timezone.utc)
    by_stamp = _aggregate_ra_stamps(rows, export_moment=export_moment)
    return _rows_from_by_stamp(
        by_stamp,
        headers=list(RA_EXPORT_XLSX_HEADERS),
        include_totals=True,
    )


async def _collect_stamp_rows() -> List[List]:
    rows = await _fetch_ra_stamp_user_rows()
    export_moment = datetime.now(timezone.utc)
    by_stamp = _aggregate_ra_stamps(rows, export_moment=export_moment)
    return _rows_from_by_stamp(
        by_stamp,
        headers=list(_SHEET_HEADERS),
        include_totals=False,
    )


def _write_google_sheet(rows: List[List]) -> None:
    import gspread
    from google.oauth2.service_account import Credentials

    if not GOOGLE_PATH_ZOOMER_RA:
        return
    cred_path = GOOGLE_SERVICE_ACCOUNT_FILE
    if not os.path.isfile(cred_path):
        raise FileNotFoundError(f"Google credentials not found: {cred_path}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    client = gspread.authorize(creds)
    sheet_id = _spreadsheet_id_from_env(GOOGLE_PATH_ZOOMER_RA)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1
    worksheet.clear()
    if rows:
        worksheet.update(rows, value_input_option="USER_ENTERED")


async def export_zoomer_ra_google_cron() -> None:
    if not GOOGLE_PATH_ZOOMER_RA:
        logger.debug("GOOGLE_PATH_ZOOMER_RA не задан — выгрузка RA пропущена")
        return
    try:
        logger.info("Выгрузка статистики RA_ в Google Sheets…")
        rows = await _collect_stamp_rows()
        await asyncio.to_thread(_write_google_sheet, rows)
        logger.info(
            "Выгрузка RA_ в Google Sheets завершена: {} меток",
            max(0, len(rows) - 1),
        )
    except Exception:
        logger.exception("Ошибка выгрузки RA_ в Google Sheets")
