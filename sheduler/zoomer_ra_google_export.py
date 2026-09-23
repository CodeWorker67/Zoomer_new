"""Периодическая выгрузка статистики по меткам RA_ в Google Sheets."""
from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy import func, select

from config import GOOGLE_PATH_ZOOMER_RA, GOOGLE_SERVICE_ACCOUNT_FILE
from config_bd.models import AsyncSessionLocal, Users
from handlers.handlers_statistic import _load_all_successful_payments
from logging_config import logger

_SHEET_HEADERS = [
    "ключ",
    "пользователи",
    "оплаты",
    "действующие триалы",
    "действующие триалы, которые подключили впн",
    "все триалы",
    "все триалы, которые подключали впн",
]


def _spreadsheet_id_from_env(value: str) -> str:
    value = value.strip()
    if "/d/" in value:
        return value.split("/d/", 1)[1].split("/", 1)[0]
    return value


@dataclass
class _StampAgg:
    users: int = 0
    first_payments_sum: int = 0
    active_trials: int = 0
    active_trials_connect: int = 0
    all_trials: int = 0
    all_trials_connect: int = 0


def _first_payment_rub_by_user(all_payments) -> Dict[int, Tuple[int, datetime]]:
    best: Dict[int, Tuple[int, datetime]] = {}
    for p in all_payments:
        prev = best.get(p.user_id)
        if prev is None or p.time_created < prev[1]:
            best[p.user_id] = (p.amount_rub, p.time_created)
    return {uid: (amt, tc) for uid, (amt, tc) in best.items()}


async def _collect_stamp_rows() -> List[List]:
    async with AsyncSessionLocal() as session:
        stmt = select(
            Users.user_id,
            Users.stamp,
            Users.reserve_field,
            Users.in_panel,
            Users.is_connect,
        ).where(
            Users.is_delete == False,
            # В ILIKE «_» — один любой символ; нужна подстрока «ra_», не «ra» + символ.
            func.strpos(func.lower(Users.stamp), "ra_") > 0,
        )
        rows = (await session.execute(stmt)).all()
        all_payments = await _load_all_successful_payments(session)
    first_pay = _first_payment_rub_by_user(all_payments)

    by_stamp: Dict[str, _StampAgg] = defaultdict(_StampAgg)
    for user_id, stamp, reserve_field, in_panel, is_connect in rows:
        key = (stamp or "").strip()
        if not key or "ra_" not in key.lower():
            continue
        agg = by_stamp[key]
        agg.users += 1
        fp = first_pay.get(user_id)
        if fp is not None:
            agg.first_payments_sum += fp[0]

        if in_panel:
            agg.all_trials += 1
            if is_connect:
                agg.all_trials_connect += 1

        if in_panel and not reserve_field:
            agg.active_trials += 1
            if is_connect:
                agg.active_trials_connect += 1

    out: List[List] = [_SHEET_HEADERS]
    for stamp in sorted(by_stamp.keys(), key=str.casefold):
        a = by_stamp[stamp]
        out.append(
            [
                stamp,
                a.users,
                a.first_payments_sum,
                a.active_trials,
                a.active_trials_connect,
                a.all_trials,
                a.all_trials_connect,
            ]
        )
    return out


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
