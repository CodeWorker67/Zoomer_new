"""Выгрузка статистики по Yandex Direct (метки /start YD…, users.yandex_id)."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import select

from config_bd.models import AsyncSessionLocal, Users
from sheduler.zoomer_ra_google_export import _subscription_active_at

MSK = ZoneInfo("Europe/Moscow")

YD_EXPORT_XLSX_HEADERS: Tuple[str, ...] = (
    "Ключ",
    "Всего пользователей",
    "С подпиской",
    "По промокоду",
    "По промокоду(актив)",
    "Без подписки",
)


@dataclass
class _YdStampAgg:
    users: int = 0
    with_subscription: int = 0
    by_promo: int = 0
    by_promo_active: int = 0
    no_subscription: int = 0


def _msk_day_bounds_as_utc_naive(day: date) -> Tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=MSK)
    end = start + timedelta(days=1)
    return (
        start.astimezone(timezone.utc).replace(tzinfo=None),
        end.astimezone(timezone.utc).replace(tzinfo=None),
    )


def yesterday_msk_bounds_utc_naive() -> Tuple[datetime, datetime]:
    today_msk = datetime.now(MSK).date()
    return _msk_day_bounds_as_utc_naive(today_msk - timedelta(days=1))


def _aggregate_yd_stamps(
    rows,
    *,
    export_moment: datetime,
) -> Dict[str, _YdStampAgg]:
    by_stamp: Dict[str, _YdStampAgg] = defaultdict(_YdStampAgg)
    for _user_id, stamp, reserve_field, in_panel, sub_end in rows:
        key = (stamp or "").strip()
        if not key:
            continue
        agg = by_stamp[key]
        agg.users += 1
        if reserve_field:
            agg.with_subscription += 1
        if in_panel:
            agg.by_promo += 1
            if _subscription_active_at(sub_end, at=export_moment):
                agg.by_promo_active += 1
        else:
            agg.no_subscription += 1
    return by_stamp


def _rows_from_by_stamp(
    by_stamp: Dict[str, _YdStampAgg],
    *,
    include_totals: bool,
) -> List[List]:
    out: List[List] = [list(YD_EXPORT_XLSX_HEADERS)]
    totals = _YdStampAgg()
    for stamp in sorted(by_stamp.keys(), key=str.casefold):
        a = by_stamp[stamp]
        out.append(
            [
                stamp,
                a.users,
                a.with_subscription,
                a.by_promo,
                a.by_promo_active,
                a.no_subscription,
            ]
        )
        totals.users += a.users
        totals.with_subscription += a.with_subscription
        totals.by_promo += a.by_promo
        totals.by_promo_active += a.by_promo_active
        totals.no_subscription += a.no_subscription

    if include_totals and by_stamp:
        out.append(
            [
                "ИТОГО",
                totals.users,
                totals.with_subscription,
                totals.by_promo,
                totals.by_promo_active,
                totals.no_subscription,
            ]
        )
    return out


async def _fetch_yd_stamp_user_rows(
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
            Users.subscription_end_date,
        ).where(
            Users.yandex_id.isnot(None),
            Users.yandex_id != "",
        )
        if period_start is not None:
            stmt = stmt.where(Users.create_user >= period_start)
        if period_end is not None:
            stmt = stmt.where(Users.create_user < period_end)
        return (await session.execute(stmt)).all()


async def collect_yd_export_sheet_rows(
    *,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
) -> List[List]:
    rows = await _fetch_yd_stamp_user_rows(
        period_start=period_start,
        period_end=period_end,
    )
    export_moment = datetime.now(timezone.utc)
    by_stamp = _aggregate_yd_stamps(rows, export_moment=export_moment)
    return _rows_from_by_stamp(by_stamp, include_totals=True)


async def collect_yd_export_workbook_rows() -> Tuple[List[List], List[List]]:
    """Листы ОБЩИЙ и ВЧЕРА (регистрация вчера по календарю МСК)."""
    general = await collect_yd_export_sheet_rows()
    y_start, y_end = yesterday_msk_bounds_utc_naive()
    yesterday = await collect_yd_export_sheet_rows(
        period_start=y_start,
        period_end=y_end,
    )
    return general, yesterday
