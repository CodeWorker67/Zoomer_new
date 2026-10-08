"""Загрузка офлайн-конверсий в Яндекс Метрику после успешной оплаты."""
from __future__ import annotations

import time
from decimal import Decimal, ROUND_HALF_UP
import aiohttp

from bot import bot, sql
from config import (
    CHECKER_ID,
    LEAD_TRACKER_STAR_RUB_PER_STAR,
    YANDEX_ACCESS_TOKEN,
    YANDEX_METRIKA_COUNTER_ID,
    YANDEX_METRIKA_GOAL_TARGET,
)
from logging_config import logger

_UPLOAD_URL = (
    "https://api-metrika.yandex.net/management/v1/counter"
    f"/{YANDEX_METRIKA_COUNTER_ID}/offline_conversions/upload"
)


def is_enabled() -> bool:
    return bool((YANDEX_ACCESS_TOKEN or "").strip())


def _payment_rub(method: str, amount: int | float) -> int:
    if method == "stars":
        stars = Decimal(str(amount))
        rate = Decimal(str(LEAD_TRACKER_STAR_RUB_PER_STAR))
        rub = (stars * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return int(rub)
    return int(Decimal(str(amount)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _build_offline_csv(client_id: str, target: str, dt_sec: int, price_rub: int) -> str:
    return (
        "ClientId,Target,DateTime,Price,Currency\n"
        f"{client_id},{target},{dt_sec},{price_rub},RUB\n"
    )


async def report_landing_payment_to_metrica(
    billing_user_id: int,
    method: str,
    amount: int | float,
) -> None:
    """
    Если у плательщика есть yandex_id (ClientID Метрики) — загружает офлайн-конверсию
    и шлёт уведомление в CHECKER_ID.
    """
    if not is_enabled():
        logger.debug("Yandex Metrica: пропуск, YANDEX_ACCESS_TOKEN не задан")
        return

    try:
        user = await sql.get_user_object_by_user_id(billing_user_id)
    except Exception as e:
        logger.error("Yandex Metrica: не удалось загрузить user_id={}: {}", billing_user_id, e)
        return

    if user is None:
        return

    yandex_id = (user.yandex_id or "").strip()
    if not yandex_id:
        return

    price_rub = _payment_rub(method, amount)
    dt_sec = int(time.time())
    target = YANDEX_METRIKA_GOAL_TARGET
    csv_body = _build_offline_csv(yandex_id, target, dt_sec, price_rub)
    token = (YANDEX_ACCESS_TOKEN or "").strip()
    headers = {"Authorization": f"OAuth {token}"}

    logger.info(
        "Yandex Metrica: загрузка офлайн-конверсии user_id={} ClientId={} Target={} Price={} RUB",
        billing_user_id,
        yandex_id,
        target,
        price_rub,
    )

    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            form = aiohttp.FormData()
            form.add_field(
                "file",
                csv_body.encode("utf-8"),
                filename="offline-conversions.csv",
                content_type="text/csv",
            )
            async with session.post(_UPLOAD_URL, headers=headers, data=form) as resp:
                text = await resp.text()
                if resp.status not in (200, 201):
                    logger.warning(
                        "Yandex Metrica: ответ HTTP {} user_id={} ClientId={} body={}",
                        resp.status,
                        billing_user_id,
                        yandex_id,
                        text[:1500],
                    )
                else:
                    logger.info(
                        "Yandex Metrica: успешно HTTP {} user_id={} ClientId={} body={}",
                        resp.status,
                        billing_user_id,
                        yandex_id,
                        text[:1500],
                    )
    except Exception as e:
        logger.error(
            "Yandex Metrica: ошибка POST user_id={} ClientId={}: {}",
            billing_user_id,
            yandex_id,
            e,
        )

    if CHECKER_ID is not None:
        try:
            await bot.send_message(
                chat_id=CHECKER_ID,
                text=f"Клиент с лендинга оплатил {price_rub} рублей",
            )
        except Exception as e:
            logger.error(
                "Yandex Metrica: ошибка уведомления CHECKER_ID user_id={}: {}",
                billing_user_id,
                e,
            )
