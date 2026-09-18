"""Начисление билетиков розыгрыша за оплату подписки / подарка."""
from __future__ import annotations

from bot import bot, sql
from lexicon import lexicon
from logging_config import logger

PURCHASE_DAYS_TO_TICKETS = {
    30: 1,
    90: 3,
    180: 6,
    365: 12,
    730: 24,
}


def tickets_for_duration_days(panel_days: int) -> int:
    return PURCHASE_DAYS_TO_TICKETS.get(int(panel_days), 0)


async def grant_purchase_tickets(user_id: int, panel_days: int) -> int:
    extra = tickets_for_duration_days(panel_days)
    if extra <= 0:
        return 0
    new_total = await sql.add_tickets(user_id, extra)
    if new_total is None:
        logger.warning("Raffle tickets: user {} not found, skip +{}", user_id, extra)
        return 0
    logger.info(
        "Raffle: +{} ticket(s) for user {} (purchase {} days), total={}",
        extra,
        user_id,
        panel_days,
        new_total,
    )
    return extra


async def notify_purchase_tickets(user_id: int, count: int) -> None:
    if count <= 0 or user_id <= 0:
        return
    try:
        await bot.send_message(
            chat_id=user_id,
            text=lexicon["raffle_tickets_granted"].format(count=count),
        )
    except Exception as e:
        logger.error("Raffle notify tickets uid={}: {}", user_id, e)
