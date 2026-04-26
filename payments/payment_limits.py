"""Лимиты на создание платежей (антифлуд незавершёнными счетами)."""

from bot import sql
from config import ADMIN_IDS, PAYMENT_MAX_PENDING_PER_USER
from logging_config import logger


async def payment_creation_allowed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    n = await sql.count_open_payment_slots_for_user(user_id)
    if n >= PAYMENT_MAX_PENDING_PER_USER:
        logger.warning(
            "Лимит незавершённых оплат: user_id={} count={} max={}",
            user_id,
            n,
            PAYMENT_MAX_PENDING_PER_USER,
        )
        return False
    return True
