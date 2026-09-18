"""Ссылки t.me на мини-апп колеса и уведомления о начислении попыток."""
from __future__ import annotations

from config import BOT_URL
from bot import bot
from logging_config import logger
from lexicon import lexicon


def wheel_miniapp_tme_url() -> str:
    bot_url = (BOT_URL or "").lower()
    if "zoomerskyvpn_bot" in bot_url:
        return "https://t.me/zoomerskyvpn_bot/wheel"
    return "https://t.me/TestWorkza_bot/wheel"


def wheel_attempts_word(count: int) -> str:
    n = abs(int(count)) % 100
    if 11 <= n <= 14:
        return "вращений"
    last = n % 10
    if last == 1:
        return "вращение"
    if 2 <= last <= 4:
        return "вращения"
    return "вращений"


async def notify_purchase_wheel_attempts(user_id: int, count: int) -> None:
    if count <= 0 or user_id <= 0:
        return
    wheel_url = wheel_miniapp_tme_url()
    text = lexicon["wheel_attempts_granted"].format(
        count=count,
        word=wheel_attempts_word(count),
        wheel_url=wheel_url,
    )
    try:
        await bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error("Wheel notify purchase uid={}: {}", user_id, e)


async def notify_partner_wheel_attempts(
    partner_id: int,
    count: int,
    paid_friends: int,
) -> None:
    if count <= 0 or partner_id <= 0:
        return
    wheel_url = wheel_miniapp_tme_url()
    text = lexicon["wheel_partner_friend_paid"].format(
        paid=paid_friends,
        count=count,
        word=wheel_attempts_word(count),
        wheel_url=wheel_url,
    )
    try:
        await bot.send_message(
            chat_id=partner_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error("Wheel notify partner uid={}: {}", partner_id, e)
