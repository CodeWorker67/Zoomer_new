import asyncio

from aiogram import Router

from logging_config import logger
from telegram_ids import is_telegram_chat_id
from handlers.handlers_raffle import send_raffle_video_message

router = Router()

_START_RAFFLE_DELAY_SEC = 30 * 60


def schedule_start_prize(user_id: int) -> None:
    if not is_telegram_chat_id(user_id):
        return
    asyncio.create_task(_send_start_raffle_later(user_id))


async def _send_start_raffle_later(user_id: int) -> None:
    try:
        await asyncio.sleep(_START_RAFFLE_DELAY_SEC)
        await send_raffle_video_message(user_id)
        logger.info("start_raffle: отправлено user_id={}", user_id)
    except Exception:
        logger.exception("start_raffle: не удалось отправить user_id={}", user_id)
