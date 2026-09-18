from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery

from bot import bot, sql
from keyboard import (
    RAFFLE_BACK_CB,
    RAFFLE_CB,
    RAFFLE_PARTICIPATE_CB,
    RAFFLE_TOP_CB,
    keyboard_buy_menu,
    keyboard_raffle,
)
from lexicon import RAFFLE_CAPTION, buy_menu_caption, raffle_top_caption
from logging_config import logger
from utils.menu_photos import menu_photo, raffle_video

router = Router()


async def send_raffle_video_message(
    chat_id: int,
    caption: str = RAFFLE_CAPTION,
    *,
    top: bool = False,
) -> None:
    await bot.send_video(
        chat_id=chat_id,
        video=raffle_video(),
        caption=caption,
        parse_mode="HTML",
        reply_markup=keyboard_raffle(show_back_to_desc=top),
    )


async def _send_raffle_video(
    callback: CallbackQuery,
    caption: str = RAFFLE_CAPTION,
    *,
    top: bool = False,
) -> None:
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_raffle_video_message(callback.message.chat.id, caption, top=top)


async def _edit_raffle_caption(callback: CallbackQuery, caption: str, *, top: bool) -> None:
    try:
        await callback.message.edit_caption(
            caption=caption,
            parse_mode="HTML",
            reply_markup=keyboard_raffle(show_back_to_desc=top),
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        logger.warning("raffle edit_caption failed: {}", e)
        await _send_raffle_video(callback, caption, top=top)


async def _top_caption(user_id: int) -> str:
    rows = await sql.get_top_ticket_holders(10)
    own_tickets = await sql.get_tickets(user_id)
    return raffle_top_caption(rows, own_tickets=own_tickets)


@router.callback_query(F.data == RAFFLE_CB)
async def raffle_open(callback: CallbackQuery):
    await callback.answer()
    await _send_raffle_video(callback)


@router.callback_query(F.data == RAFFLE_PARTICIPATE_CB)
async def raffle_participate(callback: CallbackQuery):
    await callback.answer()
    await bot.send_photo(
        chat_id=callback.message.chat.id,
        photo=menu_photo("buy_subscription"),
        caption=buy_menu_caption(),
        parse_mode="HTML",
        reply_markup=keyboard_buy_menu(),
    )


@router.callback_query(F.data == RAFFLE_TOP_CB)
async def raffle_top(callback: CallbackQuery):
    await callback.answer()
    await _edit_raffle_caption(
        callback,
        await _top_caption(callback.from_user.id),
        top=True,
    )


@router.callback_query(F.data == RAFFLE_BACK_CB)
async def raffle_back(callback: CallbackQuery):
    await callback.answer()
    await _edit_raffle_caption(callback, RAFFLE_CAPTION, top=False)
