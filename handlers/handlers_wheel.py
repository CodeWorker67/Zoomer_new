from aiogram import Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo

from config import WHEEL_MINIAPP_URL

router = Router()


@router.message(Command("wheel"))
async def cmd_wheel(message: Message) -> None:
    url = (WHEEL_MINIAPP_URL or "").strip()
    if not url:
        await message.answer(
            "Мини-приложение колеса не настроено.\n"
            "Добавьте в <code>.env</code> переменную <code>WHEEL_MINIAPP_URL</code> "
            "(HTTPS, например URL из ngrok).",
        )
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🎡 Открыть колесо фортуны",
                    web_app=WebAppInfo(url=url),
                ),
            ],
        ],
    )
    await message.answer("Нажмите кнопку ниже, чтобы открыть колесо:", reply_markup=kb)
