from aiogram import Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo

from bot import sql
from config import ADMIN_IDS, WHEEL_MINIAPP_URL

router = Router()


@router.message(Command("wheel"))
async def cmd_wheel(message: Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        return
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


@router.message(Command("add_wheel"))
async def cmd_add_wheel(message: Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (message.text or "").split()
    if len(args) < 3:
        await message.answer("❌ Использование: /add_wheel <tg_id> <кол-во attempt>")
        return

    try:
        target_id = int(args[1])
        add_count = int(args[2])
    except ValueError:
        await message.answer("❌ tg_id и количество attempt должны быть целыми числами.")
        return

    if target_id == 0:
        await message.answer("❌ tg_id не может быть 0.")
        return
    if add_count <= 0:
        await message.answer("❌ Количество attempt должно быть больше 0.")
        return
    if add_count > 1000:
        await message.answer("❌ Слишком большое значение (максимум 1000 за раз).")
        return

    if target_id > 0:
        await sql.ensure_telegram_user_with_partner(target_id, None)

    await sql.wheel_add_attempts(target_id, add_count)
    row = await sql.get_wheel_fortuna(target_id)
    total_attempt = int(row.attempt or 0) if row else add_count
    rotation = int(row.rotation_number or 0) if row else 0
    active = max(0, total_attempt - rotation)

    await message.answer(
        f"✅ Начислено <b>+{add_count}</b> attempt пользователю <code>{target_id}</code>.\n\n"
        f"attempt: <b>{total_attempt}</b>\n"
        f"rotation: <b>{rotation}</b>\n"
        f"активных: <b>{active}</b>",
        parse_mode="HTML",
    )
