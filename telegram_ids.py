"""Различие синтетических id сайта (≤0 в users.user_id) и Telegram user id для ЛС."""

from __future__ import annotations

from typing import Any


def is_telegram_chat_id(user_id: Any) -> bool:
    """
    True, если user_id можно использовать как chat_id для личного сообщения пользователю в Telegram.

    В БД отрицательные user_id — выданные сайту billing id, не чаты Telegram.
    """
    if isinstance(user_id, bool):
        return False
    if isinstance(user_id, int):
        return user_id > 0
    try:
        n = int(user_id)
    except (TypeError, ValueError):
        return False
    return n > 0
