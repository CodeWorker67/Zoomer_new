"""Скользящий лимит апдейтов на пользователя (защита от флуда и лишней нагрузки)."""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, Deque, Dict, Optional, Set

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, TelegramObject, Update

from lexicon import lexicon
from logging_config import logger


def _user_id_from_update(update: Update) -> Optional[int]:
    if update.message and update.message.from_user:
        return update.message.from_user.id
    if update.edited_message and update.edited_message.from_user:
        return update.edited_message.from_user.id
    if update.callback_query and update.callback_query.from_user:
        return update.callback_query.from_user.id
    if update.inline_query and update.inline_query.from_user:
        return update.inline_query.from_user.id
    if update.chosen_inline_result and update.chosen_inline_result.from_user:
        return update.chosen_inline_result.from_user.id
    if update.shipping_query and update.shipping_query.from_user:
        return update.shipping_query.from_user.id
    if update.pre_checkout_query and update.pre_checkout_query.from_user:
        return update.pre_checkout_query.from_user.id
    if update.poll_answer and update.poll_answer.user:
        return update.poll_answer.user.id
    if update.my_chat_member and update.my_chat_member.from_user:
        return update.my_chat_member.from_user.id
    if update.chat_member and update.chat_member.from_user:
        return update.chat_member.from_user.id
    if update.chat_join_request and update.chat_join_request.from_user:
        return update.chat_join_request.from_user.id
    return None


class UserThrottleMiddleware(BaseMiddleware):
    """
    Не более ``max_per_window`` апдейтов от одного user_id за ``window_sec`` секунд.
    По умолчанию 25 за 8 с — обычный тап по меню не режется, скриптовый спам — да.
    """

    def __init__(
        self,
        *,
        max_per_window: int,
        window_sec: float,
        bypass_user_ids: Set[int],
    ) -> None:
        self._max = max_per_window
        self._window = window_sec
        self._bypass = bypass_user_ids
        self._hits: dict[int, Deque[float]] = defaultdict(deque)

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        if not isinstance(event, Update):
            return await handler(event, data)

        uid = _user_id_from_update(event)
        if uid is None or uid in self._bypass:
            return await handler(event, data)

        now = time.monotonic()
        q = self._hits[uid]
        cutoff = now - self._window
        while q and q[0] < cutoff:
            q.popleft()

        if len(q) >= self._max:
            cq = event.callback_query
            if cq is not None:
                try:
                    await cq.answer(lexicon["throttle_callback"], show_alert=False)
                except Exception as e:
                    logger.debug("throttle callback.answer: {}", e)
            logger.warning("User {} throttled (>{}/{}s)", uid, self._max, self._window)
            return None

        q.append(now)
        return await handler(event, data)
