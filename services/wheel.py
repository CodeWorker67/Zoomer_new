"""Колесо фортуны: начисление попыток, спин, выдача призов."""
from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import delete, select

from bot import bot, sql, x3
from config_bd.models import AsyncSessionLocal, WheelFortuna
from keyboard import create_kb, keyboard_sub_after_buy
from lexicon import lexicon
from logging_config import logger
from wl_traffic.service import credit_wl_subscription_bonus, fetch_panel_user, reassign_to_active_squad, subscription_bonus_gb, user_on_limited_squad

# Индексы 0..10 — строго как zoomer_wheel/src/data/prizes.js (включая weight=0)
WHEEL_SEGMENTS: list[dict[str, Any]] = [
    {"id": "vpn_2w", "index": 0, "weight": 20, "rim": "2 НЕДЕЛИ"},
    {"id": "cash_100k", "index": 1, "weight": 0, "rim": "100 000 ₽"},
    {"id": "vpn_1m", "index": 2, "weight": 10, "rim": "1 МЕСЯЦ"},
    {"id": "disc_50", "index": 3, "weight": 7, "rim": "СКИДКА"},
    {"id": "disc_10", "index": 4, "weight": 30, "rim": "СКИДКА"},
    {"id": "iphone", "index": 5, "weight": 0, "rim": "IPHONE 17"},
    {"id": "vpn_3m", "index": 6, "weight": 3, "rim": "3 МЕСЯЦА"},
    {"id": "disc_30", "index": 7, "weight": 20, "rim": "СКИДКА"},
    {"id": "secret", "index": 8, "weight": 0, "rim": "СЕКРЕТ"},
    {"id": "cash_1k", "index": 9, "weight": 0, "rim": "1 000 ₽"},
    {"id": "gift", "index": 10, "weight": 10, "rim": "ПОДАРОК"},
]

PRIZE_BY_ID = {p["id"]: p for p in WHEEL_SEGMENTS}

# Свежий pending — завершение только через /spin/complete (защита от двойной выдачи)
_PENDING_AUTO_COMPLETE_AFTER_SEC = 45

PURCHASE_DAYS_TO_ATTEMPTS = {
    180: 1,
    365: 2,
    730: 4,
}

PARTNER_PAID_BATCH = 7

# Лента: только фейковые «1 000 ₽», их число ≈ 7–11% от реальных записей в history.
FAKE_FEED_PRIZE_ID = "cash_1k"
FAKE_FEED_RATIO_MIN = 0.07
FAKE_FEED_RATIO_MAX = 0.11
FAKE_FEED_MAX_ADD_PER_CRON = 20
_FAKE_NAME_LETTERS = "АБВДЕИКЛМНОПРСТУФХЦЧШЭЮЯ"


def telegram_user_full_name(user: dict[str, Any]) -> str:
    first = (user.get("first_name") or "").strip()
    last = (user.get("last_name") or "").strip()
    if first and last:
        return f"{first} {last}"
    return first or last or ""


def telegram_user_username(user: dict[str, Any]) -> Optional[str]:
    raw = (user.get("username") or "").strip().lstrip("@")
    return raw or None


async def wheel_sync_telegram_profile(user_id: int, tg_user: Optional[dict[str, Any]]) -> None:
    if not tg_user:
        return
    await sql.wheel_touch_profile(
        user_id,
        username=telegram_user_username(tg_user),
        full_name=telegram_user_full_name(tg_user),
    )


def mask_winner_name_parts(
    full_name: Optional[str],
    username: Optional[str] = None,
) -> tuple[str, int]:
    source = (full_name or "").strip()
    if not source:
        source = (username or "").strip().lstrip("@")
    if not source:
        return "У", 4
    letter = source[0]
    stars = max(3, min(len(source) - 1, 8)) if len(source) > 1 else 4
    return letter, stars


def mask_winner_display_name(
    full_name: Optional[str],
    username: Optional[str] = None,
) -> str:
    letter, stars = mask_winner_name_parts(full_name, username)
    return f"{letter}{'*' * stars}"


def pick_weighted_prize() -> dict[str, Any]:
    pool = [p for p in WHEEL_SEGMENTS if (p.get("weight") or 0) > 0]
    total = sum(p["weight"] for p in pool)
    roll = random.random() * total
    for entry in pool:
        roll -= entry["weight"]
        if roll <= 0:
            return entry
    return pool[-1]


def _active_attempts(row: WheelFortuna) -> int:
    return max(0, int(row.attempt or 0) - int(row.rotation_number or 0))


def _parse_history(raw: Optional[str]) -> list[dict[str, Any]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def _vpn_days_for_prize(prize_id: str) -> Optional[int]:
    if prize_id == "vpn_2w":
        return 14
    if prize_id == "vpn_1m":
        return 30
    if prize_id == "vpn_3m":
        return 90
    return None


def _recent_win_payload(
    *,
    entry_id: str,
    prize_id: str,
    at: str,
    name_initial: str,
    mask_stars: int,
) -> dict[str, Any]:
    initial = name_initial or "У"
    stars = max(3, int(mask_stars or 4))
    return {
        "id": entry_id,
        "prize_id": prize_id,
        "name_initial": initial,
        "mask_stars": stars,
        "masked_name": f"{initial}{'*' * stars}",
        "time_ago": _time_ago_ru(at) if at else "недавно",
    }


def _fake_feed_target_count(real_wins: int) -> int:
    if real_wins <= 0:
        return 0
    lo = math.ceil(real_wins * FAKE_FEED_RATIO_MIN)
    hi = math.ceil(real_wins * FAKE_FEED_RATIO_MAX)
    if hi < lo:
        hi = lo
    return random.randint(lo, hi)


def _random_fake_name_parts() -> tuple[str, int]:
    letter = random.choice(_FAKE_NAME_LETTERS)
    stars = random.randint(3, 7)
    return letter, stars


def _random_fake_won_at() -> datetime:
    sec_ago = random.randint(120, 72 * 3600)
    return datetime.now(timezone.utc) - timedelta(seconds=sec_ago)


async def _insert_fake_recent_win() -> None:
    initial, stars = _random_fake_name_parts()
    won_at = _random_fake_won_at()
    await sql.wheel_fake_recent_win_insert(
        prize_id=FAKE_FEED_PRIZE_ID,
        name_initial=initial,
        mask_stars=stars,
        won_at=won_at.replace(tzinfo=None),
    )


async def wheel_fake_recent_win_cron() -> None:
    """Поддерживает в БД только cash_1k; количество — случайное 7–11% от реальных выигрышей."""
    try:
        removed = await sql.wheel_fake_recent_win_delete_not_cash_1k()
        real_wins = await sql.wheel_count_history_wins()
        target = _fake_feed_target_count(real_wins)
        fake_count = await sql.wheel_fake_recent_win_count()
    except Exception:
        logger.warning("Wheel fake feed: таблица wheel_fake_recent_wins недоступна (нужна миграция)")
        return

    if removed:
        logger.info("Wheel fake feed: удалено {} записей не cash_1k", removed)

    if fake_count > target:
        await sql.wheel_fake_recent_win_delete_oldest(fake_count - target)
        fake_count = target

    to_add = min(target - fake_count, FAKE_FEED_MAX_ADD_PER_CRON)
    for _ in range(to_add):
        await _insert_fake_recent_win()

    if to_add or (fake_count + to_add) != target:
        logger.info(
            "Wheel fake feed: real_wins={} target={} fake_now≈{} (+{})",
            real_wins,
            target,
            fake_count + to_add,
            to_add,
        )


def _time_ago_ru(iso_ts: str) -> str:
    try:
        raw = iso_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        sec = max(0, int((now - dt).total_seconds()))
    except (TypeError, ValueError):
        return "недавно"
    if sec < 60:
        return "только что"
    if sec < 3600:
        m = sec // 60
        return f"{m} мин. назад"
    if sec < 86400:
        h = sec // 3600
        return f"{h} ч. назад"
    d = sec // 86400
    return f"{d} дн. назад"


def _pending_is_stale(pending_since: Optional[datetime]) -> bool:
    if pending_since is None:
        return True
    age = (datetime.now() - pending_since).total_seconds()
    return age >= _PENDING_AUTO_COMPLETE_AFTER_SEC


async def wheel_auto_complete_pending(user_id: int) -> bool:
    row = await sql.get_wheel_fortuna(user_id)
    if row is None or not row.pending_prize_id:
        return False
    if not _pending_is_stale(row.pending_since):
        return False
    try:
        await wheel_complete_spin(user_id)
        return True
    except ValueError:
        return False


async def wheel_public_state(
    user_id: int,
    tg_user: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    await wheel_sync_telegram_profile(user_id, tg_user)
    await wheel_auto_complete_pending(user_id)
    row = await sql.get_wheel_fortuna(user_id)
    attempt = int(row.attempt) if row else 0
    rotation = int(row.rotation_number) if row else 0
    active = max(0, attempt - rotation)
    partner_friends = await sql.select_partner_count(user_id)
    partner_paid = await sql.select_partner_paid_count(user_id)
    pending_prize_id = row.pending_prize_id if row else None
    pending_index = None
    if pending_prize_id and pending_prize_id in PRIZE_BY_ID:
        pending_index = PRIZE_BY_ID[pending_prize_id]["index"]
    return {
        "active_attempts": active,
        "total_attempts": attempt,
        "rotation_number": rotation,
        "partner_friends_count": partner_friends,
        "partner_paid_count": partner_paid,
        "partner_paid_progress": partner_paid % PARTNER_PAID_BATCH,
        "pending_prize_id": pending_prize_id,
        "pending_win_index": pending_index,
        "discount_10": int(row.discount_10 or 0) if row else 0,
        "discount_30": int(row.discount_30 or 0) if row else 0,
        "discount_50": int(row.discount_50 or 0) if row else 0,
    }


async def wheel_recent_wins(limit: int = 24) -> list[dict[str, Any]]:
    raw_entries = await sql.wheel_collect_recent_history(
        limit_users=500,
        limit_entries=limit,
    )
    merged: list[dict[str, Any]] = []
    for idx, entry in enumerate(raw_entries):
        prize_id = entry.get("prize_id")
        if not prize_id or prize_id not in PRIZE_BY_ID:
            continue
        uid = int(entry["user_id"])
        at = entry.get("at") or ""
        initial, stars = mask_winner_name_parts(
            entry.get("full_name"),
            entry.get("username"),
        )
        merged.append(
            {
                "sort_at": at,
                "payload": _recent_win_payload(
                    entry_id=f"real-{uid}-{at}-{idx}",
                    prize_id=str(prize_id),
                    at=at,
                    name_initial=initial,
                    mask_stars=stars,
                ),
            }
        )

    try:
        fake_entries = await sql.wheel_fake_recent_wins_list(limit=max(limit * 2, 40))
    except Exception:
        fake_entries = []
    for fake in fake_entries:
        prize_id = fake.get("prize_id")
        if not prize_id or prize_id not in PRIZE_BY_ID:
            continue
        at = fake.get("at") or ""
        merged.append(
            {
                "sort_at": at,
                "payload": _recent_win_payload(
                    entry_id=f"fake-{fake['fake_id']}",
                    prize_id=str(prize_id),
                    at=at,
                    name_initial=str(fake.get("name_initial") or "У"),
                    mask_stars=int(fake.get("mask_stars") or 4),
                ),
            }
        )

    merged.sort(key=lambda x: x.get("sort_at") or "", reverse=True)
    return [item["payload"] for item in merged[:limit]]


async def merge_wheel_fortuna_on_account_link(old_billing_uid: int, telegram_user_id: int) -> None:
    """
    Суммирует попытки и статистику колеса при слиянии сайт-аккаунта (user_id < 0) с Telegram.
    """
    if old_billing_uid >= 0 or telegram_user_id <= 0:
        return

    for uid in (old_billing_uid, telegram_user_id):
        row = await sql.get_wheel_fortuna(uid)
        if row and row.pending_prize_id:
            try:
                await wheel_complete_spin(uid)
            except ValueError:
                logger.warning(
                    "Wheel merge: не удалось завершить pending spin user={}",
                    uid,
                )

    async with AsyncSessionLocal() as session:
        old_r = (
            await session.execute(
                select(WheelFortuna).where(WheelFortuna.user_id == old_billing_uid).with_for_update()
            )
        ).scalar_one_or_none()
        new_r = (
            await session.execute(
                select(WheelFortuna).where(WheelFortuna.user_id == telegram_user_id).with_for_update()
            )
        ).scalar_one_or_none()

        if old_r is None:
            await session.commit()
            return

        if new_r is None:
            new_r = WheelFortuna(user_id=telegram_user_id)
            session.add(new_r)
            await session.flush()

        new_r.attempt = int(new_r.attempt or 0) + int(old_r.attempt or 0)
        new_r.rotation_number = int(new_r.rotation_number or 0) + int(old_r.rotation_number or 0)
        new_r.discount_10 = int(new_r.discount_10 or 0) + int(old_r.discount_10 or 0)
        new_r.discount_30 = int(new_r.discount_30 or 0) + int(old_r.discount_30 or 0)
        new_r.discount_50 = int(new_r.discount_50 or 0) + int(old_r.discount_50 or 0)
        new_r.partner_wheel_batches_credited = max(
            int(new_r.partner_wheel_batches_credited or 0),
            int(old_r.partner_wheel_batches_credited or 0),
        )

        merged_hist = _parse_history(old_r.history) + _parse_history(new_r.history)
        merged_hist.sort(key=lambda x: x.get("at") or "", reverse=True)
        new_r.history = json.dumps(merged_hist[:200], ensure_ascii=False)

        if not new_r.pending_prize_id and old_r.pending_prize_id:
            new_r.pending_prize_id = old_r.pending_prize_id
            new_r.pending_since = old_r.pending_since

        if not (new_r.full_name or "").strip() and (old_r.full_name or "").strip():
            new_r.full_name = old_r.full_name
        if not (new_r.username or "").strip() and (old_r.username or "").strip():
            new_r.username = old_r.username

        new_r.updated_at = datetime.now()
        await session.execute(delete(WheelFortuna).where(WheelFortuna.user_id == old_billing_uid))
        await session.commit()
        logger.info(
            "Wheel merged {} -> {} (attempt={}, rotation={})",
            old_billing_uid,
            telegram_user_id,
            new_r.attempt,
            new_r.rotation_number,
        )


async def grant_purchase_wheel_attempts(user_id: int, panel_days: int) -> int:
    extra = PURCHASE_DAYS_TO_ATTEMPTS.get(int(panel_days), 0)
    if extra <= 0:
        return 0
    await sql.wheel_add_attempts(user_id, extra)
    logger.info("Wheel: +{} attempt(s) for user {} (purchase {} days)", extra, user_id, panel_days)
    return extra


async def sync_partner_wheel_attempts(partner_id: int) -> int:
    """+1 попытка за каждые 7 оплативших друзей по partner-ссылке."""
    if partner_id <= 0:
        return 0
    paid = await sql.select_partner_paid_count(partner_id)
    batches = paid // PARTNER_PAID_BATCH
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WheelFortuna).where(WheelFortuna.user_id == partner_id).with_for_update()
        )
        row = result.scalar_one_or_none()
        if row is None:
            row = WheelFortuna(user_id=partner_id)
            session.add(row)
            await session.flush()
        credited = int(row.partner_wheel_batches_credited or 0)
        if batches <= credited:
            await session.commit()
            return 0
        delta = batches - credited
        row.attempt = int(row.attempt or 0) + delta
        row.partner_wheel_batches_credited = batches
        row.updated_at = datetime.now()
        await session.commit()
        logger.info(
            "Wheel: +{} attempt(s) for partner {} (paid referrals {}, batches {})",
            delta,
            partner_id,
            paid,
            batches,
        )
        return delta


async def wheel_begin_spin(
    user_id: int,
    tg_user: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    await wheel_sync_telegram_profile(user_id, tg_user)
    prize = pick_weighted_prize()
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WheelFortuna).where(WheelFortuna.user_id == user_id).with_for_update()
        )
        row = result.scalar_one_or_none()
        if row is None:
            row = WheelFortuna(user_id=user_id)
            session.add(row)
            await session.flush()

        if row.pending_prize_id:
            pid = row.pending_prize_id
            meta = PRIZE_BY_ID.get(pid, prize)
            await session.commit()
            return {
                "prize_id": pid,
                "win_index": meta["index"],
                "rim": meta.get("rim", ""),
                "resumed_pending": True,
            }

        if _active_attempts(row) <= 0:
            await session.commit()
            raise ValueError("no_attempts")

        row.rotation_number = int(row.rotation_number or 0) + 1
        row.pending_prize_id = prize["id"]
        row.pending_since = datetime.now()
        row.updated_at = datetime.now()
        await session.commit()

    return {
        "prize_id": prize["id"],
        "win_index": prize["index"],
        "rim": prize.get("rim", ""),
        "resumed_pending": False,
    }


async def wheel_complete_spin(
    user_id: int,
    tg_user: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    await wheel_sync_telegram_profile(user_id, tg_user)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WheelFortuna).where(WheelFortuna.user_id == user_id).with_for_update()
        )
        row = result.scalar_one_or_none()
        if row is None or not row.pending_prize_id:
            await session.commit()
            return {"prize_id": None, "rim": "", "already_completed": True}

        prize_id = row.pending_prize_id
        if prize_id not in PRIZE_BY_ID or (PRIZE_BY_ID[prize_id].get("weight") or 0) <= 0:
            logger.error("Wheel: invalid pending prize_id={} user={}", prize_id, user_id)
            row.pending_prize_id = None
            row.pending_since = None
            await session.commit()
            raise ValueError("invalid_pending_prize")
        row.pending_prize_id = None
        row.pending_since = None
        row.updated_at = datetime.now()

        hist = _parse_history(row.history)
        hist.append(
            {
                "prize_id": prize_id,
                "at": datetime.now(timezone.utc).isoformat(),
            }
        )
        row.history = json.dumps(hist[-200:], ensure_ascii=False)

        if prize_id == "disc_10":
            row.discount_10 = int(row.discount_10 or 0) + 1
        elif prize_id == "disc_30":
            row.discount_30 = int(row.discount_30 or 0) + 1
        elif prize_id == "disc_50":
            row.discount_50 = int(row.discount_50 or 0) + 1

        await session.commit()

    await _deliver_prize(user_id, prize_id)
    meta = PRIZE_BY_ID.get(prize_id, {})
    return {"prize_id": prize_id, "rim": meta.get("rim", "")}


async def _deliver_prize(user_id: int, prize_id: str) -> None:
    days = _vpn_days_for_prize(prize_id)
    if days is not None:
        await _deliver_vpn_prize(user_id, days)
        return
    if prize_id == "gift":
        await _deliver_gift_prize(user_id)
        return


async def _deliver_vpn_prize(user_id: int, days: int) -> None:
    from payments.process_payload import _apply_panel_subscription
    from X3 import panel_username_for_site_user

    notify_tg: Optional[int] = user_id if user_id > 0 else None
    use_add_client_site = False
    site_email_norm = None
    if user_id > 0:
        user_id_str = str(user_id)
    else:
        row = await sql.get_user(user_id)
        if row and row[15]:
            from config_bd.utils import _norm_email

            site_email_norm = _norm_email(str(row[15]))
            use_add_client_site = True
        user_id_str = panel_username_for_site_user(user_id, False)

    existing_user = await x3.get_user_by_username(user_id_str)
    existed = bool(existing_user and existing_user.get("response"))

    response, _ = await _apply_panel_subscription(
        days,
        user_id_str,
        user_id,
        white_flag=False,
        use_add_client_site=use_add_client_site,
        site_email_norm=site_email_norm,
    )
    if not response:
        logger.error("Wheel VPN prize: panel failed user={} days={}", user_id, days)
        return

    result_active = await x3.activ(user_id_str)
    subscription_time = result_active.get("time", "-")

    if await sql.get_user(user_id) is not None:
        await sql.update_in_panel(user_id)
    else:
        await sql.add_user(user_id, True)

    bonus_gb = subscription_bonus_gb(days)
    if bonus_gb > 0:
        await sql.add_wl_limit(user_id, bonus_gb)
    panel_user = await fetch_panel_user(x3, user_id, sql=sql)
    if panel_user and user_on_limited_squad(panel_user):
        await reassign_to_active_squad(x3, panel_user)
    await credit_wl_subscription_bonus(sql, user_id, days)

    if notify_tg is not None:
        try:
            sub_link = await x3.sublink(user_id_str)
            marker = "продлена" if existed else "активирована"
            text = lexicon["wheel_vpn_success"].format(marker, subscription_time, days, sub_link)
            await bot.send_message(
                chat_id=notify_tg,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard_sub_after_buy(sub_link),
            )
        except Exception as e:
            logger.error("Wheel VPN notify failed user={}: {}", user_id, e)


async def _deliver_gift_prize(user_id: int) -> None:
    duration = 30
    gift_id = await sql.create_gift(user_id, duration, white_flag=False)
    gift_message = lexicon["wheel_payment_gift"].format(duration, "", gift_id)
    if user_id <= 0:
        logger.info("Wheel gift for site user {} gift_id={} (Telegram notify skipped)", user_id, gift_id)
        return
    try:
        await bot.send_message(
            chat_id=user_id,
            text=gift_message,
            disable_web_page_preview=True,
        )
        await bot.send_message(
            chat_id=user_id,
            text=lexicon["wheel_payment_gift_faq"],
            reply_markup=create_kb(1, back_to_main="🔙 Назад"),
        )
        await bot.send_message(
            chat_id=user_id,
            text=lexicon["wheel_payment_gift_web"].format(gift_id),
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error("Wheel gift notify failed user={}: {}", user_id, e)
