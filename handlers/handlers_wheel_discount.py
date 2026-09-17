"""Выбор скидки колеса перед оплатой."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery

from keyboard import (
    keyboard_payment_method,
    keyboard_wl_traffic_payment_method,
    keyboard_wheel_discount,
)
from lexicon import format_wheel_discount_balances, lexicon
from services.wheel_discount import (
    KIND_GIFT,
    KIND_SUB,
    KIND_TRAFFIC,
    activate_wheel_discount,
    build_standard_checkout_text,
    clear_wheel_discount_checkout,
    format_wheel_discount_tariff_line,
    get_checkout_quote,
    wheel_discount_counts,
    wheel_discount_has_any,
    _duration_days_for_product,
)
from utils.menu_ui import edit_or_send_photo
from wl_traffic.texts import format_subscription_wl_bonus_line

router = Router()


def _payment_keyboard(kind: str, product_key: str):
    if kind == KIND_TRAFFIC:
        return keyboard_wl_traffic_payment_method(product_key)
    if kind == KIND_GIFT:
        return keyboard_payment_method(_gift_tariff_cb(product_key))
    return keyboard_payment_method(_sub_tariff_cb(product_key))


def _pay_prompt(kind: str) -> str:
    if kind == KIND_GIFT:
        return lexicon["wheel_discount_pay_gift"]
    return lexicon["wheel_discount_pay_sub"]


async def show_tariff_with_optional_discount(
    callback: CallbackQuery,
    *,
    kind: str,
    product_key: str,
    intro_text: str,
    photo: str,
    append_pay_prompt: bool = True,
) -> None:
    """Скидки колеса — или сразу способы оплаты, если скидок нет."""
    uid = callback.from_user.id
    counts = await wheel_discount_counts(uid)
    if kind == KIND_TRAFFIC:
        base = await format_wheel_discount_tariff_line(uid, kind, product_key)
    else:
        base = intro_text.strip()

    if wheel_discount_has_any(counts):
        text = (
            base
            + "\n\n"
            + format_wheel_discount_balances(
                counts.discount_10,
                counts.discount_30,
                counts.discount_50,
            )
            + "\n\n"
            + lexicon["wheel_discount_warning"]
            + "\n\n"
            + lexicon["wheel_discount_pick"]
        )
        kb = keyboard_wheel_discount(kind, product_key, counts)
    else:
        text = base
        if append_pay_prompt:
            text += "\n\n" + _pay_prompt(kind)
        kb = _payment_keyboard(kind, product_key)

    await edit_or_send_photo(callback, photo, text, kb)


def _parse_pick(data: str) -> tuple[str, str, int] | None:
    if not data.startswith("wd_p:"):
        return None
    parts = data.split(":")
    if len(parts) != 4:
        return None
    kind, product, pct_s = parts[1], parts[2], parts[3]
    if kind not in (KIND_SUB, KIND_GIFT, KIND_TRAFFIC):
        return None
    try:
        pct = int(pct_s)
    except ValueError:
        return None
    if pct not in (0, 10, 30, 50):
        return None
    return kind, product, pct


def _sub_tariff_cb(product_key: str) -> str:
    if product_key == "30secret":
        return "r_30secret"
    return f"r_{product_key}"


def _gift_tariff_cb(product_key: str) -> str:
    return f"gift_r_{product_key}"


def _total_line(quote) -> str:
    if int(quote.percent) > 0:
        return lexicon["wheel_discount_total"].format(
            final_rub=int(quote.final_rub),
            pct=int(quote.percent),
        )
    return lexicon["wheel_discount_total_no_disc"].format(final_rub=int(quote.final_rub))


async def _discount_screen_text(
    uid: int,
    kind: str,
    product_key: str,
    *,
    payment_step: bool = False,
) -> str:
    lines = [lexicon["wheel_discount_intro"], ""]
    lines.append(await format_wheel_discount_tariff_line(uid, kind, product_key))
    if kind in (KIND_SUB, KIND_GIFT):
        wl_bonus = format_subscription_wl_bonus_line(
            _duration_days_for_product(product_key),
        )
        if wl_bonus:
            lines.append(wl_bonus)
    if not payment_step:
        counts = await wheel_discount_counts(uid)
        lines.extend(
            [
                "",
                format_wheel_discount_balances(
                    counts.discount_10,
                    counts.discount_30,
                    counts.discount_50,
                ),
                "",
                lexicon["wheel_discount_warning"],
                "",
                lexicon["wheel_discount_pick"],
            ]
        )
    else:
        lines.append("")
    if payment_step:
        quote = await get_checkout_quote(uid, kind=kind, product_key=product_key)
        lines.extend(["", _total_line(quote)])
    return "\n".join(lines)


@router.callback_query(F.data.startswith("wd_p:"))
async def wheel_discount_pick(callback: CallbackQuery):
    parsed = _parse_pick(callback.data or "")
    if parsed is None:
        await callback.answer()
        return
    kind, product_key, pct = parsed
    uid = callback.from_user.id
    hide_back = False

    if pct == 0:
        await clear_wheel_discount_checkout(uid)
        await callback.answer(lexicon["wheel_discount_none_ok"])
        text = await build_standard_checkout_text(uid, kind, product_key)
        text += "\n\n" + _pay_prompt(kind)
        pay_kb = _payment_keyboard(kind, product_key)
        photo = "buy_traffic" if kind == KIND_TRAFFIC else "buy_subscription"
        await edit_or_send_photo(callback, photo, text, pay_kb)
        return

    counts = await wheel_discount_counts(uid)
    available = {10: counts.discount_10, 30: counts.discount_30, 50: counts.discount_50}[pct]
    if available <= 0:
        await callback.answer(lexicon["wheel_discount_empty"], show_alert=True)
        return
    ok = await activate_wheel_discount(uid, kind=kind, product_key=product_key, percent=pct)
    if not ok:
        await callback.answer(lexicon["wheel_discount_activate_fail"], show_alert=True)
        return
    await callback.answer(lexicon["wheel_discount_applied"].format(pct=pct))
    hide_back = True

    text = await _discount_screen_text(uid, kind, product_key, payment_step=True)
    if kind == KIND_TRAFFIC:
        pay_kb = keyboard_wl_traffic_payment_method(
            product_key,
            hide_back=hide_back,
        )
        photo = "buy_traffic"
        text += "\n\n" + lexicon["wheel_discount_pay_sub"]
    elif kind == KIND_GIFT:
        pay_kb = keyboard_payment_method(_gift_tariff_cb(product_key), hide_back=hide_back)
        photo = "buy_subscription"
        text += "\n\n" + lexicon["wheel_discount_pay_gift"]
    else:
        pay_kb = keyboard_payment_method(_sub_tariff_cb(product_key), hide_back=hide_back)
        photo = "buy_subscription"
        text += "\n\n" + lexicon["wheel_discount_pay_sub"]

    await edit_or_send_photo(callback, photo, text, pay_kb)


@router.callback_query(F.data.startswith("wd_go:"))
async def wheel_discount_go(callback: CallbackQuery):
    data = callback.data or ""
    if not data.startswith("wd_go:"):
        await callback.answer()
        return
    parts = data.split(":", 2)
    if len(parts) != 3:
        await callback.answer()
        return
    _, kind, product_key = parts
    if kind not in (KIND_SUB, KIND_GIFT, KIND_TRAFFIC):
        await callback.answer()
        return

    uid = callback.from_user.id
    await callback.answer()
    photo = "buy_traffic" if kind == KIND_TRAFFIC else "buy_subscription"
    await show_tariff_with_optional_discount(
        callback,
        kind=kind,
        product_key=product_key,
        intro_text=lexicon["wheel_discount_intro"],
        photo=photo,
    )
