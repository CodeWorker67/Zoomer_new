"""Скидки колеса: активация при выборе (сразу −1), оплата по активной сессии."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from bot import sql
from config import ADMIN_IDS
from logging_config import logger
from payments.gift_pricing import gift_rub_amount_and_desc, regular_rub_amount
from payments.tariff_gate import normalize_tariff_duration_key
from lexicon import lexicon
from wl_traffic.constants import WL_TRAFFIC_TARIFFS
from wl_traffic.service import parse_traffic_duration
from wl_traffic.texts import format_pro_payment_link

CHECKOUT_TTL_MINUTES = 30

KIND_SUB = "sub"
KIND_GIFT = "gift"
KIND_TRAFFIC = "traffic"

_STARS_PRICES = {
    "7": 99,
    "30": 299,
    "30secret": 149,
    "90": 749,
    "120": 749,
    "180": 1349,
    "365": 2399,
    "730": 3699,
    "5000": 4990,
    "5000sale": 2790,
    "30old": 99,
}


def _stars_for_duration(duration_key: str) -> int:
    dur = normalize_tariff_duration_key(duration_key)
    return int(_STARS_PRICES.get(dur, 0))


@dataclass
class WheelDiscountCounts:
    discount_10: int
    discount_30: int
    discount_50: int


@dataclass
class CheckoutQuote:
    base_rub: int
    final_rub: int
    base_stars: int
    final_stars: int
    percent: int
    token: Optional[str]
    payload_suffix: str


async def wheel_discount_counts(user_id: int) -> WheelDiscountCounts:
    row = await sql.get_wheel_fortuna(user_id)
    if not row:
        return WheelDiscountCounts(0, 0, 0)
    return WheelDiscountCounts(
        int(row.discount_10 or 0),
        int(row.discount_30 or 0),
        int(row.discount_50 or 0),
    )


def wheel_discount_has_any(counts: WheelDiscountCounts) -> bool:
    return bool(counts.discount_10 or counts.discount_30 or counts.discount_50)


def _discounted_amount(base: int, percent: int) -> int:
    if percent <= 0:
        return base
    return max(1, int(round(base * (100 - percent) / 100)))


async def base_rub_for_product(user_id: int, kind: str, product_key: str) -> int:
    if kind == KIND_TRAFFIC:
        return int(WL_TRAFFIC_TARIFFS.get(product_key, 0))
    if kind == KIND_GIFT:
        amount, _ = await gift_rub_amount_and_desc(sql, user_id, product_key)
        return int(amount)
    return int(regular_rub_amount(product_key))


def base_stars_for_product(kind: str, product_key: str) -> int:
    if kind == KIND_TRAFFIC:
        return int(WL_TRAFFIC_TARIFFS.get(product_key, 0))
    return _stars_for_duration(product_key)


def product_duration_label(kind: str, product_key: str) -> str:
    if kind == KIND_TRAFFIC:
        return f"{product_key} GB"
    if product_key in ("5000", "5000sale"):
        return "Навсегда"
    if product_key == "30secret":
        return "30 дней"
    try:
        int(product_key)
    except ValueError:
        return product_key
    return f"{product_key} дней"


def _duration_days_for_product(product_key: str) -> int:
    if product_key == "30secret":
        return 30
    if product_key in ("5000", "5000sale"):
        return 5000
    return int(product_key)


async def format_wheel_discount_tariff_inline(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    if kind in (KIND_SUB, KIND_GIFT):
        quote = await get_checkout_quote(user_id, kind=kind, product_key=product_key)
        price = quote.final_rub
    else:
        price = await base_rub_for_product(user_id, kind, product_key)
    if kind == KIND_TRAFFIC:
        return lexicon["wheel_discount_tariff_traffic"].format(
            gb=product_key,
            price=price,
        )
    duration = product_duration_label(kind, product_key)
    if kind == KIND_GIFT:
        return lexicon["wheel_discount_tariff_gift_inline"].format(
            duration=duration,
            price=price,
        )
    return lexicon["wheel_discount_tariff_sub_inline"].format(
        duration=duration,
        price=price,
    )


async def format_wheel_discount_tariff_line(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    return await format_wheel_discount_tariff_inline(user_id, kind, product_key)


async def build_pro_checkout_intro(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    from lexicon import TRIAL_DISCOUNT_BANNER
    from utils.trial_discount import TRIAL_DISCOUNT_PAYLOAD_SUFFIX

    tariff_summary = await format_wheel_discount_tariff_inline(user_id, kind, product_key)
    days = _duration_days_for_product(product_key)
    text = format_pro_payment_link(days, tariff_summary=tariff_summary)
    if kind == KIND_SUB:
        quote = await get_checkout_quote(user_id, kind=kind, product_key=product_key)
        if quote.payload_suffix == TRIAL_DISCOUNT_PAYLOAD_SUFFIX:
            text = f"{TRIAL_DISCOUNT_BANNER}\n\n{text}"
    return text


async def build_standard_checkout_text(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    """Текст оплаты без блока «скидка с колеса» (как у пользователя без скидок)."""
    if kind == KIND_TRAFFIC:
        return await format_wheel_discount_tariff_line(user_id, kind, product_key)
    if kind == KIND_SUB and product_key == "30secret":
        summary = await format_wheel_discount_tariff_inline(user_id, kind, product_key)
        return lexicon["secret_tariff_checkout_intro"].format(tariff_summary=summary)
    if kind in (KIND_SUB, KIND_GIFT):
        return await build_pro_checkout_intro(user_id, kind, product_key)
    return ""


def _payload_suffix(kind: str, product_key: str, percent: int, base_rub: int) -> str:
    return f",wdpct:{percent},wdkind:{kind},wdprod:{product_key},wdbase:{base_rub}"


async def activate_wheel_discount(
    user_id: int,
    *,
    kind: str,
    product_key: str,
    percent: int,
) -> bool:
    if percent not in (10, 30, 50):
        return False
    await sql.wheel_discount_checkout_expire()
    base_rub = await base_rub_for_product(user_id, kind, product_key)
    base_stars = base_stars_for_product(kind, product_key)
    final_rub = _discounted_amount(base_rub, percent)
    final_stars = _discounted_amount(base_stars, percent)
    expires = datetime.now() + timedelta(minutes=CHECKOUT_TTL_MINUTES)
    return await sql.wheel_discount_checkout_activate(
        user_id,
        kind=kind,
        product_key=product_key,
        percent=percent,
        base_rub=base_rub,
        final_rub=final_rub,
        base_stars=base_stars,
        final_stars=final_stars,
        expires_at=expires,
    )


async def clear_wheel_discount_checkout(user_id: int) -> None:
    await sql.wheel_discount_checkout_expire()
    await sql.wheel_discount_checkout_clear(user_id)


async def get_checkout_quote(
    user_id: int,
    *,
    kind: str,
    product_key: str,
) -> CheckoutQuote:
    await sql.wheel_discount_checkout_expire()
    base_rub = await base_rub_for_product(user_id, kind, product_key)
    base_stars = base_stars_for_product(kind, product_key)
    active = await sql.wheel_discount_checkout_get(user_id)
    if (
        active
        and str(active.kind) == kind
        and str(active.product_key) == product_key
        and int(active.percent) in (10, 30, 50)
    ):
        pct = int(active.percent)
        return CheckoutQuote(
            base_rub=int(active.base_rub),
            final_rub=int(active.final_rub),
            base_stars=int(active.base_stars),
            final_stars=int(active.final_stars),
            percent=pct,
            token=None,
            payload_suffix=_payload_suffix(kind, product_key, pct, int(active.base_rub)),
        )
    quote = CheckoutQuote(
        base_rub=base_rub,
        final_rub=base_rub,
        base_stars=base_stars,
        final_stars=base_stars,
        percent=0,
        token=None,
        payload_suffix="",
    )
    return await _apply_trial_discount_quote(user_id, kind, product_key, quote)


async def _apply_trial_discount_quote(
    user_id: int,
    kind: str,
    product_key: str,
    quote: CheckoutQuote,
) -> CheckoutQuote:
    if kind != KIND_SUB or quote.percent in (10, 30, 50):
        return quote
    from lexicon import trial_discounted_rub, trial_discounted_stars
    from utils.trial_discount import (
        TRIAL_DISCOUNT_PAYLOAD_SUFFIX,
        TRIAL_DISCOUNT_PERCENT,
        is_user_eligible_for_trial_discount,
        trial_discount_applies_to_product,
    )

    if not await is_user_eligible_for_trial_discount(sql, user_id):
        return quote
    if not trial_discount_applies_to_product(product_key):
        return quote
    key = normalize_tariff_duration_key(product_key)
    return CheckoutQuote(
        base_rub=quote.base_rub,
        final_rub=trial_discounted_rub(key),
        base_stars=quote.base_stars,
        final_stars=trial_discounted_stars(key),
        percent=TRIAL_DISCOUNT_PERCENT,
        token=None,
        payload_suffix=TRIAL_DISCOUNT_PAYLOAD_SUFFIX,
    )


def _payment_ref(payload: str, transaction_id: Optional[str]) -> str:
    if transaction_id:
        return f"tx:{transaction_id}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    return f"pl:{digest}"


def _payload_matches_product(
    payload_parts: dict[str, str],
    *,
    kind: str,
    product_key: str,
) -> bool:
    raw_duration = str(payload_parts.get("duration", "") or "")
    is_gift = payload_parts.get("gift", "False") == "True"
    traffic_gb = parse_traffic_duration(raw_duration)

    if kind == KIND_TRAFFIC:
        return traffic_gb is not None and str(traffic_gb) == str(product_key) and not is_gift
    if kind == KIND_GIFT:
        if not is_gift:
            return False
        dur = normalize_tariff_duration_key(raw_duration)
        return dur == normalize_tariff_duration_key(product_key)
    if kind == KIND_SUB:
        if is_gift or traffic_gb is not None:
            return False
        dur = normalize_tariff_duration_key(raw_duration)
        return dur == normalize_tariff_duration_key(product_key)
    return False


def _checkout_from_payload(payload_parts: dict[str, str]) -> tuple[str, str, int] | None:
    pct_s = (payload_parts.get("wdpct") or "").strip()
    kind = (payload_parts.get("wdkind") or "").strip()
    prod = (payload_parts.get("wdprod") or "").strip()
    if not pct_s or not kind or not prod:
        return None
    try:
        pct = int(pct_s)
    except ValueError:
        return None
    if pct not in (10, 30, 50) or kind not in (KIND_SUB, KIND_GIFT, KIND_TRAFFIC):
        return None
    return kind, prod, pct


async def validate_payload_discount(
    payer_user_id: int,
    payload_parts: dict[str, str],
    *,
    payload: str = "",
    transaction_id: Optional[str] = None,
) -> bool:
    parsed = _checkout_from_payload(payload_parts)
    if parsed is None:
        return True

    kind, product_key, pct = parsed
    payment_ref = _payment_ref(payload or "", transaction_id)
    if await sql.wheel_discount_redemption_exists(payment_ref):
        return True

    active = await sql.wheel_discount_checkout_get(payer_user_id)
    if active is None:
        logger.error("Wheel discount: нет активной скидки uid={}", payer_user_id)
        return False
    if (
        str(active.kind) != kind
        or str(active.product_key) != product_key
        or int(active.percent) != pct
    ):
        logger.error("Wheel discount: активная скидка не совпадает с payload uid={}", payer_user_id)
        return False

    if not _payload_matches_product(payload_parts, kind=kind, product_key=product_key):
        logger.error("Wheel discount: тариф в payload не совпадает uid={}", payer_user_id)
        return False

    method = payload_parts.get("method", "")
    paid = int(float(payload_parts.get("amount", 0)))
    expected = int(active.final_stars if method == "stars" else active.final_rub)
    admin_test = int(payer_user_id) in ADMIN_IDS and paid == 1
    if paid != expected and not admin_test:
        logger.error(
            "Wheel discount: сумма {} != {} uid={}",
            paid,
            expected,
            payer_user_id,
        )
        return False
    return True


async def commit_payload_discount(
    payer_user_id: int,
    payload: str,
    payload_parts: dict[str, str],
    *,
    transaction_id: Optional[str] = None,
) -> bool:
    parsed = _checkout_from_payload(payload_parts)
    if parsed is None:
        return True
    if not await validate_payload_discount(
        payer_user_id,
        payload_parts,
        payload=payload,
        transaction_id=transaction_id,
    ):
        return False
    kind, product_key, pct = parsed
    ref = _payment_ref(payload, transaction_id)
    ok = await sql.wheel_discount_checkout_complete(
        payer_user_id,
        payment_ref=ref,
        payload=payload,
        kind=kind,
        product_key=product_key,
        percent=pct,
    )
    if ok:
        logger.info("Wheel discount checkout complete uid={} ref={}", payer_user_id, ref)
    else:
        logger.error("Wheel discount checkout complete failed uid={}", payer_user_id)
    return ok


def payer_id_from_payload_parts(payload_parts: dict[str, str]) -> Optional[int]:
    raw = str(payload_parts.get("user_id", "") or "").strip()
    if not raw or "@" in raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
