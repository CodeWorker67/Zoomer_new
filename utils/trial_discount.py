"""Скидка 20% на первую покупку после триала (field_bool_3, без активной подписки)."""
from __future__ import annotations

from config_bd.models import Users
from config_bd.utils import user_has_any_active_subscription
from lexicon import dct_price
from payments.tariff_gate import normalize_tariff_duration_key

TRIAL_DISCOUNT_PERCENT = 20
TRIAL_DISCOUNT_PAYLOAD_SUFFIX = ",trial_disc"

# Тарифы, на которые не распространяется пост-триальная скидка.
_TRIAL_DISCOUNT_EXCLUDE = frozenset({"30secret", "5000", "5000sale", "30old", "120"})


def trial_discount_applies_to_product(product_key: str) -> bool:
    key = normalize_tariff_duration_key(product_key)
    return key in dct_price and key not in _TRIAL_DISCOUNT_EXCLUDE


def user_eligible_for_trial_discount(user: Users | None) -> bool:
    if user is None or not user.field_bool_3:
        return False
    return not user_has_any_active_subscription(user)


async def is_user_eligible_for_trial_discount(sql, uid: int) -> bool:
    user = await sql.get_user_object_by_user_id(uid)
    return user_eligible_for_trial_discount(user)


def payload_has_trial_discount(payload_parts: dict[str, str]) -> bool:
    return bool(payload_parts.get("trial_disc"))


async def validate_payload_trial_discount(
    payer_user_id: int,
    payload_parts: dict[str, str],
) -> bool:
    if not payload_has_trial_discount(payload_parts):
        return True
    if payload_parts.get("wdpct"):
        return False
    if payload_parts.get("gift", "False") == "True":
        return False
    from bot import sql
    from config import ADMIN_IDS

    user = await sql.get_user_object_by_user_id(payer_user_id)
    if not user_eligible_for_trial_discount(user):
        return False

    raw_duration = str(payload_parts.get("duration", "") or "").strip()
    product_key = normalize_tariff_duration_key(raw_duration)
    if not trial_discount_applies_to_product(product_key):
        return False

    from lexicon import trial_discounted_rub, trial_discounted_stars

    method = payload_parts.get("method", "")
    try:
        paid = int(float(payload_parts.get("amount", 0)))
    except (TypeError, ValueError):
        return False

    if method == "stars":
        expected = trial_discounted_stars(product_key)
    else:
        expected = trial_discounted_rub(product_key)

    admin_test = int(payer_user_id) in ADMIN_IDS and paid == 1
    return paid == expected or admin_test
