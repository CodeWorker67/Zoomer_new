"""Единая точка: цена с учётом резерва скидки колеса."""
from __future__ import annotations

from config import ADMIN_IDS
from services.wheel_discount import CheckoutQuote, get_checkout_quote, KIND_GIFT, KIND_SUB, KIND_TRAFFIC

ADMIN_TEST_RUB = 1
ADMIN_PLATEGA_TEST_RUB = 50
PLATEGA_PAYLOAD_METHODS = frozenset({"sbp", "card", "crypto"})


async def quote_subscription(user_id: int, duration_key: str) -> CheckoutQuote:
    return await get_checkout_quote(user_id, kind=KIND_SUB, product_key=duration_key)


async def quote_gift(user_id: int, duration_key: str) -> CheckoutQuote:
    return await get_checkout_quote(user_id, kind=KIND_GIFT, product_key=duration_key)


async def quote_traffic(user_id: int, gb: str) -> CheckoutQuote:
    return await get_checkout_quote(user_id, kind=KIND_TRAFFIC, product_key=gb)


def apply_admin_test_price(user_id: int, quote: CheckoutQuote, *, stars: bool = False) -> CheckoutQuote:
    """1 ₽ / 1 ⭐ для теста (не Platega); wdtoken сохраняем — скидка списывается после оплаты."""
    if user_id not in ADMIN_IDS:
        return quote
    if stars:
        return CheckoutQuote(
            base_rub=quote.base_rub,
            final_rub=quote.final_rub,
            base_stars=ADMIN_TEST_RUB,
            final_stars=ADMIN_TEST_RUB,
            percent=quote.percent,
            token=quote.token,
            payload_suffix=quote.payload_suffix,
        )
    return CheckoutQuote(
        base_rub=quote.base_rub,
        final_rub=ADMIN_TEST_RUB,
        base_stars=quote.base_stars,
        final_stars=quote.final_stars,
        percent=quote.percent,
        token=quote.token,
        payload_suffix=quote.payload_suffix,
    )


def apply_admin_platega_test_price(user_id: int, quote: CheckoutQuote) -> CheckoutQuote:
    """50 ₽ для теста Platega (СБП/карта); wdtoken сохраняем."""
    if user_id not in ADMIN_IDS:
        return quote
    return CheckoutQuote(
        base_rub=quote.base_rub,
        final_rub=ADMIN_PLATEGA_TEST_RUB,
        base_stars=quote.base_stars,
        final_stars=quote.final_stars,
        percent=quote.percent,
        token=quote.token,
        payload_suffix=quote.payload_suffix,
    )


def admin_test_payment_ok(payer_user_id: int, paid: int, method: str) -> bool:
    if int(payer_user_id) not in ADMIN_IDS:
        return False
    if method in PLATEGA_PAYLOAD_METHODS:
        return paid == ADMIN_PLATEGA_TEST_RUB
    return paid == ADMIN_TEST_RUB
