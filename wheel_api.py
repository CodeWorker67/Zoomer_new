"""HTTP API для Telegram Mini App «Колесо фортуны»."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Annotated, Any, Literal
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from bot import sql
from config import PAYMENT_MAX_PENDING_PER_USER, TG_TOKEN
from lexicon import dct_desc, lexicon
from payments.gift_pricing import gift_rub_amount_and_desc
from payments.payload_source import MINIAPP
from payments.pay_platega import PLATEGA_CARD_METHOD, PLATEGA_SBP_METHOD, pay, pay_for_gift
from payments.tariff_gate import normalize_tariff_duration_key
from payments.wheel_checkout import apply_admin_test_price, quote_gift, quote_subscription
from services.wheel import wheel_begin_spin, wheel_complete_spin, wheel_public_state, wheel_recent_wins

_WHEEL_CHECKOUT_DURATIONS = frozenset({"7", "30", "90", "180", "365", "730"})

router = APIRouter(prefix="/api/wheel", tags=["wheel"])


def _verify_webapp_init_data(init_data: str) -> dict[str, Any]:
    if not TG_TOKEN:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "TG_TOKEN is not configured")
    if not init_data or not init_data.strip():
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing initData")

    pairs = parse_qsl(init_data, keep_blank_values=True)
    data = dict(pairs)
    received_hash = data.pop("hash", None)
    if not received_hash:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing hash")

    auth_date = data.get("auth_date")
    try:
        ts = int(auth_date)
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid auth_date")
    if abs(int(time.time()) - ts) > 86400:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "initData expired")

    check_lines = [f"{k}={v}" for k, v in sorted(data.items())]
    data_check_string = "\n".join(check_lines)
    secret_key = hmac.new(b"WebAppData", TG_TOKEN.encode(), hashlib.sha256).digest()
    calculated = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if calculated != received_hash:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid initData hash")

    user_raw = data.get("user")
    if not user_raw:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing user")
    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid user payload")
    uid = user.get("id")
    if not isinstance(uid, int):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid user id")
    return {"user_id": uid, "user": user}


class InitDataBody(BaseModel):
    init_data: str = Field(..., min_length=10)


async def wheel_tg_user(body: InitDataBody) -> dict[str, Any]:
    return _verify_webapp_init_data(body.init_data)


WheelUser = Annotated[dict[str, Any], Depends(wheel_tg_user)]


@router.get("/recent-wins")
async def wheel_recent_wins_public(limit: int = Query(24, ge=1, le=50)):
    wins = await wheel_recent_wins(limit=limit)
    return {"wins": wins}


@router.post("/state")
async def wheel_state(_: Request, auth: WheelUser):
    uid = int(auth["user_id"])
    return await wheel_public_state(uid, auth.get("user"))


@router.post("/spin/begin")
async def wheel_spin_begin(_: Request, auth: WheelUser):
    uid = int(auth["user_id"])
    try:
        return await wheel_begin_spin(uid, auth.get("user"))
    except ValueError as e:
        if str(e) == "no_attempts":
            raise HTTPException(status.HTTP_403_FORBIDDEN, "У вас нет попыток!")
        raise


@router.post("/spin/complete")
async def wheel_spin_complete(_: Request, auth: WheelUser):
    uid = int(auth["user_id"])
    try:
        return await wheel_complete_spin(uid, auth.get("user"))
    except ValueError as e:
        if str(e) == "invalid_pending_prize":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Некорректный приз")
        raise


class WheelCheckoutQuoteBody(InitDataBody):
    duration_key: str = Field(..., min_length=1, max_length=16)
    target: Literal["self", "gift"]


class WheelCheckoutCreateBody(WheelCheckoutQuoteBody):
    payment: Literal["sbp", "card"]


def _validate_checkout_duration(duration_key: str) -> str:
    key = normalize_tariff_duration_key(duration_key.strip())
    if key not in _WHEEL_CHECKOUT_DURATIONS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Недоступный тариф")
    return key


async def _wheel_checkout_quote(uid: int, duration_key: str, target: str):
    desc_key = _validate_checkout_duration(duration_key)
    if target == "gift":
        quote = await quote_gift(uid, desc_key)
    else:
        quote = await quote_subscription(uid, desc_key)
    return desc_key, apply_admin_test_price(uid, quote)


@router.post("/checkout/quote")
async def wheel_checkout_quote(_: Request, body: WheelCheckoutQuoteBody, auth: WheelUser):
    uid = int(auth["user_id"])
    desc_key, quote = await _wheel_checkout_quote(uid, body.duration_key, body.target)
    return {
        "duration_key": desc_key,
        "target": body.target,
        "base_rub": quote.base_rub,
        "final_rub": quote.final_rub,
        "discount_percent": quote.percent,
    }


@router.post("/checkout/create")
async def wheel_checkout_create(_: Request, body: WheelCheckoutCreateBody, auth: WheelUser):
    uid = int(auth["user_id"])
    tg_user = auth.get("user") or {}
    desc_key, quote = await _wheel_checkout_quote(uid, body.duration_key, body.target)
    rub_amount = quote.final_rub
    suffix = quote.payload_suffix
    duration = normalize_tariff_duration_key(desc_key)
    user_id = str(uid)
    tg_uname = tg_user.get("username")
    payment_method = PLATEGA_SBP_METHOD if body.payment == "sbp" else PLATEGA_CARD_METHOD

    if body.target == "gift":
        _, gift_des = await gift_rub_amount_and_desc(sql, uid, desc_key)
        payment_info = await pay_for_gift(
            val=str(rub_amount),
            des=gift_des,
            user_id=user_id,
            duration=duration,
            white=False,
            payment_method=payment_method,
            telegram_username=tg_uname,
            source=MINIAPP,
            payload_suffix=suffix,
        )
    else:
        payment_info = await pay(
            val=str(rub_amount),
            des=dct_desc[desc_key],
            user_id=user_id,
            duration=duration,
            white=False,
            payment_method=payment_method,
            telegram_username=tg_uname,
            source=MINIAPP,
            payload_suffix=suffix,
        )

    status_val = payment_info.get("status") or "error"
    if status_val == "rate_limited":
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
        )
    if status_val != "pending" or not payment_info.get("url"):
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Не удалось создать платёж")

    return {
        "status": "pending",
        "amount_rub": rub_amount,
        "payment_url": payment_info["url"],
        "payment_id": payment_info.get("id") or "",
        "target": body.target,
        "duration_key": desc_key,
        "payment": body.payment,
    }
