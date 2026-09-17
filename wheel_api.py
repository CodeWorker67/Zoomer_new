"""HTTP API для Telegram Mini App «Колесо фортуны»."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Annotated, Any
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from config import TG_TOKEN
from services.wheel import wheel_begin_spin, wheel_complete_spin, wheel_public_state, wheel_recent_wins

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
