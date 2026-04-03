import hashlib
import hmac
import time
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional

import jwt
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from bot import sql, x3
from config import ADMIN_IDS, BOT_URL, JWT_SECRET, PLATEGA_API_KEY, PLATEGA_MERCHANT_ID, TG_TOKEN
from lexicon import dct_desc, dct_price, lexicon
from logging_config import logger
from payments.pay_platega import PlategaPayment

app = FastAPI(title="Zoomer Web API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://zoomersky.online", "http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

bearer_scheme = HTTPBearer(auto_error=False)

TARIFF_PUBLIC = [
    ("7", "7 дней", 3, False),
    ("30", "30 дней", 3, False),
    ("90", "90 дней", 3, False),
    ("120", "120 дней (акция, автоворонка)", 3, True),
    ("180", "180 дней", 3, False),
    ("white_30", "Mobile 30 дней", 1, False),
]


def _require_jwt_secret() -> str:
    if not JWT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT_SECRET is not configured",
        )
    return JWT_SECRET


def _verify_telegram_login(data: dict[str, Any]) -> None:
    if not TG_TOKEN:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "TG_TOKEN is not configured")
    check_hash = data.get("hash")
    if not check_hash:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing hash")

    auth_date = data.get("auth_date")
    if auth_date is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing auth_date")
    try:
        ts = int(auth_date)
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid auth_date")
    if abs(int(time.time()) - ts) > 86400:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "auth_date expired")

    pairs = []
    for key in sorted(data.keys()):
        if key == "hash":
            continue
        val = data[key]
        if val is None:
            continue
        sval = val if isinstance(val, str) else str(val)
        pairs.append(f"{key}={sval}")
    data_check_string = "\n".join(pairs)
    secret_key = hashlib.sha256(TG_TOKEN.encode()).digest()
    h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
    if h.hexdigest() != check_hash:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid Telegram hash")


def _activ_block(result: dict) -> tuple[bool, Optional[str]]:
    active = str(result.get("activ", "")).startswith("✅")
    t = result.get("time") or "-"
    expires = t if active and t != "-" else None
    return active, expires


def _tariff_parts(tariff_id: str) -> tuple[str, str, bool]:
    desc_key = tariff_id
    white = "white_" in tariff_id
    d = tariff_id
    if white:
        d = d.replace("white_", "", 1)
    if "old" in d:
        d = d.replace("old", "")
    return desc_key, d, white


async def get_jwt_context(
    cred: Annotated[Optional[HTTPAuthorizationCredentials], Depends(bearer_scheme)],
) -> dict[str, Any]:
    if cred is None or not cred.credentials:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    secret = _require_jwt_secret()
    try:
        payload = jwt.decode(cred.credentials, secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    uid = payload.get("user_id")
    if isinstance(uid, (int, float)):
        uid = int(uid)
    elif isinstance(uid, str) and uid.isdigit():
        uid = int(uid)
    else:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    return {"user_id": uid, "username": payload.get("username")}


JwtCtx = Annotated[dict[str, Any], Depends(get_jwt_context)]


class TelegramAuthIn(BaseModel):
    id: int
    auth_date: int
    hash: str
    first_name: str = ""
    last_name: Optional[str] = None
    username: Optional[str] = None
    photo_url: Optional[str] = None


class CreatePaymentIn(BaseModel):
    tariff_id: str
    method: Literal["sbp", "card", "crypto"]
    is_gift: bool = False


@app.post("/api/auth/telegram")
async def auth_telegram(body: TelegramAuthIn):
    data = body.model_dump(exclude_none=True)
    _verify_telegram_login(data)
    uid = body.id

    user_row = await sql.get_user(uid)
    if user_row is None:
        await sql.add_user(uid, False, False)

    secret = _require_jwt_secret()
    exp = datetime.now(timezone.utc) + timedelta(hours=24)
    token = jwt.encode(
        {
            "user_id": uid,
            "username": body.username,
            "exp": exp,
        },
        secret,
        algorithm="HS256",
    )
    if isinstance(token, bytes):
        token = token.decode("utf-8")

    return {
        "token": token,
        "user": {
            "id": uid,
            "first_name": body.first_name or "",
            "username": body.username,
            "photo_url": body.photo_url,
        },
    }


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


@app.get("/api/auth/me")
async def auth_me(ctx: JwtCtx):
    user_id = ctx["user_id"]
    row = await sql.get_user(user_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    subscription_end = row[9]
    is_pay_null = subscription_end is None
    return {
        "id": user_id,
        "username": ctx.get("username"),
        "is_pay_null": is_pay_null,
        "subscription_end_date": _iso(subscription_end),
        "has_discount": bool(row[24]),
        "created_at": _iso(row[6]),
    }


@app.get("/api/user/subscription")
async def user_subscription(ctx: JwtCtx):
    user_id = ctx["user_id"]
    result_pro = await x3.activ(str(user_id))
    result_white = await x3.activ(str(user_id) + "_white")
    pa, pe = _activ_block(result_pro)
    ma, me = _activ_block(result_white)
    return {
        "pro": {"active": pa, "expires": pe},
        "mobile": {"active": ma, "expires": me},
    }


@app.get("/api/user/keys")
async def user_keys(ctx: JwtCtx):
    user_id = ctx["user_id"]
    sub_url = await x3.sublink(str(user_id))
    sub_white = await x3.sublink(str(user_id) + "_white")
    return {
        "pro_url": sub_url or None,
        "mobile_url": sub_white or None,
    }


@app.get("/api/user/referrals")
async def user_referrals(ctx: JwtCtx):
    user_id = ctx["user_id"]
    count = await sql.select_ref_count(user_id)
    base = BOT_URL.rstrip("/")
    link = f"{base}?start=ref{user_id}"
    return {"count": count, "referral_link": link}


@app.get("/api/config/tariffs")
async def config_tariffs():
    out: list[dict[str, Any]] = []
    for tid, label, devices, first_only in TARIFF_PUBLIC:
        if tid not in dct_price:
            continue
        item: dict[str, Any] = {
            "id": tid,
            "label": label,
            "price": dct_price[tid],
            "devices": devices,
        }
        if first_only:
            item["first_payment_only"] = True
        out.append(item)
    return out


@app.post("/api/trial/activate")
async def trial_activate(ctx: JwtCtx):
    user_id = ctx["user_id"]
    user_data = await sql.get_user(user_id)
    in_panel = False
    if user_data is not None and len(user_data) > 4:
        in_panel = user_data[4]
    if in_panel:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Вы уже убедились в надежности нашего VPN"},
        )

    day = 5
    logger.info(await x3.addClient(day, str(user_id), user_id))
    result_active = await x3.activ(str(user_id))
    time_str = result_active["time"]

    if await sql.get_user(user_id) is not None:
        await sql.update_in_panel(user_id)
    else:
        await sql.add_user(user_id, True)

    sub_url = await x3.sublink(str(user_id))
    return {
        "success": True,
        "expires": time_str,
        "subscription_url": sub_url or None,
    }


@app.post("/api/payments/create")
async def payments_create(ctx: JwtCtx, body: CreatePaymentIn):
    user_id = ctx["user_id"]
    tariff_id = body.tariff_id
    if tariff_id not in dct_price:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown tariff")

    desc_key, duration_str, white = _tariff_parts(tariff_id)
    price = dct_price[tariff_id]
    if user_id in ADMIN_IDS:
        price = 1

    method = body.method
    if method == "sbp":
        payment_method = 2
        method_label = "sbp"
    elif method == "card":
        payment_method = 11
        method_label = "card"
    else:
        payment_method = 13
        method_label = "crypto"

    payload = (
        f"user_id:{user_id},duration:{duration_str},white:{white},"
        f"gift:{body.is_gift},method:{method_label},amount:{int(price)}"
    )
    description = (
        f"Подписка в подарок {dct_desc[desc_key]}" if body.is_gift else dct_desc[desc_key]
    )

    if not PLATEGA_API_KEY or not PLATEGA_MERCHANT_ID:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Platega is not configured")

    platega = PlategaPayment(PLATEGA_API_KEY, PLATEGA_MERCHANT_ID)
    result = await platega.create_payment(
        amount=float(price),
        description=description,
        payment_method=payment_method,
        payload=payload,
    )

    if method == "sbp":
        await sql.add_platega_payment(
            user_id, int(price), result["status"], result["id"], payload, is_gift=body.is_gift
        )
    elif method == "card":
        await sql.add_platega_card_payment(
            user_id, int(price), result["status"], result["id"], payload, is_gift=body.is_gift
        )
    else:
        await sql.add_platega_crypto_payment(
            user_id, int(price), result["status"], result["id"], payload, is_gift=body.is_gift
        )

    return {
        "payment_url": result.get("url") or "",
        "payment_id": result.get("id") or "",
    }


@app.get("/api/payments/{transaction_id}/status")
async def payment_status(ctx: JwtCtx, transaction_id: str):
    user_id = ctx["user_id"]
    st = await sql.get_payment_by_transaction_id(transaction_id, user_id)
    if st is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return {"status": st}


@app.post("/api/gifts/{gift_id}/activate")
async def gift_activate(ctx: JwtCtx, gift_id: str):
    user_id = ctx["user_id"]
    result = await sql.activate_gift(gift_id, user_id)

    if not result[0]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=lexicon["gift_no"])

    duration = result[1]
    white_flag = result[2]

    user_id_str = str(user_id)
    if white_flag:
        user_id_str += "_white"

    was_in_db = await sql.get_user(user_id) is not None
    if not was_in_db:
        await sql.add_user(user_id, False)

    existing_user = await x3.get_user_by_username(user_id_str)
    if existing_user and "response" in existing_user and existing_user["response"]:
        response = await x3.updateClient(duration, user_id_str, user_id)
    else:
        response = await x3.addClient(duration, user_id_str, user_id)

    if not response:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=lexicon["gift_error"])

    result_active = await x3.activ(user_id_str)
    subscription_time = result_active.get("time", "-")
    await sql.update_in_panel(user_id)

    return {
        "success": True,
        "days_added": duration,
        "expires": subscription_time,
    }
