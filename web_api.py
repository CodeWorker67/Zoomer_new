import asyncio
import hashlib
import hmac
import secrets
import smtplib
import string
import time
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional

import bcrypt
import jwt
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.exc import IntegrityError

from bot import bot, sql, x3
from config_bd.utils import _norm_email, user_row_to_api_dict
from X3 import panel_username_for_site_user
from config import (
    ADMIN_IDS,
    BOT_URL,
    JWT_SECRET,
    PLATEGA_API_KEY,
    PLATEGA_MERCHANT_ID,
    SMTP_FROM,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USER,
    TG_TOKEN,
)
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
    ("120", "120 дней (акция)", 3, True),
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
    auth = payload.get("auth") or "telegram"
    if auth not in ("telegram", "email"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    return {"user_id": uid, "username": payload.get("username"), "auth": auth}


JwtCtx = Annotated[dict[str, Any], Depends(get_jwt_context)]


async def _user_row_from_jwt(ctx: dict[str, Any]):
    if ctx.get("auth") == "email":
        return await sql.get_user_by_internal_id(ctx["user_id"])
    return await sql.get_user(ctx["user_id"])


async def resolve_telegram_user_id(ctx: dict[str, Any]) -> int:
    row = await _user_row_from_jwt(ctx)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    tg_col = row[1]
    linked = row[28]
    tg: Optional[int] = None
    if tg_col is not None and int(tg_col) > 0:
        tg = int(tg_col)
    elif linked is not None:
        tg = int(linked)
    if tg is None:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Привяжите Telegram-аккаунт для этой операции",
        )
    return tg


async def _panel_vpn_usernames(ctx: dict[str, Any]) -> tuple[str, str]:
    """
    Username в панели для pro и mobile: Telegram id (+ _white) или
    panel_username_for_site_user(user_id) для клиента только с сайта.
    """
    row = await _user_row_from_jwt(ctx)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    tg_col = row[1]
    linked = row[28]
    tg: Optional[int] = None
    if tg_col is not None and int(tg_col) > 0:
        tg = int(tg_col)
    elif linked is not None and int(linked) > 0:
        tg = int(linked)
    if tg is not None:
        s = str(tg)
        return s, f"{s}_white"
    db_uid = int(tg_col)
    return (
        panel_username_for_site_user(db_uid, False),
        panel_username_for_site_user(db_uid, True),
    )


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, password_hash: Optional[str]) -> bool:
    if not password_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        return False


def _issue_jwt(*, user_id: int, auth: str, username: Optional[str]) -> str:
    secret = _require_jwt_secret()
    exp = datetime.now(timezone.utc) + timedelta(hours=24)
    payload: dict[str, Any] = {"user_id": user_id, "auth": auth, "exp": exp}
    if username is not None:
        payload["username"] = username
    token = jwt.encode(payload, secret, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def _random_linking_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


def _random_reset_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _send_smtp_reset_email(to_email: str, code: str) -> None:
    if not SMTP_HOST or not SMTP_FROM:
        raise RuntimeError("SMTP not configured")
    body = f"Код для сброса пароля: {code}\n\nЕсли вы не запрашивали сброс, проигнорируйте письмо."
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = "Сброс пароля"
    msg["From"] = SMTP_FROM
    msg["To"] = to_email
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        if SMTP_USER and SMTP_PASSWORD:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
        s.send_message(msg)


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


class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


class LoginIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


class ResetPasswordIn(BaseModel):
    email: EmailStr


class ConfirmResetIn(BaseModel):
    email: EmailStr
    code: str = Field(min_length=6, max_length=6)
    new_password: str = Field(min_length=1, max_length=256)


class LinkIn(BaseModel):
    code: str = Field(min_length=1, max_length=32)


class ChangePasswordIn(BaseModel):
    current_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=1, max_length=256)


async def _deliver_reset_code(email: str, code: str, row: tuple) -> None:
    tg: Optional[int] = None
    if row[1] is not None and int(row[1]) > 0:
        tg = int(row[1])
    elif row[28] is not None:
        tg = int(row[28])
    smtp_ok = False
    if SMTP_HOST and SMTP_FROM:
        try:
            await asyncio.to_thread(_send_smtp_reset_email, email, code)
            smtp_ok = True
        except Exception as e:
            logger.warning("SMTP password reset failed: {}", e)
    if not smtp_ok and tg is not None:
        try:
            await bot.send_message(tg, f"Код сброса пароля: {code}")
        except Exception as e:
            logger.warning("Telegram password reset failed: {}", e)
    if not smtp_ok and tg is None:
        logger.warning("Password reset code for {} not delivered (configure SMTP or Telegram)", email)


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
            "auth": "telegram",
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


@app.get("/api/user/subscription")
async def user_subscription(ctx: JwtCtx):
    pro_un, white_un = await _panel_vpn_usernames(ctx)
    result_pro = await x3.activ(pro_un)
    result_white = await x3.activ(white_un)
    pa, pe = _activ_block(result_pro)
    ma, me = _activ_block(result_white)
    return {
        "pro": {"active": pa, "expires": pe},
        "mobile": {"active": ma, "expires": me},
    }


@app.get("/api/user/keys")
async def user_keys(ctx: JwtCtx):
    pro_un, white_un = await _panel_vpn_usernames(ctx)
    sub_url = await x3.sublink(pro_un)
    sub_white = await x3.sublink(white_un)
    return {
        "pro_url": sub_url or None,
        "mobile_url": sub_white or None,
    }


@app.get("/api/user/referrals")
async def user_referrals(ctx: JwtCtx):
    user_id = await resolve_telegram_user_id(ctx)
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
    if ctx.get("auth") == "email":
        row = await _user_row_from_jwt(ctx)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
        email = row[18] or ctx.get("username")
        if not email:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Нет email в профиле")
        em = _norm_email(str(email))
        in_panel = bool(row[4])
        if in_panel:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Триал уже взят"},
            )
        billing_uid = int(row[1])
        panel_un = panel_username_for_site_user(billing_uid, False)
        existing_panel = await x3.get_user_by_username(panel_un)
        if existing_panel and existing_panel.get("response"):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"error": "Триал уже взят"},
            )
        day = 5
        ok = await x3.add_client_site(day, em, False, billing_uid)
        if not ok:
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Не удалось активировать триал",
            )
        logger.info("trial site user {} panel username={}", billing_uid, panel_un)
        result_active = await x3.activ(panel_un)
        time_str = result_active["time"]

        if await sql.get_user(billing_uid) is not None:
            await sql.update_in_panel(billing_uid)
        else:
            await sql.add_user(billing_uid, True)

        sub_url = await x3.sublink(panel_un)
        return {
            "success": True,
            "expires": time_str,
            "subscription_url": sub_url or None,
        }

    user_id = await resolve_telegram_user_id(ctx)
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
    row = await _user_row_from_jwt(ctx)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    if ctx.get("auth") == "email":
        em = row[18] or ctx.get("username")
        if not em:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Нет email в профиле")
        payload_user = _norm_email(str(em))
        billing_user_id = int(row[1])
    else:
        billing_user_id = await resolve_telegram_user_id(ctx)
        payload_user = str(billing_user_id)
    tariff_id = body.tariff_id
    if tariff_id not in dct_price:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown tariff")

    desc_key, duration_str, white = _tariff_parts(tariff_id)
    price = dct_price[tariff_id]
    if billing_user_id in ADMIN_IDS:
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
        f"user_id:{payload_user},duration:{duration_str},white:{white},"
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
            billing_user_id, int(price), result["status"], result["id"], payload, is_gift=body.is_gift
        )
    elif method == "card":
        await sql.add_platega_card_payment(
            billing_user_id, int(price), result["status"], result["id"], payload, is_gift=body.is_gift
        )
    else:
        await sql.add_platega_crypto_payment(
            billing_user_id, int(price), result["status"], result["id"], payload, is_gift=body.is_gift
        )

    return {
        "payment_url": result.get("url") or "",
        "payment_id": result.get("id") or "",
    }


@app.get("/api/payments/{transaction_id}/status")
async def payment_status(ctx: JwtCtx, transaction_id: str):
    row = await _user_row_from_jwt(ctx)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    billing_uid = int(row[1])
    st = await sql.get_payment_by_transaction_id(transaction_id, billing_uid)
    if st is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return {"status": st}


@app.post("/api/gifts/{gift_id}/activate")
async def gift_activate(ctx: JwtCtx, gift_id: str):
    user_id = await resolve_telegram_user_id(ctx)
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


@app.post("/api/auth/register")
async def auth_register(body: RegisterIn):
    if await sql.get_user_by_email(str(body.email)):
        raise HTTPException(status.HTTP_409_CONFLICT, "Email already registered")
    h = _hash_password(body.password)
    internal_id = await sql.register_email_user(str(body.email), h)
    em = str(body.email).strip().lower()
    token = _issue_jwt(user_id=internal_id, auth="email", username=em)
    return {"token": token, "user": {"id": internal_id, "email": em}}


@app.post("/api/auth/login")
async def auth_login(body: LoginIn):
    row = await sql.get_user_by_email(str(body.email))
    if row is None or not _verify_password(body.password, row[27]):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid email or password")
    internal_id = int(row[0])
    em = row[18] or str(body.email).strip().lower()
    token = _issue_jwt(user_id=internal_id, auth="email", username=em)
    return {"token": token, "user": {"id": internal_id, "email": em}}


@app.post("/api/auth/reset-password")
async def auth_reset_password(body: ResetPasswordIn):
    row = await sql.get_user_by_email(str(body.email))
    if row is None:
        return {"success": True}
    code = _random_reset_code()
    expires = datetime.now(timezone.utc) + timedelta(minutes=15)
    await sql.replace_password_reset_codes(str(body.email), code, expires)
    await _deliver_reset_code(str(body.email), code, row)
    return {"success": True}


@app.post("/api/auth/confirm-reset")
async def auth_confirm_reset(body: ConfirmResetIn):
    if not body.code.isdigit() or len(body.code) != 6:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid code")
    if not await sql.verify_password_reset_code(str(body.email), body.code):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid or expired code")
    row = await sql.get_user_by_email(str(body.email))
    if row is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid or expired code")
    await sql.set_password_hash_by_internal_id(int(row[0]), _hash_password(body.new_password))
    await sql.delete_password_reset_codes_for_email(str(body.email))
    return {"success": True}


@app.post("/api/auth/generate-linking-code")
async def auth_generate_linking_code(ctx: JwtCtx):
    row = await _user_row_from_jwt(ctx)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    expires = datetime.now(timezone.utc) + timedelta(minutes=15)
    code = ""
    last_err: Optional[Exception] = None
    for _ in range(12):
        code = _random_linking_code()
        try:
            await sql.replace_linking_code(int(row[0]), code, expires)
            last_err = None
            break
        except IntegrityError as e:
            last_err = e
            continue
    if last_err is not None:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Could not generate code")
    return {"success": True, "linkingCode": code}


@app.post("/api/auth/link")
async def auth_link(ctx: JwtCtx, body: LinkIn):
    if ctx.get("auth") != "email":
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Доступно только для входа по email",
        )
    row_e = await sql.get_user_by_internal_id(ctx["user_id"])
    if row_e is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    if row_e[1] is not None and int(row_e[1]) > 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Telegram уже привязан")

    raw = body.code.strip().upper()
    hit = await sql.get_valid_linking_code(raw)
    if hit is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный или просроченный код")
    code_id, creator_internal_id = hit
    if creator_internal_id == row_e[0]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Нельзя использовать свой код")

    creator = await sql.get_user_by_internal_id(creator_internal_id)
    if creator is None:
        await sql.delete_linking_code_by_id(code_id)
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный или просроченный код")
    if creator[1] is None or int(creator[1]) < 0:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Отправьте этот код боту в Telegram",
        )

    ok = await sql.merge_email_placeholder_into_telegram(row_e[0], int(creator[1]))
    if not ok:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Не удалось объединить аккаунты")
    await sql.delete_linking_code_by_id(code_id)
    return {"success": True, "linkedTelegramId": int(creator[1])}


@app.get("/api/user/profile")
async def user_profile(ctx: JwtCtx):
    if ctx.get("auth") == "email":
        user = await sql.get_user_object_by_internal_id(int(ctx["user_id"]))
    else:
        user = await sql.get_user_object_by_user_id(int(ctx["user_id"]))
    if user is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    return user_row_to_api_dict(user)


@app.post("/api/user/change-password")
async def user_change_password(ctx: JwtCtx, body: ChangePasswordIn):
    row = await _user_row_from_jwt(ctx)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    if not row[27]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль не установлен")
    if not _verify_password(body.current_password, row[27]):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный текущий пароль")
    await sql.set_password_hash_by_internal_id(int(row[0]), _hash_password(body.new_password))
    return {"success": True}
