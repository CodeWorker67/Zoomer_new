"""Landing site API — /api/landing/* (OTP email, Google, tariffs, payments)."""
from __future__ import annotations

import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional
from urllib.parse import urlparse

import aiohttp
import bcrypt
import jwt
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field

from bot import sql, x3
from lead_tracker import post_user_trial
from X3 import panel_username_for_site_user
from config import (
    ADMIN_IDS,
    BOT_URL,
    JWT_SECRET,
    LANDING_GOOGLE_CLIENT_ID,
    LANDING_PARTNER_MIN,
    LANDING_PARTNER_PROCENT,
    LANDING_SITE_URL,
    PAYMENT_MAX_PENDING_PER_USER,
    PLATEGA_API_KEY,
    PLATEGA_MERCHANT_ID,
    SUPPORT_URL,
)
from config_bd.utils import _norm_email
from lexicon import dct_desc, dct_price, lexicon
from logging_config import logger
from payments.payload_source import SITE
from payments.pay_platega import pay_site_card, pay_site_sbp
from services.unisender import send_email as send_unisender_email

landing_router = APIRouter(prefix="/api/landing", tags=["landing"])

LANDING_JWT_MAX_AGE = 365 * 24 * 3600  # 1 year
LANDING_COOKIE = "landing_auth"

TARIFF_PUBLIC = [
    ("5000", "Навсегда", 5, False),
    ("730", "2 года", 5, False),
    ("365", "365 дней", 5, False),
    ("180", "180 дней", 5, False),
    ("90", "90 дней", 5, False),
    ("30", "30 дней", 5, False),
    ("7", "7 дней", 5, False),
]

_rate_limits: dict[str, list[float]] = {}
bearer_scheme = HTTPBearer(auto_error=False)


def _rate_check(key: str, max_requests: int, window_sec: int) -> bool:
    now = time.time()
    timestamps = [t for t in _rate_limits.get(key, []) if now - t < window_sec]
    if len(timestamps) >= max_requests:
        _rate_limits[key] = timestamps
        return False
    timestamps.append(now)
    _rate_limits[key] = timestamps
    return True


def _rate_limit_or_raise(request_ip: str, action: str, max_req: int = 5, window: int = 300):
    key = f"landing:{action}:{request_ip}"
    if not _rate_check(key, max_req, window):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Слишком много попыток. Подождите 5 минут.")


def _site_url_from_request(request: Request) -> Optional[str]:
    origin = (request.headers.get("origin") or "").strip()
    if origin:
        return origin.rstrip("/")
    referer = (request.headers.get("referer") or "").strip()
    if referer:
        parsed = urlparse(referer)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return None


def _client_ip(request: Request) -> str:
    x_real = (request.headers.get("x-real-ip") or "").strip()
    if x_real:
        return x_real
    xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if xff:
        return xff
    return request.client.host if request.client else ""


def _client_is_https(request: Request) -> bool:
    proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    if proto == "https":
        return True
    return request.url.scheme == "https"


def _cookie_params(request: Request) -> tuple[Literal["lax", "none"], bool]:
    if _client_is_https(request):
        return "none", True
    return "lax", False


def _require_jwt_secret() -> str:
    if not JWT_SECRET:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "JWT_SECRET is not configured")
    return JWT_SECRET


def _issue_landing_jwt(*, user_id: int, auth: str, username: Optional[str]) -> str:
    secret = _require_jwt_secret()
    exp = datetime.now(timezone.utc) + timedelta(seconds=LANDING_JWT_MAX_AGE)
    payload: dict[str, Any] = {
        "user_id": user_id,
        "auth": auth,
        "site": "landing",
        "exp": exp,
    }
    if username is not None:
        payload["username"] = username
    token = jwt.encode(payload, secret, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def _set_landing_cookie(request: Request, response, token: str) -> None:
    samesite, secure = _cookie_params(request)
    response.set_cookie(
        key=LANDING_COOKIE,
        value=token,
        httponly=True,
        secure=secure,
        samesite=samesite,
        max_age=LANDING_JWT_MAX_AGE,
        path="/",
    )


def _clear_landing_cookie(request: Request, response) -> None:
    samesite, secure = _cookie_params(request)
    response.delete_cookie(key=LANDING_COOKIE, path="/", secure=secure, httponly=True, samesite=samesite)


def _auth_response(request: Request, token: str, user: dict, **extra) -> JSONResponse:
    body = {"token": token, "user": user, **extra}
    resp = JSONResponse(content=body)
    resp.headers["X-Auth-Token"] = token
    _set_landing_cookie(request, resp, token)
    return resp


async def get_landing_jwt_context(
    request: Request,
    cred: Annotated[Optional[HTTPAuthorizationCredentials], Depends(bearer_scheme)],
) -> dict[str, Any]:
    raw_token = None
    if cred and cred.credentials:
        raw_token = cred.credentials
    else:
        raw_token = request.cookies.get(LANDING_COOKIE)
    if not raw_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    secret = _require_jwt_secret()
    try:
        payload = jwt.decode(raw_token, secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    if payload.get("site") != "landing":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    uid = payload.get("user_id")
    if isinstance(uid, (int, float)):
        uid = int(uid)
    elif isinstance(uid, str) and uid.isdigit():
        uid = int(uid)
    else:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    auth = payload.get("auth") or "email"
    if auth not in ("email", "google"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    return {"user_id": uid, "username": payload.get("username"), "auth": auth}


LandingCtx = Annotated[dict[str, Any], Depends(get_landing_jwt_context)]


def _random_otp_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


async def _send_landing_otp(email: str) -> None:
    code = _random_otp_code()
    expires = datetime.now(timezone.utc) + timedelta(minutes=15)
    activation_value = f"{code}:{int(expires.timestamp())}"
    await sql.set_landing_activation_pass_by_email(email, activation_value)
    body = f"Ваш код для входа: {code}\n\nКод действителен 15 минут."
    try:
        await send_unisender_email(
            to_email=email,
            subject="Код входа — Зумерский VPN",
            text=body,
        )
    except Exception as e:
        logger.warning("Landing OTP email failed: {}", e)
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "Не удалось отправить код на почту. Попробуйте позже.",
        ) from e


def _landing_user_dict(user, site) -> dict[str, Any]:
    return {
        "id": int(user.id),
        "email": site.email,
        "auth": "google" if site.google_sub else "email",
        "billing_user_id": int(user.user_id),
        "has_password": bool(site.password),
    }


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
    except ValueError:
        return False


def _validate_password_pair(password: str, confirm: str) -> None:
    if len(password) < 4:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль должен быть не короче 4 символов")
    if password != confirm:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароли не совпадают")


def _tariff_parts(tariff_id: str) -> tuple[str, str, bool]:
    white = "white" in tariff_id
    desc_key = tariff_id.replace("_white", "")
    return desc_key, desc_key, white


def _reject_mobile_purchase(tariff_id: str) -> None:
    if _tariff_parts(tariff_id)[2]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, lexicon["mobile_purchase_disabled"])


def _activ_block(result: dict) -> tuple[bool, Optional[str]]:
    active = str(result.get("activ", "")).startswith("✅")
    t = result.get("time") or "-"
    expires = t if active and t != "-" else None
    return active, expires


async def _landing_user_pair(ctx: LandingCtx):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    return pair


async def _landing_panel_username(ctx: LandingCtx) -> str:
    user, _site = await _landing_user_pair(ctx)
    return panel_username_for_site_user(int(user.user_id), False)


def _parse_partner_ref(raw: Optional[str]) -> Optional[str]:
    """Парсит partner id из ?start=partner_{id} или голого id."""
    if not raw:
        return None
    value = str(raw).strip()
    if value.startswith("partner_"):
        value = value.replace("partner_", "", 1)
    try:
        pid = int(value)
    except ValueError:
        return None
    if pid == 0:
        return None
    return str(pid)


class EmailIn(BaseModel):
    email: EmailStr
    partner: Optional[str] = None


class VerifyCodeIn(BaseModel):
    email: EmailStr
    code: str = Field(min_length=6, max_length=6)


class GoogleAuthIn(BaseModel):
    credential: str
    partner: Optional[str] = None


class CreatePaymentIn(BaseModel):
    tariff_id: str
    method: Literal["sbp", "card"]
    is_gift: bool = False


class PasswordLoginIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=4)


class SetPasswordIn(BaseModel):
    password: str = Field(min_length=4)
    password_confirm: str = Field(min_length=4)


class ChangePasswordIn(BaseModel):
    current_password: str = Field(min_length=4)
    new_password: str = Field(min_length=4)
    password_confirm: str = Field(min_length=4)


class RemovePasswordIn(BaseModel):
    current_password: str = Field(min_length=4)


@landing_router.post("/auth/check-email")
async def landing_check_email(body: EmailIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "check-email", max_req=20, window=300)
    em = str(body.email).strip().lower()
    pair = await sql.get_landing_user_by_email(em)
    has_password = bool(pair and pair[1].password)
    return {"email": em, "has_password": has_password}


@landing_router.post("/auth/password-login")
async def landing_password_login(body: PasswordLoginIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "password-login", max_req=10, window=300)
    em = str(body.email).strip().lower()
    pair = await sql.get_landing_user_by_email(em)
    if pair is None or not _verify_password(body.password, pair[1].password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Неверный email или пароль")
    user, site = pair
    internal_id = int(user.id)
    await sql.set_landing_email_verified(internal_id, True)
    token = _issue_landing_jwt(user_id=internal_id, auth="email", username=em)
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.post("/auth/send-code")
async def landing_send_code(body: EmailIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "send-code", max_req=5, window=300)
    em = str(body.email).strip().lower()
    existing = await sql.get_landing_user_by_email(em)
    if existing is None:
        partner_raw = body.partner or request.query_params.get("start")
        partner = _parse_partner_ref(partner_raw) or ""
        await sql.register_landing_email_user(
            em,
            site_url=_site_url_from_request(request),
            partner=partner,
        )
    await _send_landing_otp(em)
    return {"success": True, "email": em}


@landing_router.post("/auth/verify-code")
async def landing_verify_code(body: VerifyCodeIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "verify-code", max_req=10, window=300)
    if not body.code.isdigit() or len(body.code) != 6:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    pair = await sql.get_landing_user_by_email(str(body.email))
    if pair is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пользователь не найден")
    user, site = pair
    activation = site.activation_pass
    if not activation or ":" not in str(activation):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Код не был отправлен")
    stored_code, expires_ts = str(activation).rsplit(":", 1)
    try:
        if int(time.time()) > int(expires_ts):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Код истёк, запросите новый")
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    if stored_code != body.code:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный код")
    internal_id = int(user.id)
    await sql.set_landing_email_verified(internal_id, True)
    await sql.set_landing_activation_pass_by_email(str(body.email), None)
    em = site.email or str(body.email).strip().lower()
    token = _issue_landing_jwt(user_id=internal_id, auth="email", username=em)
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.post("/auth/resend-code")
async def landing_resend_code(body: EmailIn, request: Request):
    _rate_limit_or_raise(_client_ip(request), "resend-code", max_req=3, window=300)
    em = str(body.email).strip().lower()
    pair = await sql.get_landing_user_by_email(em)
    if pair is None:
        return {"success": True}
    await _send_landing_otp(em)
    return {"success": True}


@landing_router.post("/auth/google")
async def landing_google(body: GoogleAuthIn, request: Request):
    if not LANDING_GOOGLE_CLIENT_ID:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Google login not configured")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://oauth2.googleapis.com/tokeninfo?id_token={body.credential}"
        ) as resp:
            if resp.status != 200:
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid Google token")
            payload = await resp.json()
    if payload.get("aud") != LANDING_GOOGLE_CLIENT_ID:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid Google token audience")
    google_email = payload.get("email")
    google_sub = payload.get("sub")
    if not google_email or not payload.get("email_verified") or not google_sub:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Google email not verified")
    em = google_email.strip().lower()
    pair = await sql.get_landing_user_by_google_sub(google_sub)
    if pair is None:
        by_email = await sql.get_landing_user_by_email(em)
        if by_email:
            user, site = by_email
            internal_id = int(user.id)
        else:
            partner_raw = body.partner or request.query_params.get("start")
            partner = _parse_partner_ref(partner_raw) or ""
            internal_id = await sql.register_landing_google_user(
                em,
                google_sub,
                site_url=_site_url_from_request(request),
                partner=partner,
            )
            pair = await sql.get_landing_user_by_internal_id(internal_id)
            if pair is None:
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "User creation failed")
            user, site = pair
    else:
        user, site = pair
    internal_id = int(user.id)
    token = _issue_landing_jwt(user_id=internal_id, auth="google", username=em)
    return _auth_response(request, token, _landing_user_dict(user, site), success=True)


@landing_router.post("/auth/logout")
async def landing_logout(request: Request):
    resp = JSONResponse(content={"success": True})
    _clear_landing_cookie(request, resp)
    return resp


@landing_router.get("/auth/me")
async def landing_me(ctx: LandingCtx):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    user, site = pair
    return _landing_user_dict(user, site)


@landing_router.get("/config/tariffs")
async def landing_tariffs():
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


@landing_router.post("/payments/create")
async def landing_payments_create(ctx: LandingCtx, body: CreatePaymentIn):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    user, site = pair
    billing_user_id = int(user.user_id)
    payload_user = str(billing_user_id)
    tariff_id = body.tariff_id
    if tariff_id not in dct_price:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Unknown tariff")
    _reject_mobile_purchase(tariff_id)
    desc_key, duration_str, white = _tariff_parts(tariff_id)
    price = dct_price[tariff_id]
    if billing_user_id in ADMIN_IDS:
        price = 1
    if body.method == "sbp" and (not PLATEGA_API_KEY or not PLATEGA_MERCHANT_ID):
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Platega is not configured")
    if body.method == "card" and (not PLATEGA_API_KEY or not PLATEGA_MERCHANT_ID):
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Platega is not configured")
    description = (
        f"Подписка в подарок {dct_desc[desc_key]}" if body.is_gift else dct_desc[desc_key]
    )
    success_url = f"{LANDING_SITE_URL}/success"
    fail_url = f"{LANDING_SITE_URL}/checkout"
    site_uname = ctx.get("username") if isinstance(ctx.get("username"), str) else None
    if body.method == "card":
        result = await pay_site_card(
            val=str(price),
            des=description,
            payload_user=payload_user,
            billing_user_id=billing_user_id,
            duration=duration_str,
            white=white,
            is_gift=body.is_gift,
            telegram_username=site_uname,
            payload_source=SITE,
            return_url=success_url,
            failed_url=fail_url,
        )
    else:
        result = await pay_site_sbp(
            val=str(price),
            des=description,
            payload_user=payload_user,
            billing_user_id=billing_user_id,
            duration=duration_str,
            white=white,
            is_gift=body.is_gift,
            telegram_username=site_uname,
            payload_source=SITE,
            return_url=success_url,
            failed_url=fail_url,
        )
    if result["status"] == "rate_limited":
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
        )
    if result["status"] != "pending":
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Не удалось создать платёж")
    return {
        "payment_url": result.get("url") or "",
        "payment_id": result.get("id") or "",
    }


@landing_router.get("/user/password-status")
async def landing_password_status(ctx: LandingCtx):
    _user, site = await _landing_user_pair(ctx)
    return {"has_password": bool(site.password)}


@landing_router.post("/user/set-password")
async def landing_set_password(ctx: LandingCtx, body: SetPasswordIn):
    _validate_password_pair(body.password, body.password_confirm)
    user, site = await _landing_user_pair(ctx)
    if site.password:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль уже установлен")
    ok = await sql.set_landing_password_by_internal_id(int(user.id), _hash_password(body.password))
    if not ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось сохранить пароль")
    return {"success": True, "has_password": True}


@landing_router.post("/user/change-password")
async def landing_change_password(ctx: LandingCtx, body: ChangePasswordIn):
    user, site = await _landing_user_pair(ctx)
    if not site.password:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль не установлен")
    if not _verify_password(body.current_password, site.password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный текущий пароль")
    _validate_password_pair(body.new_password, body.password_confirm)
    ok = await sql.set_landing_password_by_internal_id(int(user.id), _hash_password(body.new_password))
    if not ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось сохранить пароль")
    return {"success": True, "has_password": True}


@landing_router.post("/user/remove-password")
async def landing_remove_password(ctx: LandingCtx, body: RemovePasswordIn):
    user, site = await _landing_user_pair(ctx)
    if not site.password:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Пароль не установлен")
    if not _verify_password(body.current_password, site.password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Неверный текущий пароль")
    ok = await sql.clear_landing_password_by_internal_id(int(user.id))
    if not ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось удалить пароль")
    return {"success": True, "has_password": False}


@landing_router.get("/user/subscription")
async def landing_user_subscription(ctx: LandingCtx):
    panel_un = await _landing_panel_username(ctx)
    result_pro = await x3.activ(panel_un)
    active, expires = _activ_block(result_pro)
    return {
        "active": active,
        "expires": expires,
        "pro": {"active": active, "expires": expires},
    }


@landing_router.get("/user/keys")
async def landing_user_keys(ctx: LandingCtx):
    panel_un = await _landing_panel_username(ctx)
    sub_url = await x3.sublink(panel_un)
    return {
        "subscription_url": sub_url or None,
        "pro_url": sub_url or None,
    }


@landing_router.post("/trial/activate")
async def landing_trial_activate(ctx: LandingCtx):
    user, site = await _landing_user_pair(ctx)
    billing_uid = int(user.user_id)
    if user.in_panel:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Триал уже был активирован"},
        )
    panel_un = panel_username_for_site_user(billing_uid, False)
    existing_panel = await x3.get_user_by_username(panel_un)
    if existing_panel and existing_panel.get("response"):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Триал уже был активирован"},
        )
    em = site.email or ctx.get("username")
    if not em:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Нет email в профиле")
    day = 1
    ok = await x3.add_client_site(day, str(em).strip().lower(), False, billing_uid)
    if not ok:
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Не удалось активировать триал",
        )
    logger.info("landing trial user {} panel username={}", billing_uid, panel_un)
    result_active = await x3.activ(panel_un)
    time_str = result_active["time"]
    if await sql.get_user(billing_uid) is not None:
        await sql.update_in_panel(billing_uid)
    else:
        await sql.add_user(billing_uid, True)
    await sql.init_wl_trial_limits(billing_uid)
    sub_url = await x3.sublink(panel_un)
    await post_user_trial(billing_uid)
    return {
        "success": True,
        "expires": time_str,
        "subscription_url": sub_url or None,
    }


@landing_router.get("/payments/{transaction_id}/status")
async def landing_payment_status(ctx: LandingCtx, transaction_id: str):
    pair = await sql.get_landing_user_by_internal_id(ctx["user_id"])
    if pair is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    billing_uid = int(pair[0].user_id)
    st = await sql.get_payment_by_transaction_id(transaction_id, billing_uid)
    if st is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return {"status": st}


@landing_router.get("/user/partner")
async def landing_user_partner(ctx: LandingCtx, request: Request):
    user, _site = await _landing_user_pair(ctx)
    billing_uid = int(user.user_id)
    referrals = await sql.select_partner_count(billing_uid)
    payments_sum = await sql.select_partner_referrals_payments_sum(billing_uid)
    balance = user.partner_balance or 0
    paid_out = user.partner_pay or 0
    total_earned = balance + paid_out
    site_base = (_site_url_from_request(request) or LANDING_SITE_URL).rstrip("/")
    partner_code = str(billing_uid)
    site_link = f"{site_base}/?start=partner_{partner_code}"
    telegram_link = f"{BOT_URL}?start=partner_{partner_code}"
    referrals_list = await sql.select_partner_referrals_list(billing_uid)
    return {
        "referrals": referrals,
        "payments_sum": payments_sum,
        "balance": balance,
        "paid_out": paid_out,
        "total_earned": total_earned,
        "percent": LANDING_PARTNER_PROCENT,
        "min_withdraw": LANDING_PARTNER_MIN,
        "can_withdraw": balance >= LANDING_PARTNER_MIN,
        "partner_code": partner_code,
        "site_link": site_link,
        "telegram_link": telegram_link,
        "support_url": SUPPORT_URL,
        "referrals_list": referrals_list,
    }
