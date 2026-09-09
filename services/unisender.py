"""Unisender Go: HTTP API with SMTP fallback."""
from __future__ import annotations

import asyncio
import html as html_lib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

import aiohttp
from loguru import logger

from config import (
    SMTP_FROM,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USER,
    UNISENDER_API_KEY,
    UNISENDER_API_URL,
    UNISENDER_FROM_NAME,
)

_SEND_URL = f"{UNISENDER_API_URL}/email/send.json"
_TIMEOUT = aiohttp.ClientTimeout(total=20)
_SMTP_TIMEOUT = 20


def is_http_configured() -> bool:
    return bool(UNISENDER_API_KEY and SMTP_FROM)


def is_smtp_configured() -> bool:
    return bool(SMTP_HOST and SMTP_USER and SMTP_PASSWORD and SMTP_FROM)


def is_configured() -> bool:
    return is_http_configured() or is_smtp_configured()


def _html_from_text(text: str) -> str:
    escaped = html_lib.escape(text).replace("\n", "<br>\n")
    return f"<p>{escaped}</p>"


def _payload(*, to_email: str, subject: str, text: str, from_name: str, skip_unsubscribe: int) -> dict[str, Any]:
    return {
        "message": {
            "recipients": [{"email": to_email}],
            "from_email": SMTP_FROM,
            "from_name": from_name,
            "subject": subject,
            "body": {
                "plaintext": text,
                "html": _html_from_text(text),
            },
            "skip_unsubscribe": skip_unsubscribe,
            "template_engine": "none",
            "global_language": "ru",
        }
    }


def _should_fallback_to_smtp(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError)):
        return True
    if isinstance(exc, aiohttp.ClientError):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc).lower()
        if "rejected recipient" in msg or "failed_emails" in msg:
            return False
        if "unisender error:" in msg and "http" not in msg:
            return False
        if "unisender http 4" in msg:
            return False
        return True
    return False


async def _post_send(payload: dict[str, Any]) -> dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-API-KEY": UNISENDER_API_KEY or "",
    }
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.post(_SEND_URL, json=payload, headers=headers) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                raw = await resp.text()
                raise RuntimeError(f"Unisender HTTP {resp.status}: {raw[:500]}") from None
            if resp.status != 200:
                raise RuntimeError(f"Unisender HTTP {resp.status}: {data}")
            if not isinstance(data, dict):
                raise RuntimeError(f"Unisender unexpected response: {data}")
            if data.get("status") != "success":
                raise RuntimeError(f"Unisender error: {data}")
            failed = data.get("failed_emails") or {}
            if failed:
                raise RuntimeError(f"Unisender rejected recipient: {failed}")
            return data


async def _send_via_http(*, to_email: str, subject: str, text: str, from_name: str) -> None:
    try:
        await _post_send(_payload(to_email=to_email, subject=subject, text=text, from_name=from_name, skip_unsubscribe=1))
    except RuntimeError as e:
        if "skip_unsubscribe" not in str(e).lower():
            raise
        await _post_send(_payload(to_email=to_email, subject=subject, text=text, from_name=from_name, skip_unsubscribe=0))


def _send_via_smtp_sync(*, to_email: str, subject: str, text: str, from_name: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{SMTP_FROM}>"
    msg["To"] = to_email
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(_html_from_text(text), "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=_SMTP_TIMEOUT) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(SMTP_USER or "", SMTP_PASSWORD or "")
        smtp.sendmail(SMTP_FROM or "", [to_email], msg.as_string())


async def _send_via_smtp(*, to_email: str, subject: str, text: str, from_name: str) -> None:
    await asyncio.to_thread(
        _send_via_smtp_sync,
        to_email=to_email,
        subject=subject,
        text=text,
        from_name=from_name,
    )


async def send_email(
    *,
    to_email: str,
    subject: str,
    text: str,
    from_name: Optional[str] = None,
) -> None:
    if not is_configured():
        raise RuntimeError("Email is not configured (UNISENDER_API_KEY / SMTP_* / SMTP_FROM)")

    name = (from_name or UNISENDER_FROM_NAME).strip() or "Зумерский VPN"
    http_error: Optional[BaseException] = None

    if is_http_configured():
        try:
            await _send_via_http(to_email=to_email, subject=subject, text=text, from_name=name)
            return
        except Exception as e:
            if not _should_fallback_to_smtp(e) or not is_smtp_configured():
                raise
            http_error = e
            logger.warning("Unisender HTTP unavailable ({!r}), falling back to SMTP", e)

    if is_smtp_configured():
        try:
            await _send_via_smtp(to_email=to_email, subject=subject, text=text, from_name=name)
            return
        except Exception as smtp_error:
            if http_error is not None:
                raise RuntimeError(
                    f"Unisender HTTP failed ({http_error}); SMTP failed ({smtp_error})"
                ) from smtp_error
            raise

    if http_error is not None:
        raise http_error
    raise RuntimeError("Email transport is not configured")
