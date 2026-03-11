import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl

import aiohttp
import asyncpg
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int = 0) -> int:
    value = _env(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


BOT_TOKEN = _env("BOT_TOKEN")
DATABASE_URL = _env("DATABASE_URL")
CONTEST_CHANNEL_ID = _env("CONTEST_CHANNEL_ID") or _env("CONTEST_VOTING_CHAT_ID")
MAX_VOTES_PER_USER = max(1, _env_int("CONTEST_MAX_VOTES_PER_USER", 3))

router = APIRouter(prefix="/contest")

_pool: asyncpg.Pool | None = None
_http: aiohttp.ClientSession | None = None


@dataclass(slots=True)
class VoteError(Exception):
    message: str
    code: str
    status: int = 400


def _hash_optional(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _telegram_secret(bot_token: str) -> bytes:
    return hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()


def parse_and_verify_init_data(init_data: str, bot_token: str, max_age_seconds: int = 3600) -> dict[str, Any]:
    if not init_data:
        raise ValueError("Не переданы данные Telegram WebApp.")

    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    recv_hash = pairs.pop("hash", "")
    if not recv_hash:
        raise ValueError("Не удалось проверить Telegram-пользователя.")

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = _telegram_secret(bot_token)
    calc_hash = hmac.new(secret, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, recv_hash):
        raise ValueError("Telegram авторизация недействительна.")

    auth_date_raw = pairs.get("auth_date", "0")
    try:
        auth_date = int(auth_date_raw)
    except ValueError as exc:
        raise ValueError("Некорректное время авторизации Telegram.") from exc

    if int(time.time()) - auth_date > max_age_seconds:
        raise ValueError("Сессия Telegram устарела, откройте мини-приложение заново.")

    user_raw = pairs.get("user")
    if not user_raw:
        raise ValueError("Не передан Telegram user.")

    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("Некорректные данные Telegram user.") from exc

    user_id = int(user.get("id", 0) or 0)
    if user_id <= 0:
        raise ValueError("Некорректный Telegram user id.")

    return {"user_id": user_id, "user": user, "payload": pairs}


async def startup() -> None:
    global _pool, _http
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    if not CONTEST_CHANNEL_ID:
        raise RuntimeError("CONTEST_CHANNEL_ID or CONTEST_VOTING_CHAT_ID is required")

    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    await ensure_schema()


async def shutdown() -> None:
    global _pool, _http
    if _http is not None:
        await _http.close()
    if _pool is not None:
        await _pool.close()


async def ensure_schema() -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS ip_hash TEXT")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS user_agent_hash TEXT")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS is_suspicious BOOLEAN NOT NULL DEFAULT FALSE")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS suspicion_reason TEXT")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized")
    return _pool


def get_http() -> aiohttp.ClientSession:
    if _http is None:
        raise RuntimeError("HTTP session is not initialized")
    return _http


def _extract_auth(init_data: str) -> dict[str, Any]:
    try:
        return parse_and_verify_init_data(init_data, BOT_TOKEN)
    except ValueError as exc:
        raise VoteError(str(exc), "auth_failed", 401) from exc


async def _check_channel_subscription(user_id: int) -> bool:
    method_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
    async with get_http().get(method_url, params={"chat_id": CONTEST_CHANNEL_ID, "user_id": user_id}) as resp:
        data = await resp.json(content_type=None)

    if not data.get("ok"):
        logger.warning("getChatMember failed: %s", data)
        raise VoteError("Не удалось проверить подписку на канал, попробуйте позже.", "subscription_check_failed", 503)

    status = str(((data.get("result") or {}).get("status") or "")).lower()
    return status not in {"left", "kicked"}


async def _get_contest_settings(conn: asyncpg.Connection | None = None) -> dict[str, bool]:
    db = conn or get_pool()
    row = await db.fetchrow("SELECT enabled, voting_open FROM contest_settings WHERE id = 1")
    if row is None:
        return {"enabled": False, "voting_open": False}
    return {"enabled": bool(row["enabled"]), "voting_open": bool(row["voting_open"])}


async def _is_suspicious(conn: asyncpg.Connection, voter_user_id: int, ip_hash: str | None, user_agent_hash: str | None) -> bool:
    if not ip_hash or not user_agent_hash:
        return True

    shared_voters = await conn.fetchval(
        """
        SELECT COUNT(DISTINCT voter_user_id)::INT
        FROM contest_votes
        WHERE ip_hash = $1
          AND created_at >= NOW() - INTERVAL '24 hours'
        """,
        ip_hash,
    )
    if int(shared_voters or 0) >= 8:
        return True

    repeated_same_ua = await conn.fetchval(
        """
        SELECT COUNT(*)::INT
        FROM contest_votes
        WHERE voter_user_id = $1
          AND user_agent_hash = $2
        """,
        voter_user_id,
        user_agent_hash,
    )
    return int(repeated_same_ua or 0) >= 2


@router.get("/entries/approved")
async def approved_entries(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        rows = await get_pool().fetch(
            """
            SELECT
                e.id,
                e.owner_user_id,
                e.owner_username_last_seen,
                e.owner_first_name_last_seen,
                e.owner_last_name_last_seen,
                e.storage_chat_id,
                e.storage_message_id,
                e.storage_message_ids,
                e.submitted_at,
                e.created_at,
                COALESCE(v.votes_count, 0) AS votes_count
            FROM contest_entries e
            LEFT JOIN (
                SELECT entry_id, COUNT(*)::INT AS votes_count
                FROM contest_votes
                GROUP BY entry_id
            ) v ON v.entry_id = e.id
            WHERE e.status = 'approved'
              AND COALESCE(e.is_deleted, FALSE) = FALSE
            ORDER BY e.submitted_at DESC NULLS LAST, e.id DESC
            """
        )
        current_user_id = int(auth["user_id"])
        items = []
        for row in rows:
            item = dict(row)
            item["is_owned_by_current_user"] = int(item["owner_user_id"]) == current_user_id
            items.append(item)
        return JSONResponse({"ok": True, "current_user": {"user_id": current_user_id}, "items": items})
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)


@router.get("/votes/state")
async def votes_state(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        settings = await _get_contest_settings()
        is_channel_member = await _check_channel_subscription(auth["user_id"])
        used = await get_pool().fetchval("SELECT COUNT(*)::INT FROM contest_votes WHERE voter_user_id = $1", auth["user_id"])
        voted_entry_rows = await get_pool().fetch(
            "SELECT entry_id FROM contest_votes WHERE voter_user_id = $1 ORDER BY created_at ASC",
            auth["user_id"],
        )
        used = int(used or 0)
        return JSONResponse(
            {
                "ok": True,
                "user_id": auth["user_id"],
                "max_votes": MAX_VOTES_PER_USER,
                "votes_used": used,
                "votes_remaining": max(0, MAX_VOTES_PER_USER - used),
                "voted_entry_ids": [int(row["entry_id"]) for row in voted_entry_rows],
                "voting_open": bool(settings["enabled"]) and bool(settings["voting_open"]),
                "is_channel_member": is_channel_member,
            }
        )
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)


@router.post("/votes/cast")
async def cast_vote(request: Request, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        payload = await request.json()
        entry_id = int(payload.get("entry_id") or 0)
        if entry_id <= 0:
            raise VoteError("Выберите работу для голосования.", "bad_entry_id", 400)

        is_member = await _check_channel_subscription(auth["user_id"])
        if not is_member:
            raise VoteError("Чтобы голосовать, нужно подписаться на канал.", "subscription_required", 403)

        async with get_pool().acquire() as conn:
            async with conn.transaction():
                settings = await _get_contest_settings(conn)
                if not bool(settings["enabled"]) or not bool(settings["voting_open"]):
                    raise VoteError("Голосование сейчас закрыто.", "voting_closed", 403)

                entry = await conn.fetchrow(
                    "SELECT id, owner_user_id, status, COALESCE(is_deleted, FALSE) AS is_deleted FROM contest_entries WHERE id = $1 FOR UPDATE",
                    entry_id,
                )
                if entry is None or entry["status"] != "approved" or bool(entry["is_deleted"]):
                    raise VoteError("Работа недоступна для голосования.", "entry_not_available", 404)
                if int(entry["owner_user_id"]) == int(auth["user_id"]):
                    raise VoteError("Нельзя голосовать за свою работу.", "self_vote_forbidden", 400)

                used_votes = await conn.fetchval("SELECT COUNT(*)::INT FROM contest_votes WHERE voter_user_id = $1", auth["user_id"])
                if int(used_votes or 0) >= MAX_VOTES_PER_USER:
                    raise VoteError("Голосов не осталось.", "vote_limit_reached", 400)

                duplicate = await conn.fetchval(
                    "SELECT 1 FROM contest_votes WHERE voter_user_id = $1 AND entry_id = $2",
                    auth["user_id"],
                    entry_id,
                )
                if duplicate:
                    raise VoteError("Вы уже голосовали за эту работу.", "duplicate_vote", 409)

                client_host = request.client.host if request.client else None
                ip_hash = _hash_optional(client_host)
                ua_hash = _hash_optional(request.headers.get("User-Agent"))
                suspicious = await _is_suspicious(conn, auth["user_id"], ip_hash, ua_hash)

                await conn.execute(
                    """
                    INSERT INTO contest_votes (
                        voter_user_id,
                        entry_id,
                        ip_hash,
                        user_agent_hash,
                        is_suspicious,
                        suspicion_reason,
                        created_at,
                        updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                    """,
                    auth["user_id"],
                    entry_id,
                    ip_hash,
                    ua_hash,
                    suspicious,
                    None,
                )
                new_used = int(used_votes or 0) + 1

        return JSONResponse(
            {
                "ok": True,
                "entry_id": entry_id,
                "votes_used": new_used,
                "votes_remaining": max(0, MAX_VOTES_PER_USER - new_used),
                "is_suspicious": suspicious,
            }
        )
    except (ValueError, json.JSONDecodeError):
        return JSONResponse({"ok": False, "error_code": "bad_request", "message": "Некорректный формат запроса."}, status_code=400)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except asyncpg.UniqueViolationError:
        return JSONResponse({"ok": False, "error_code": "duplicate_vote", "message": "Вы уже голосовали за эту работу."}, status_code=409)


@router.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}