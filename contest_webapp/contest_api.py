import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlparse

import aiohttp
import asyncpg
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int = 0) -> int:
    value = _env(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def _env_admin_ids() -> set[int]:
    raw = _env("ADMIN_IDS")
    ids: set[int] = set()
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                parsed = int(part)
            except ValueError:
                continue
            if parsed > 0:
                ids.add(parsed)
    legacy = _env_int("ADMIN_ID", 0)
    if legacy > 0:
        ids.add(legacy)
    return ids


BOT_TOKEN = _env("BOT_TOKEN")
DATABASE_URL = _env("DATABASE_URL")
CONTEST_CHANNEL_ID = _env("CONTEST_CHANNEL_ID") or _env("CONTEST_VOTING_CHAT_ID")
MAX_VOTES_PER_USER = max(1, _env_int("CONTEST_MAX_VOTES_PER_USER", 3))
VOTE_CONFIRM_REQUIRED = 3
ADMIN_IDS = _env_admin_ids()
CONTEST_MEDIA_BASE_URL = _env("CONTEST_MEDIA_BASE_URL")
MEDIA_BASE_URL = _env("MEDIA_BASE_URL")
TELEGRAM_FILE_BASE_URL = "https://api.telegram.org/file"
ENTRY_IMAGE_TOKEN_SECRET = _env("ENTRY_IMAGE_TOKEN_SECRET")
ENTRY_IMAGE_TOKEN_TTL_SECONDS = max(1, _env_int("ENTRY_IMAGE_TOKEN_TTL_SECONDS", 3600))

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
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE contest_entries ADD COLUMN IF NOT EXISTS penalty_votes INTEGER NOT NULL DEFAULT 0")
        await conn.execute("ALTER TABLE contest_entries ADD COLUMN IF NOT EXISTS display_order_anchor TIMESTAMPTZ")
        await conn.execute(
            """
            UPDATE contest_entries
            SET display_order_anchor = COALESCE(display_order_anchor, submitted_at, created_at, NOW())
            WHERE status = 'approved'
              AND COALESCE(is_deleted, FALSE) = FALSE
              AND display_order_anchor IS NULL
            """
        )
        await conn.execute("ALTER TABLE contest_settings ADD COLUMN IF NOT EXISTS penalties_applied_at TIMESTAMPTZ")
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION set_contest_entry_display_order_anchor()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NEW.status = 'approved' AND COALESCE(NEW.is_deleted, FALSE) = FALSE THEN
                    IF TG_OP = 'INSERT' THEN
                        IF NEW.display_order_anchor IS NULL THEN
                            NEW.display_order_anchor := NOW();
                        END IF;
                    ELSIF OLD.status <> 'approved'
                        OR COALESCE(OLD.is_deleted, FALSE) = TRUE
                        OR OLD.display_order_anchor IS NULL THEN
                        NEW.display_order_anchor := NOW();
                    END IF;
                END IF;
                RETURN NEW;
            END
            $$
            """
        )
        await conn.execute("DROP TRIGGER IF EXISTS trg_contest_entry_display_order_anchor ON contest_entries")
        await conn.execute(
            """
            CREATE TRIGGER trg_contest_entry_display_order_anchor
            BEFORE INSERT OR UPDATE OF status, is_deleted ON contest_entries
            FOR EACH ROW
            EXECUTE FUNCTION set_contest_entry_display_order_anchor()
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contest_vote_drafts (
                voter_user_id BIGINT NOT NULL,
                entry_id BIGINT NOT NULL REFERENCES contest_entries(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY(voter_user_id, entry_id)
            )
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_contest_vote_drafts_voter ON contest_vote_drafts(voter_user_id)")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contest_vote_confirmations (
                voter_user_id BIGINT PRIMARY KEY,
                confirmed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                selected_entry_ids BIGINT[] NOT NULL,
                ip_hash TEXT,
                user_agent_hash TEXT,
                is_suspicious BOOLEAN NOT NULL DEFAULT FALSE,
                suspicion_reason TEXT
            )
            """
        )


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


def _is_admin(user_id: int) -> bool:
    return int(user_id) in ADMIN_IDS


def _build_entry_image_token(entry_id: int, now: int | None = None) -> str | None:
    secret = ENTRY_IMAGE_TOKEN_SECRET.encode("utf-8")
    if not secret:
        return None
    issued_at = int(now or time.time())
    payload = f"{int(entry_id)}:{issued_at}"
    signature = hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{issued_at}.{signature}"


def _verify_entry_image_token(entry_id: int, token: str | None, now: int | None = None) -> bool:
    if not token:
        return True

    secret = ENTRY_IMAGE_TOKEN_SECRET.encode("utf-8")
    if not secret:
        return True

    try:
        issued_at_raw, signature = str(token).split(".", 1)
        issued_at = int(issued_at_raw)
    except (TypeError, ValueError):
        return False

    current = int(now or time.time())
    if issued_at <= 0 or current - issued_at > ENTRY_IMAGE_TOKEN_TTL_SECONDS:
        return False

    payload = f"{int(entry_id)}:{issued_at}"
    expected = hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _base_origin(value: str) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return None


def _absolute_url(raw_url: str | None, request_origin: str | None = None) -> str | None:
    value = (raw_url or "").strip()
    if not value:
        return None
    if value.startswith("//"):
        value = f"https:{value}"

    has_scheme = "://" in value
    if not has_scheme and not value.startswith("/"):
        value = f"https://{value}"

    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc:
        return parsed.geturl()

    if request_origin:
        joined = f"{request_origin.rstrip('/')}/{value.lstrip('/')}"
        joined_parsed = urlparse(joined)
        if joined_parsed.scheme and joined_parsed.netloc:
            return joined_parsed.geturl()

    logger.warning("Failed to normalize media URL: %r", raw_url)
    return None


def _build_entry_image_url(entry: dict[str, Any], request_origin: str | None = None) -> str | None:
    entry_id = entry.get("id")
    if entry_id is not None:
        local_url = _absolute_url(f"/api/contest/entries/{entry_id}/image", request_origin)
        if local_url:
            token = _build_entry_image_token(int(entry_id))
            if token:
                separator = "&" if "?" in local_url else "?"
                return f"{local_url}{separator}t={token}"
            return local_url

    for key in ("image_url", "file_url", "storage_url"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            normalized = _absolute_url(value, request_origin)
            if normalized:
                return normalized
            logger.warning("Entry %s has invalid %s: %r", entry.get("id"), key, value)
            return None

    storage_message_id = entry.get("storage_message_id")
    if not storage_message_id:
        storage_message_ids = entry.get("storage_message_ids")
        if isinstance(storage_message_ids, list) and storage_message_ids:
            storage_message_id = storage_message_ids[0]

    media_base = CONTEST_MEDIA_BASE_URL or MEDIA_BASE_URL
    if storage_message_id and media_base:
        normalized = _absolute_url(f"{media_base.rstrip('/')}/{storage_message_id}", request_origin)
        if normalized:
            return normalized
        logger.warning("Entry %s has invalid media base URL: %r", entry.get("id"), media_base)
    return None


def _entry_storage_message_id(entry: dict[str, Any]) -> int | None:
    message_id = entry.get("storage_message_id")
    if message_id:
        try:
            return int(message_id)
        except (TypeError, ValueError):
            return None

    storage_message_ids = entry.get("storage_message_ids")
    if isinstance(storage_message_ids, list) and storage_message_ids:
        try:
            return int(storage_message_ids[0])
        except (TypeError, ValueError):
            return None
    return None


async def _telegram_api_call(method: str, params: dict[str, Any]) -> dict[str, Any]:
    method_url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    async with get_http().post(method_url, json=params) as resp:
        data = await resp.json(content_type=None)
    if not data.get("ok"):
        raise RuntimeError(f"Telegram method {method} failed: {data}")
    return data


def _extract_photo_file_id(message: dict[str, Any]) -> str | None:
    photos = message.get("photo")
    if isinstance(photos, list) and photos:
        for item in reversed(photos):
            file_id = item.get("file_id") if isinstance(item, dict) else None
            if file_id:
                return str(file_id)

    for field in ("document", "animation", "sticker"):
        media = message.get(field)
        if isinstance(media, dict) and media.get("file_id"):
            return str(media["file_id"])

    return None


async def _download_entry_image(storage_chat_id: int, storage_message_id: int) -> tuple[bytes, str]:
    forwarded_message_id: int | None = None
    try:
        forwarded = await _telegram_api_call(
            "forwardMessage",
            {
                "chat_id": storage_chat_id,
                "from_chat_id": storage_chat_id,
                "message_id": storage_message_id,
                "disable_notification": True,
            },
        )
        result = forwarded.get("result") or {}
        forwarded_message_id = int(result.get("message_id") or 0) or None
        file_id = _extract_photo_file_id(result)
        if not file_id:
            raise RuntimeError("No supported image attachment in forwarded message")

        get_file = await _telegram_api_call("getFile", {"file_id": file_id})
        file_result = get_file.get("result") or {}
        file_path = str(file_result.get("file_path") or "").strip()
        if not file_path:
            raise RuntimeError("Telegram getFile returned empty file_path")

        file_url = f"{TELEGRAM_FILE_BASE_URL}/bot{BOT_TOKEN}/{file_path}"
        async with get_http().get(file_url) as file_resp:
            if file_resp.status != 200:
                raise RuntimeError(f"Telegram file download failed with status {file_resp.status}")
            payload = await file_resp.read()
            content_type = (file_resp.headers.get("Content-Type") or "application/octet-stream").strip()
        if not payload:
            raise RuntimeError("Downloaded Telegram image is empty")
        return payload, content_type
    finally:
        if forwarded_message_id:
            try:
                await _telegram_api_call(
                    "deleteMessage",
                    {"chat_id": storage_chat_id, "message_id": forwarded_message_id},
                )
            except Exception:
                logger.warning(
                    "Unable to delete temporary forwarded message chat_id=%s message_id=%s",
                    storage_chat_id,
                    forwarded_message_id,
                )


def _internal_error_response(message: str) -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "error_code": "internal_error",
            "message": message,
        },
        status_code=500,
    )


async def _check_channel_subscription(user_id: int) -> bool:
    method_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
    async with get_http().get(method_url, params={"chat_id": CONTEST_CHANNEL_ID, "user_id": user_id}) as resp:
        data = await resp.json(content_type=None)

    if not data.get("ok"):
        logger.warning("getChatMember failed: %s", data)
        raise VoteError("Не удалось проверить подписку на канал, попробуйте позже.", "subscription_check_failed", 503)

    status = str(((data.get("result") or {}).get("status") or "")).lower()
    return status not in {"left", "kicked"}


async def _get_contest_settings(conn: asyncpg.Connection | None = None) -> dict[str, Any]:
    db = conn or get_pool()
    row = await db.fetchrow(
        "SELECT enabled, submission_open, voting_open, penalties_applied_at FROM contest_settings WHERE id = 1"
    )
    if row is None:
        return {"enabled": False, "submission_open": False, "voting_open": False, "penalties_applied_at": None}
    return {
        "enabled": bool(row["enabled"]),
        "submission_open": bool(row.get("submission_open", False)),
        "voting_open": bool(row["voting_open"]),
        "penalties_applied_at": row.get("penalties_applied_at"),
    }


async def _is_suspicious(conn: asyncpg.Connection, voter_user_id: int, ip_hash: str | None, user_agent_hash: str | None) -> tuple[bool, str | None]:
    reasons: list[str] = []
    if not ip_hash or not user_agent_hash:
        reasons.append("missing_fingerprint")

    if ip_hash:
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
            reasons.append("ip_cluster")

    if user_agent_hash:
        shared_agents = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT voter_user_id)::INT
            FROM contest_votes
            WHERE user_agent_hash = $1
              AND created_at >= NOW() - INTERVAL '12 hours'
            """,
            user_agent_hash,
        )
        if int(shared_agents or 0) >= 10:
            reasons.append("ua_cluster")

    fast_actions = await conn.fetchval(
        """
        SELECT COUNT(*)::INT
        FROM contest_vote_drafts
        WHERE voter_user_id = $1
          AND created_at >= NOW() - INTERVAL '30 seconds'
        """,
        voter_user_id,
    )
    if int(fast_actions or 0) >= 5:
        reasons.append("too_fast")

    is_suspicious = bool(reasons)
    return is_suspicious, ",".join(reasons) if reasons else None


async def _get_confirmed_entry_ids(conn: asyncpg.Connection, voter_user_id: int) -> list[int]:
    rows = await conn.fetch(
        "SELECT entry_id FROM contest_votes WHERE voter_user_id = $1 ORDER BY created_at ASC",
        voter_user_id,
    )
    return [int(r["entry_id"]) for r in rows]


async def _get_draft_entry_ids(conn: asyncpg.Connection, voter_user_id: int) -> list[int]:
    rows = await conn.fetch(
        "SELECT entry_id FROM contest_vote_drafts WHERE voter_user_id = $1 ORDER BY created_at ASC",
        voter_user_id,
    )
    return [int(r["entry_id"]) for r in rows]


def _require_admin(user_id: int) -> None:
    if not _is_admin(user_id):
        raise VoteError("Недостаточно прав.", "forbidden", 403)


async def _ensure_votable_entry(conn: asyncpg.Connection, entry_id: int, user_id: int) -> None:
    entry = await conn.fetchrow(
        "SELECT id, owner_user_id, status, COALESCE(is_deleted, FALSE) AS is_deleted FROM contest_entries WHERE id = $1",
        entry_id,
    )
    if entry is None or entry["status"] != "approved" or bool(entry["is_deleted"]):
        raise VoteError("Работа недоступна для голосования.", "entry_not_available", 404)
    if int(entry["owner_user_id"]) == int(user_id):
        raise VoteError("Нельзя голосовать за свою работу.", "self_vote_forbidden", 400)


async def _apply_penalties_if_needed(conn: asyncpg.Connection) -> None:
    settings = await _get_contest_settings(conn)
    if settings.get("voting_open"):
        return
    if settings.get("penalties_applied_at") is not None:
        return

    await conn.execute(
        """
        UPDATE contest_entries e
        SET penalty_votes = 3,
            updated_at = NOW()
        WHERE e.status = 'approved'
          AND COALESCE(e.is_deleted, FALSE) = FALSE
          AND NOT EXISTS (
            SELECT 1 FROM contest_vote_confirmations c
            WHERE c.voter_user_id = e.owner_user_id
          )
        """
    )
    await conn.execute("UPDATE contest_settings SET penalties_applied_at = NOW(), updated_at = NOW() WHERE id = 1")


@router.get("/entries/approved")
async def approved_entries(request: Request, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        request_origin = _base_origin(str(getattr(request, "base_url", "") or ""))
        rows = await get_pool().fetch(
            """
            SELECT
                e.id,
                ROW_NUMBER() OVER (ORDER BY e.display_order_anchor ASC, e.id ASC) AS display_number,
                e.owner_user_id,
                e.owner_username_last_seen,
                e.owner_first_name_last_seen,
                e.owner_last_name_last_seen,
                e.storage_chat_id,
                e.storage_message_id,
                e.storage_message_ids,
                e.submitted_at,
                e.created_at,
                e.penalty_votes,
                COALESCE(v.votes_count, 0) AS votes_count
            FROM contest_entries e
            LEFT JOIN (
                SELECT entry_id, COUNT(*)::INT AS votes_count
                FROM contest_votes
                GROUP BY entry_id
            ) v ON v.entry_id = e.id
            WHERE e.status = 'approved'
              AND COALESCE(e.is_deleted, FALSE) = FALSE
            ORDER BY e.display_order_anchor ASC, e.id ASC
            """
        )
        current_user_id = int(auth["user_id"])
        items = []
        for row in rows:
            item = dict(row)
            item["is_owned_by_current_user"] = int(item["owner_user_id"]) == current_user_id
            item["image_url"] = _build_entry_image_url(item, request_origin)
            if not _is_admin(current_user_id):
                item.pop("votes_count", None)
                item.pop("penalty_votes", None)
            items.append(item)
        return JSONResponse(
            content=jsonable_encoder(
                {
                    "ok": True,
                    "current_user": {"user_id": current_user_id, "is_admin": _is_admin(current_user_id)},
                    "items": items,
                }
            )
        )
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load approved entries")
        return _internal_error_response("Не удалось загрузить список работ.")


@router.get("/entries/{entry_id}/image")
async def entry_image(entry_id: int, t: str = "", x_telegram_init_data: str = Header(default="")) -> Response:
    try:
        row = await get_pool().fetchrow(
            """
            SELECT id, status, COALESCE(is_deleted, FALSE) AS is_deleted, storage_chat_id, storage_message_id, storage_message_ids
            FROM contest_entries
            WHERE id = $1
            """,
            entry_id,
        )
        if row is None:
            return JSONResponse(
                {"ok": False, "error_code": "entry_not_found", "message": "Работа не найдена."},
                status_code=404,
            )

        entry = dict(row)
        if entry.get("status") != "approved" or bool(entry.get("is_deleted")):
            return JSONResponse(
                {"ok": False, "error_code": "entry_not_available", "message": "Работа недоступна."},
                status_code=404,
            )

        if not _verify_entry_image_token(entry_id, t):
            return JSONResponse(
                {"ok": False, "error_code": "entry_not_found", "message": "Работа не найдена."},
                status_code=404,
            )

        storage_chat_id_raw = entry.get("storage_chat_id")
        storage_message_id = _entry_storage_message_id(entry)
        if storage_chat_id_raw is None or storage_message_id is None:
            logger.warning(
                "Contest entry image metadata missing entry_id=%s storage_chat_id=%s storage_message_id=%s",
                entry_id,
                storage_chat_id_raw,
                storage_message_id,
            )
            return JSONResponse(
                {
                    "ok": False,
                    "error_code": "image_not_found",
                    "message": "Изображение для работы недоступно.",
                },
                status_code=404,
            )

        payload, content_type = await _download_entry_image(int(storage_chat_id_raw), storage_message_id)
        return Response(content=payload, media_type=content_type)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except RuntimeError as exc:
        logger.warning("Failed to fetch contest entry image entry_id=%s: %s", entry_id, exc)
        return JSONResponse(
            {
                "ok": False,
                "error_code": "image_unavailable",
                "message": "Не удалось получить изображение из Telegram storage.",
            },
            status_code=502,
        )
    except Exception:
        logger.exception("Failed to serve contest entry image entry_id=%s", entry_id)
        return _internal_error_response("Не удалось получить изображение работы.")


@router.get("/state")
async def contest_state(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        async with get_pool().acquire() as conn:
            settings = await _get_contest_settings(conn)
            confirmed = await conn.fetchrow(
                "SELECT confirmed_at, selected_entry_ids FROM contest_vote_confirmations WHERE voter_user_id = $1",
                auth["user_id"],
            )
            draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
            has_own_approved = bool(
                await conn.fetchval(
                    "SELECT 1 FROM contest_entries WHERE owner_user_id = $1 AND status = 'approved' AND COALESCE(is_deleted, FALSE) = FALSE",
                    auth["user_id"],
                )
            )
        is_channel_member = await _check_channel_subscription(auth["user_id"])
        return JSONResponse(
            content=jsonable_encoder(
                {
                    "ok": True,
                    "user_id": auth["user_id"],
                    "is_admin": _is_admin(auth["user_id"]),
                    "enabled": settings["enabled"],
                    "submission_open": settings["submission_open"],
                    "voting_open": settings["voting_open"],
                    "votes_required": VOTE_CONFIRM_REQUIRED,
                    "is_channel_member": is_channel_member,
                    "has_own_approved_entry": has_own_approved,
                    "confirmed": bool(confirmed),
                    "confirmed_at": confirmed["confirmed_at"] if confirmed else None,
                    "confirmed_entry_ids": list(confirmed["selected_entry_ids"] or []) if confirmed else [],
                    "draft_entry_ids": draft_entry_ids,
                }
            )
        )
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load contest state")
        return _internal_error_response("Не удалось загрузить состояние конкурса.")


@router.get("/my-draft")
async def my_draft(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        async with get_pool().acquire() as conn:
            confirmed = await conn.fetchrow(
                "SELECT confirmed_at, selected_entry_ids FROM contest_vote_confirmations WHERE voter_user_id = $1",
                auth["user_id"],
            )
            if confirmed:
                return JSONResponse({"ok": True, "confirmed": True, "entry_ids": list(confirmed["selected_entry_ids"] or [])})
            draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
            return JSONResponse({"ok": True, "confirmed": False, "entry_ids": draft_entry_ids})
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load vote draft")
        return _internal_error_response("Не удалось загрузить черновик голосования.")


@router.post("/draft/select")
async def draft_select(request: Request, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
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

                confirmed_exists = await conn.fetchval(
                    "SELECT 1 FROM contest_vote_confirmations WHERE voter_user_id = $1",
                    auth["user_id"],
                )
                if confirmed_exists:
                    raise VoteError("Голоса уже подтверждены.", "already_confirmed", 409)

                await _ensure_votable_entry(conn, entry_id, auth["user_id"])

                draft_count = await conn.fetchval(
                    "SELECT COUNT(*)::INT FROM contest_vote_drafts WHERE voter_user_id = $1",
                    auth["user_id"],
                )
                already = await conn.fetchval(
                    "SELECT 1 FROM contest_vote_drafts WHERE voter_user_id = $1 AND entry_id = $2",
                    auth["user_id"],
                    entry_id,
                )
                if already:
                    raise VoteError("Работа уже в выборе.", "duplicate_vote", 409)
                if int(draft_count or 0) >= VOTE_CONFIRM_REQUIRED:
                    raise VoteError("Можно выбрать только 3 работы.", "draft_limit_reached", 400)

                await conn.execute(
                    """
                    INSERT INTO contest_vote_drafts(voter_user_id, entry_id, created_at, updated_at)
                    VALUES ($1, $2, NOW(), NOW())
                    """,
                    auth["user_id"],
                    entry_id,
                )
                draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
        return JSONResponse({"ok": True, "entry_ids": draft_entry_ids, "selected": len(draft_entry_ids)})
    except (ValueError, json.JSONDecodeError):
        return JSONResponse({"ok": False, "error_code": "bad_request", "message": "Некорректный формат запроса."}, status_code=400)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to select draft vote")
        return _internal_error_response("Не удалось обновить выбор для голосования.")


@router.post("/draft/unselect")
async def draft_unselect(request: Request, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        payload = await request.json()
        entry_id = int(payload.get("entry_id") or 0)
        if entry_id <= 0:
            raise VoteError("Выберите работу для удаления.", "bad_entry_id", 400)

        async with get_pool().acquire() as conn:
            async with conn.transaction():
                confirmed_exists = await conn.fetchval(
                    "SELECT 1 FROM contest_vote_confirmations WHERE voter_user_id = $1",
                    auth["user_id"],
                )
                if confirmed_exists:
                    raise VoteError("Голоса уже подтверждены.", "already_confirmed", 409)

                await conn.execute(
                    "DELETE FROM contest_vote_drafts WHERE voter_user_id = $1 AND entry_id = $2",
                    auth["user_id"],
                    entry_id,
                )
                draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
        return JSONResponse({"ok": True, "entry_ids": draft_entry_ids, "selected": len(draft_entry_ids)})
    except (ValueError, json.JSONDecodeError):
        return JSONResponse({"ok": False, "error_code": "bad_request", "message": "Некорректный формат запроса."}, status_code=400)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to unselect draft vote")
        return _internal_error_response("Не удалось обновить выбор для голосования.")


@router.post("/votes/confirm")
async def confirm_votes(request: Request, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        is_member = await _check_channel_subscription(auth["user_id"])
        if not is_member:
            raise VoteError("Чтобы голосовать, нужно подписаться на канал.", "subscription_required", 403)

        async with get_pool().acquire() as conn:
            async with conn.transaction():
                settings = await _get_contest_settings(conn)
                if not bool(settings["enabled"]) or not bool(settings["voting_open"]):
                    raise VoteError("Голосование сейчас закрыто.", "voting_closed", 403)

                exists = await conn.fetchval(
                    "SELECT 1 FROM contest_vote_confirmations WHERE voter_user_id = $1 FOR UPDATE",
                    auth["user_id"],
                )
                if exists:
                    raise VoteError("Голоса уже подтверждены.", "already_confirmed", 409)

                draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
                unique_ids = list(dict.fromkeys(draft_entry_ids))
                if len(unique_ids) != VOTE_CONFIRM_REQUIRED:
                    raise VoteError("Нужно выбрать ровно 3 разные работы.", "exactly_three_required", 400)

                for entry_id in unique_ids:
                    await _ensure_votable_entry(conn, entry_id, auth["user_id"])

                client_host = request.client.host if request.client else None
                ip_hash = _hash_optional(client_host)
                ua_hash = _hash_optional(request.headers.get("User-Agent"))
                suspicious, suspicion_reason = await _is_suspicious(conn, auth["user_id"], ip_hash, ua_hash)

                for entry_id in unique_ids:
                    await conn.execute(
                        """
                        INSERT INTO contest_votes (
                            voter_user_id,
                            entry_id,
                            ip_hash,
                            user_agent_hash,
                            is_suspicious,
                            suspicion_reason,
                            confirmed_at,
                            created_at,
                            updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), NOW())
                        """,
                        auth["user_id"],
                        entry_id,
                        ip_hash,
                        ua_hash,
                        suspicious,
                        suspicion_reason,
                    )

                await conn.execute(
                    """
                    INSERT INTO contest_vote_confirmations (
                        voter_user_id,
                        confirmed_at,
                        selected_entry_ids,
                        ip_hash,
                        user_agent_hash,
                        is_suspicious,
                        suspicion_reason
                    )
                    VALUES ($1, NOW(), $2::BIGINT[], $3, $4, $5, $6)
                    """,
                    auth["user_id"],
                    unique_ids,
                    ip_hash,
                    ua_hash,
                    suspicious,
                    suspicion_reason,
                )

                await conn.execute("DELETE FROM contest_vote_drafts WHERE voter_user_id = $1", auth["user_id"])

        return JSONResponse({"ok": True, "confirmed": True, "entry_ids": unique_ids})
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to confirm votes")
        return _internal_error_response("Не удалось подтвердить голоса.")


@router.get("/votes/state")
async def votes_state(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        settings = await _get_contest_settings()
        is_channel_member = await _check_channel_subscription(auth["user_id"])
        async with get_pool().acquire() as conn:
            confirmed_entry_ids = await _get_confirmed_entry_ids(conn, auth["user_id"])
            draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
        used = len(confirmed_entry_ids)
        return JSONResponse(
            {
                "ok": True,
                "user_id": auth["user_id"],
                "max_votes": MAX_VOTES_PER_USER,
                "votes_used": used,
                "votes_remaining": max(0, MAX_VOTES_PER_USER - used),
                "voted_entry_ids": confirmed_entry_ids,
                "draft_entry_ids": draft_entry_ids,
                "confirmed": used >= VOTE_CONFIRM_REQUIRED,
                "voting_open": bool(settings["enabled"]) and bool(settings["voting_open"]),
                "is_channel_member": is_channel_member,
            }
        )
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load votes state")
        return _internal_error_response("Не удалось загрузить состояние голосования.")


@router.post("/votes/cast")
async def cast_vote(request: Request, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    """Legacy endpoint: now writes into draft selection to keep backward compatibility."""
    try:
        return await draft_select(request, x_telegram_init_data)
    except Exception as exc:  # pragma: no cover
        raise exc


@router.get("/rules")
async def contest_rules(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        _extract_auth(x_telegram_init_data)
        row = await get_pool().fetchrow("SELECT rules_text, rules_entities_json FROM contest_settings WHERE id = 1")
        rules_text = str(row["rules_text"] if row else "" or "").strip()
        rules_entities_json = row["rules_entities_json"] if row else None
        return JSONResponse({"ok": True, "rules_text": rules_text, "rules_entities_json": rules_entities_json})
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load contest rules")
        return _internal_error_response("Не удалось загрузить правила конкурса.")


@router.get("/admin/overview")
async def admin_overview(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        _require_admin(auth["user_id"])
        async with get_pool().acquire() as conn:
            await _apply_penalties_if_needed(conn)
            settings = await _get_contest_settings(conn)
            rows = await conn.fetch(
                """
                SELECT
                    e.id,
                    CASE
                        WHEN e.status = 'approved' THEN ROW_NUMBER() OVER (
                            PARTITION BY (e.status = 'approved')
                            ORDER BY e.display_order_anchor ASC, e.id ASC
                        )
                        ELSE NULL
                    END AS display_number,
                    e.owner_user_id,
                    e.owner_username_last_seen,
                    e.owner_first_name_last_seen,
                    e.owner_last_name_last_seen,
                    e.status,
                    e.submitted_at,
                    e.display_order_anchor,
                    e.penalty_votes,
                    COALESCE(v.confirmed_votes_count, 0) AS confirmed_votes_count,
                    COALESCE(v.suspicious_votes_count, 0) AS suspicious_votes_count
                FROM contest_entries e
                LEFT JOIN (
                    SELECT
                        entry_id,
                        COUNT(*)::INT AS confirmed_votes_count,
                        COUNT(*) FILTER (WHERE is_suspicious = TRUE)::INT AS suspicious_votes_count
                    FROM contest_votes
                    GROUP BY entry_id
                ) v ON v.entry_id = e.id
                WHERE COALESCE(e.is_deleted, FALSE) = FALSE
                ORDER BY e.display_order_anchor ASC, e.id ASC
                """
            )
            confirmations_count = await conn.fetchval("SELECT COUNT(*)::INT FROM contest_vote_confirmations")

        items = []
        for row in rows:
            item = dict(row)
            item["net_votes"] = int(item["confirmed_votes_count"] or 0) - int(item["penalty_votes"] or 0)
            items.append(item)

        payload = {
            "ok": True,
            "settings": settings,
            "confirmations_count": int(confirmations_count or 0),
            "items": items,
        }
        return JSONResponse(jsonable_encoder(payload))
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to build admin overview")
        return _internal_error_response("Не удалось загрузить админ-обзор.")


@router.get("/admin/entry/{entry_id}/votes")
async def admin_entry_votes(entry_id: int, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        _require_admin(auth["user_id"])
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    v.id,
                    v.voter_user_id,
                    v.entry_id,
                    v.created_at,
                    v.confirmed_at,
                    v.is_suspicious,
                    v.suspicion_reason,
                    v.ip_hash,
                    v.user_agent_hash,
                    c.selected_entry_ids,
                    c.confirmed_at AS voter_confirmed_at
                FROM contest_votes v
                LEFT JOIN contest_vote_confirmations c ON c.voter_user_id = v.voter_user_id
                WHERE v.entry_id = $1
                ORDER BY v.created_at DESC
                """,
                entry_id,
            )
        return JSONResponse({"ok": True, "entry_id": entry_id, "items": [dict(r) for r in rows]})
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load admin entry votes")
        return _internal_error_response("Не удалось загрузить голоса по работе.")


async def _set_stage(*, auth_user_id: int, submission_open: bool | None = None, voting_open: bool | None = None, enabled: bool | None = None) -> JSONResponse:
    _require_admin(auth_user_id)
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            settings = await _get_contest_settings(conn)
            new_enabled = settings["enabled"] if enabled is None else enabled
            new_submission = settings["submission_open"] if submission_open is None else submission_open
            new_voting = settings["voting_open"] if voting_open is None else voting_open

            if voting_open is False:
                await _apply_penalties_if_needed(conn)

            if voting_open is True:
                await conn.execute("UPDATE contest_entries SET penalty_votes = 0 WHERE penalty_votes <> 0")
                await conn.execute("UPDATE contest_settings SET penalties_applied_at = NULL WHERE id = 1")

            await conn.execute(
                """
                UPDATE contest_settings
                SET enabled = $1,
                    submission_open = $2,
                    voting_open = $3,
                    updated_at = NOW()
                WHERE id = 1
                """,
                bool(new_enabled),
                bool(new_submission),
                bool(new_voting),
            )
    return JSONResponse({"ok": True, "enabled": new_enabled, "submission_open": new_submission, "voting_open": new_voting})


@router.post("/admin/submission/open")
async def admin_submission_open(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        return await _set_stage(auth_user_id=auth["user_id"], submission_open=True)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("admin_submission_open failed")
        return _internal_error_response("Не удалось обновить стадию конкурса.")


@router.post("/admin/submission/close")
async def admin_submission_close(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        return await _set_stage(auth_user_id=auth["user_id"], submission_open=False)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("admin_submission_close failed")
        return _internal_error_response("Не удалось обновить стадию конкурса.")


@router.post("/admin/voting/open")
async def admin_voting_open(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        return await _set_stage(auth_user_id=auth["user_id"], enabled=True, submission_open=False, voting_open=True)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("admin_voting_open failed")
        return _internal_error_response("Не удалось обновить стадию конкурса.")


@router.post("/admin/voting/close")
async def admin_voting_close(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        return await _set_stage(auth_user_id=auth["user_id"], voting_open=False)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("admin_voting_close failed")
        return _internal_error_response("Не удалось обновить стадию конкурса.")


@router.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}