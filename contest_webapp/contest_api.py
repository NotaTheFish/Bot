import hashlib
import hmac
import html
import json
import logging
import mimetypes
import os
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import quote
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
CONTEST_PHASES = {"submission", "voting", "results"}
ADMIN_IDS = _env_admin_ids()
CONTEST_MEDIA_BASE_URL = _env("CONTEST_MEDIA_BASE_URL")
MEDIA_BASE_URL = _env("MEDIA_BASE_URL")
TELEGRAM_FILE_BASE_URL = "https://api.telegram.org/file"
ENTRY_IMAGE_TOKEN_SECRET = _env("ENTRY_IMAGE_TOKEN_SECRET")
ENTRY_IMAGE_TOKEN_TTL_SECONDS = max(1, _env_int("ENTRY_IMAGE_TOKEN_TTL_SECONDS", 3600))

router = APIRouter(prefix="/contest")

_pool: asyncpg.Pool | None = None
_http: aiohttp.ClientSession | None = None
_entry_image_cache: dict[int, tuple[float, bytes, str]] = {}
_telegram_file_path_cache: dict[str, tuple[float, str]] = {}
custom_emoji_cache: dict[str, tuple[float, str]] = {}
_custom_emoji_meta_cache: dict[str, tuple[float, "CustomEmojiRenderInfo"]] = {}
_custom_emoji_asset_cache: dict[str, tuple[float, bytes, str]] = {}
ENTRY_IMAGE_CACHE_TTL_SECONDS = max(1, _env_int("ENTRY_IMAGE_CACHE_TTL_SECONDS", 60))
TELEGRAM_FILE_PATH_CACHE_TTL_SECONDS = max(1, _env_int("TELEGRAM_FILE_PATH_CACHE_TTL_SECONDS", 300))
CUSTOM_EMOJI_URL_CACHE_TTL_SECONDS = max(1, _env_int("CUSTOM_EMOJI_URL_CACHE_TTL_SECONDS", 300))
CUSTOM_EMOJI_META_CACHE_TTL_SECONDS = max(1, _env_int("CUSTOM_EMOJI_META_CACHE_TTL_SECONDS", 300))
CUSTOM_EMOJI_ASSET_CACHE_TTL_SECONDS = max(1, _env_int("CUSTOM_EMOJI_ASSET_CACHE_TTL_SECONDS", 300))
CUSTOM_EMOJI_ASSET_URL_TTL_SECONDS = max(30, _env_int("CUSTOM_EMOJI_ASSET_URL_TTL_SECONDS", 900))
CUSTOM_EMOJI_ASSET_SIGNING_SECRET = _env("CUSTOM_EMOJI_ASSET_SIGNING_SECRET") or ENTRY_IMAGE_TOKEN_SECRET or BOT_TOKEN


@dataclass(slots=True)
class CustomEmojiRenderInfo:
    custom_emoji_id: str
    file_id: str
    file_unique_id: str
    is_animated: bool
    is_video: bool
    emoji: str | None
    set_name: str | None
    source_format: str
    file_path: str | None
    thumbnail_file_id: str | None
    render_mode: str
    mime_type: str
    width: int
    height: int


@dataclass(slots=True)
class VoteError(Exception):
    message: str
    code: str
    status: int = 400


def _hash_optional(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _custom_emoji_asset_signature(custom_emoji_id: str, exp: int) -> str:
    secret = (CUSTOM_EMOJI_ASSET_SIGNING_SECRET or "").encode("utf-8")
    payload = f"{custom_emoji_id}:{exp}".encode("utf-8")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def build_signed_custom_emoji_asset_url(custom_emoji_id: str) -> str:
    safe_custom_emoji_id = quote(str(custom_emoji_id), safe="")
    exp = int(time.time()) + CUSTOM_EMOJI_ASSET_URL_TTL_SECONDS
    sig = _custom_emoji_asset_signature(str(custom_emoji_id), exp)
    return f"/api/contest/custom-emoji/{safe_custom_emoji_id}/asset?exp={exp}&sig={sig}"


def _is_valid_custom_emoji_asset_signature(custom_emoji_id: str, exp: str, sig: str) -> bool:
    if not exp or not sig:
        return False
    try:
        exp_ts = int(exp)
    except (TypeError, ValueError):
        return False
    if exp_ts < int(time.time()):
        return False
    expected_sig = _custom_emoji_asset_signature(str(custom_emoji_id), exp_ts)
    return hmac.compare_digest(expected_sig, str(sig))


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
    _entry_image_cache.clear()
    _telegram_file_path_cache.clear()
    custom_emoji_cache.clear()
    _custom_emoji_meta_cache.clear()
    _custom_emoji_asset_cache.clear()


async def ensure_schema() -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS ip_hash TEXT")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS user_agent_hash TEXT")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS is_suspicious BOOLEAN NOT NULL DEFAULT FALSE")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS suspicion_reason TEXT")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'")
        await conn.execute("ALTER TABLE contest_votes DROP CONSTRAINT IF EXISTS contest_votes_status_check")
        await conn.execute(
            """
            ALTER TABLE contest_votes
            ADD CONSTRAINT contest_votes_status_check
            CHECK (status IN ('active', 'invalidated_by_disqualification'))
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_contest_votes_voter_status ON contest_votes(voter_user_id, status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_contest_votes_entry_status ON contest_votes(entry_id, status)")
        await conn.execute("ALTER TABLE contest_entries ADD COLUMN IF NOT EXISTS penalty_votes INTEGER NOT NULL DEFAULT 0")
        await conn.execute("ALTER TABLE contest_entries ADD COLUMN IF NOT EXISTS display_order_anchor TIMESTAMPTZ")
        await conn.execute("ALTER TABLE contest_settings ADD COLUMN IF NOT EXISTS phase TEXT")
        await conn.execute("ALTER TABLE contest_settings DROP CONSTRAINT IF EXISTS contest_settings_phase_check")
        await conn.execute(
            """
            ALTER TABLE contest_settings
            ADD CONSTRAINT contest_settings_phase_check
            CHECK (phase IN ('submission', 'voting', 'results') OR phase IS NULL)
            """
        )
        await conn.execute(
            """
            UPDATE contest_settings
            SET phase = CASE
                WHEN voting_open THEN 'voting'
                WHEN submission_open THEN 'submission'
                ELSE 'results'
            END
            WHERE phase IS NULL
            """
        )
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
        local_url = f"/api/contest/entries/{entry_id}/image"
        token = _build_entry_image_token(int(entry_id))
        if token:
            return f"{local_url}?t={token}"
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
    file_id = await _resolve_entry_file_id(storage_chat_id=storage_chat_id, storage_message_id=storage_message_id)
    return await _download_image_by_file_id(file_id)


def _cache_get_entry_image(entry_id: int) -> tuple[bytes, str] | None:
    cached = _entry_image_cache.get(int(entry_id))
    if not cached:
        return None
    expires_at, payload, content_type = cached
    if time.time() >= expires_at:
        _entry_image_cache.pop(int(entry_id), None)
        return None
    return payload, content_type


def _cache_set_entry_image(entry_id: int, payload: bytes, content_type: str) -> None:
    _entry_image_cache[int(entry_id)] = (time.time() + ENTRY_IMAGE_CACHE_TTL_SECONDS, payload, content_type)


def _cache_get_file_path(file_id: str) -> str | None:
    cached = _telegram_file_path_cache.get(str(file_id))
    if not cached:
        return None
    expires_at, file_path = cached
    if time.time() >= expires_at:
        _telegram_file_path_cache.pop(str(file_id), None)
        return None
    return file_path


def _cache_set_file_path(file_id: str, file_path: str) -> None:
    _telegram_file_path_cache[str(file_id)] = (time.time() + TELEGRAM_FILE_PATH_CACHE_TTL_SECONDS, str(file_path))


def _cache_get_custom_emoji_url(custom_emoji_id: str) -> str | None:
    cached = custom_emoji_cache.get(str(custom_emoji_id))
    if not cached:
        return None
    expires_at, url = cached
    if time.time() >= expires_at:
        custom_emoji_cache.pop(str(custom_emoji_id), None)
        return None
    return url


def _cache_set_custom_emoji_url(custom_emoji_id: str, url: str) -> None:
    custom_emoji_cache[str(custom_emoji_id)] = (time.time() + CUSTOM_EMOJI_URL_CACHE_TTL_SECONDS, str(url))


def _cache_get_custom_emoji_meta(custom_emoji_id: str) -> CustomEmojiRenderInfo | None:
    cached = _custom_emoji_meta_cache.get(str(custom_emoji_id))
    if not cached:
        return None
    expires_at, meta = cached
    if time.time() >= expires_at:
        _custom_emoji_meta_cache.pop(str(custom_emoji_id), None)
        return None
    return meta


def _cache_set_custom_emoji_meta(custom_emoji_id: str, meta: CustomEmojiRenderInfo) -> None:
    _custom_emoji_meta_cache[str(custom_emoji_id)] = (time.time() + CUSTOM_EMOJI_META_CACHE_TTL_SECONDS, meta)


def _cache_get_custom_emoji_asset(custom_emoji_id: str) -> tuple[bytes, str] | None:
    cached = _custom_emoji_asset_cache.get(str(custom_emoji_id))
    if not cached:
        return None
    expires_at, payload, mime_type = cached
    if time.time() >= expires_at:
        _custom_emoji_asset_cache.pop(str(custom_emoji_id), None)
        return None
    return payload, mime_type


def _cache_set_custom_emoji_asset(custom_emoji_id: str, payload: bytes, mime_type: str) -> None:
    _custom_emoji_asset_cache[str(custom_emoji_id)] = (
        time.time() + CUSTOM_EMOJI_ASSET_CACHE_TTL_SECONDS,
        payload,
        mime_type,
    )


def _source_format_for_file_path(file_path: str | None) -> str:
    if not file_path:
        return "unknown"
    lowered = file_path.lower()
    if lowered.endswith(".tgs"):
        return "tgs"
    if lowered.endswith(".webm"):
        return "webm"
    if lowered.endswith(".webp"):
        return "webp"
    if lowered.endswith(".png"):
        return "png"
    return "unknown"


def _render_mode_for_sticker(is_animated: bool, is_video: bool, source_format: str) -> str:
    if is_video or source_format == "webm":
        return "video"
    if is_animated or source_format == "tgs":
        return "lottie"
    if source_format in {"webp", "png"}:
        return "image"
    return "fallback"


def _mime_type_for_custom_emoji(source_format: str, render_mode: str) -> str:
    if render_mode == "lottie":
        return "application/x-tgsticker"
    if render_mode == "video":
        return "video/webm"
    if source_format == "png":
        return "image/png"
    return "image/webp"


async def _resolve_custom_emoji_info(custom_emoji_id: str) -> CustomEmojiRenderInfo | None:
    normalized_id = str(custom_emoji_id or "").strip()
    if not normalized_id:
        return None

    cached = _cache_get_custom_emoji_meta(normalized_id)
    if cached:
        return cached

    stickers_data = await _telegram_api_call("getCustomEmojiStickers", {"custom_emoji_ids": [normalized_id]})
    stickers = stickers_data.get("result")
    if not isinstance(stickers, list) or not stickers:
        return None

    sticker: dict[str, Any] | None = None
    for item in stickers:
        if isinstance(item, dict) and str(item.get("custom_emoji_id") or "") == normalized_id:
            sticker = item
            break
    if sticker is None and isinstance(stickers[0], dict):
        sticker = stickers[0]
    if sticker is None:
        return None

    file_id = str(sticker.get("file_id") or "").strip()
    if not file_id:
        return None

    file_unique_id = str(sticker.get("file_unique_id") or "").strip()
    is_animated = bool(sticker.get("is_animated"))
    is_video = bool(sticker.get("is_video"))
    emoji = str(sticker.get("emoji") or "").strip() or None
    set_name = str(sticker.get("set_name") or "").strip() or None
    width = int(sticker.get("width") or 100)
    height = int(sticker.get("height") or 100)
    thumbnail = sticker.get("thumbnail") if isinstance(sticker.get("thumbnail"), dict) else None
    thumbnail_file_id = str((thumbnail or {}).get("file_id") or "").strip() or None

    get_file = await _telegram_api_call("getFile", {"file_id": file_id})
    file_result = get_file.get("result") or {}
    file_path = str(file_result.get("file_path") or "").strip() or None
    if not file_path:
        return None

    _cache_set_file_path(file_id, file_path)
    source_format = _source_format_for_file_path(file_path)
    render_mode = _render_mode_for_sticker(is_animated, is_video, source_format)
    meta = CustomEmojiRenderInfo(
        custom_emoji_id=normalized_id,
        file_id=file_id,
        file_unique_id=file_unique_id,
        is_animated=is_animated,
        is_video=is_video,
        emoji=emoji,
        set_name=set_name,
        source_format=source_format,
        file_path=file_path,
        thumbnail_file_id=thumbnail_file_id,
        render_mode=render_mode,
        mime_type=_mime_type_for_custom_emoji(source_format, render_mode),
        width=max(1, width),
        height=max(1, height),
    )
    _cache_set_custom_emoji_meta(normalized_id, meta)
    return meta


def _utf16_index_boundaries(text: str) -> list[int]:
    boundaries = [0]
    index = 0
    for char in text:
        index += 2 if ord(char) > 0xFFFF else 1
        boundaries.append(index)
    return boundaries


def _py_index_for_utf16_offset(boundaries: list[int], utf16_offset: int) -> int | None:
    try:
        return boundaries.index(utf16_offset)
    except ValueError:
        return None


async def _custom_emoji_image_url(custom_emoji_id: str) -> str | None:
    resolved = await _resolve_custom_emoji_info(custom_emoji_id)
    if not resolved or not resolved.file_path:
        return None
    cached = _cache_get_custom_emoji_url(custom_emoji_id)
    if cached:
        return cached
    file_url = f"{TELEGRAM_FILE_BASE_URL}/bot{BOT_TOKEN}/{resolved.file_path}"
    _cache_set_custom_emoji_url(custom_emoji_id, file_url)
    return file_url


async def render_telegram_text_with_entities_to_html(text: str, entities: Any) -> str:
    source = str(text or "")
    if not entities:
        return html.escape(source)

    parsed_entities = entities
    if isinstance(entities, str):
        try:
            parsed_entities = json.loads(entities)
        except json.JSONDecodeError:
            return html.escape(source)

    if not isinstance(parsed_entities, list):
        return html.escape(source)

    boundaries = _utf16_index_boundaries(source)
    total_utf16_len = boundaries[-1]
    cursor_utf16 = 0
    parts: list[str] = []

    def append_escaped_slice(start_utf16: int, end_utf16: int) -> None:
        if end_utf16 < start_utf16:
            return
        start_idx = _py_index_for_utf16_offset(boundaries, start_utf16)
        end_idx = _py_index_for_utf16_offset(boundaries, end_utf16)
        if start_idx is None or end_idx is None:
            return
        if start_idx >= end_idx:
            return
        parts.append(html.escape(source[start_idx:end_idx]))

    def get_plain_slice(start_utf16: int, end_utf16: int) -> str:
        start_idx = _py_index_for_utf16_offset(boundaries, start_utf16)
        end_idx = _py_index_for_utf16_offset(boundaries, end_utf16)
        if start_idx is None or end_idx is None or start_idx > end_idx:
            return ""
        return source[start_idx:end_idx]

    normalized_entities: list[dict[str, Any]] = [item for item in parsed_entities if isinstance(item, dict)]

    def _entity_offset(item: dict[str, Any]) -> int:
        try:
            return int(item.get("offset") or 0)
        except (TypeError, ValueError):
            return 0

    for entity in sorted(normalized_entities, key=_entity_offset):
        if str(entity.get("type") or "") != "custom_emoji":
            continue

        try:
            offset = int(entity.get("offset"))
            length = int(entity.get("length"))
        except (TypeError, ValueError):
            continue

        if length <= 0 or offset < 0:
            continue

        entity_end = offset + length
        if entity_end > total_utf16_len or offset < cursor_utf16:
            continue

        custom_emoji_id = str(entity.get("custom_emoji_id") or "").strip()
        if not custom_emoji_id:
            continue

        append_escaped_slice(cursor_utf16, offset)

        fallback_emoji = html.escape(get_plain_slice(offset, entity_end) or "◻️")
        try:
            emoji_info = await _resolve_custom_emoji_info(custom_emoji_id)
        except Exception:
            logger.warning("Failed to resolve custom emoji meta for id=%s", custom_emoji_id, exc_info=True)
            emoji_info = None

        if not emoji_info:
            parts.append(f'<span class="tg-inline-emoji tg-inline-emoji--fallback">{fallback_emoji}</span>')
            cursor_utf16 = entity_end
            continue

        safe_id = html.escape(custom_emoji_id, quote=True)
        safe_emoji = html.escape(emoji_info.emoji or fallback_emoji, quote=True)
        signed_asset_url = html.escape(build_signed_custom_emoji_asset_url(custom_emoji_id), quote=True)
        if emoji_info.render_mode == "image":
            parts.append(
                '<span class="tg-inline-emoji" data-custom-emoji-id="{}" data-render-mode="image">'
                '<img class="tg-custom-emoji" src="{}" alt="{}" loading="lazy" />'
                "</span>".format(safe_id, signed_asset_url, safe_emoji)
            )
        elif emoji_info.render_mode == "lottie":
            parts.append(
                '<span class="tg-inline-emoji tg-inline-emoji--lottie" '
                'data-custom-emoji-id="{}" data-render-mode="lottie" '
                'data-asset-url="{}" aria-label="{}"></span>'.format(
                    safe_id,
                    signed_asset_url,
                    safe_emoji,
                )
            )
        elif emoji_info.render_mode == "video":
            parts.append(
                '<span class="tg-inline-emoji tg-inline-emoji--video" data-custom-emoji-id="{}" data-render-mode="video">'
                '<video class="tg-custom-emoji-video" autoplay loop muted playsinline src="{}" aria-label="{}"></video>'
                "</span>".format(safe_id, signed_asset_url, safe_emoji)
            )
        else:
            parts.append(f'<span class="tg-inline-emoji tg-inline-emoji--fallback">{safe_emoji}</span>')

        cursor_utf16 = entity_end

    append_escaped_slice(cursor_utf16, total_utf16_len)
    return "".join(parts)


async def render_telegram_text_with_custom_emoji(text: str, entities: Any) -> str:
    return await render_telegram_text_with_entities_to_html(text, entities)


def _normalize_content_type(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return raw.split(";", 1)[0].strip().lower() or None


def _detect_image_content_type(payload: bytes) -> str | None:
    if not payload:
        return None
    if payload.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if payload.startswith(b"GIF87a") or payload.startswith(b"GIF89a"):
        return "image/gif"
    if len(payload) >= 12 and payload.startswith(b"RIFF") and payload[8:12] == b"WEBP":
        return "image/webp"
    return None


def _content_type_for_file(file_path: str, header_content_type: str | None, payload: bytes) -> str:
    normalized_header = _normalize_content_type(header_content_type)
    guessed, _ = mimetypes.guess_type(file_path)
    normalized_guess = _normalize_content_type(guessed)

    if normalized_header and normalized_header.startswith("image/"):
        return normalized_header
    if normalized_guess and normalized_guess.startswith("image/"):
        return normalized_guess

    if (not normalized_header or normalized_header == "application/octet-stream") and (
        not normalized_guess or normalized_guess == "application/octet-stream"
    ):
        detected_image_type = _detect_image_content_type(payload)
        if detected_image_type:
            return detected_image_type

    if normalized_header and normalized_header != "application/octet-stream":
        return normalized_header
    if normalized_guess:
        return normalized_guess
    return "application/octet-stream"


async def _download_image_by_file_id(file_id: str, *, entry_id: int | None = None) -> tuple[bytes, str]:
    cached_file_path = _cache_get_file_path(file_id)
    file_path = cached_file_path
    if not file_path:
        get_file = await _telegram_api_call("getFile", {"file_id": file_id})
        file_result = get_file.get("result") or {}
        file_path = str(file_result.get("file_path") or "").strip()
        if not file_path:
            raise RuntimeError("Telegram getFile returned empty file_path")
        _cache_set_file_path(file_id, file_path)

    file_url = f"{TELEGRAM_FILE_BASE_URL}/bot{BOT_TOKEN}/{file_path}"
    async with get_http().get(file_url) as file_resp:
        if file_resp.status != 200:
            raise RuntimeError(f"Telegram file download failed with status {file_resp.status}")
        payload = await file_resp.read()
        source_header = file_resp.headers.get("Content-Type")
        content_type = _content_type_for_file(file_path, source_header, payload)
    if not payload:
        raise RuntimeError("Downloaded Telegram image is empty")
    logger.debug(
        "Contest entry image content type resolved entry_id=%s file_path=%s source_content_type=%s final_content_type=%s",
        entry_id,
        file_path,
        source_header,
        content_type,
    )
    return payload, content_type


async def _download_custom_emoji_asset(meta: CustomEmojiRenderInfo) -> tuple[bytes, str]:
    cached = _cache_get_custom_emoji_asset(meta.custom_emoji_id)
    if cached:
        return cached
    if not meta.file_path:
        raise RuntimeError("Custom emoji file_path is empty")

    file_url = f"{TELEGRAM_FILE_BASE_URL}/bot{BOT_TOKEN}/{meta.file_path}"
    async with get_http().get(file_url) as file_resp:
        if file_resp.status != 200:
            raise RuntimeError(f"Telegram custom emoji download failed with status {file_resp.status}")
        payload = await file_resp.read()
        source_header = file_resp.headers.get("Content-Type")
    if not payload:
        raise RuntimeError("Downloaded custom emoji asset is empty")

    mime_type = _content_type_for_file(meta.file_path, source_header, payload)
    if meta.render_mode == "lottie":
        mime_type = "application/x-tgsticker"
    elif meta.render_mode == "video":
        mime_type = "video/webm"
    _cache_set_custom_emoji_asset(meta.custom_emoji_id, payload, mime_type)
    return payload, mime_type


async def _resolve_entry_file_id(
    *,
    storage_chat_id: int,
    storage_message_id: int,
    telegram_file_id: str | None = None,
) -> str:
    if telegram_file_id:
        return str(telegram_file_id)

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
            raise RuntimeError("No supported image attachment in storage message")
        return file_id
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
        "SELECT enabled, submission_open, voting_open, penalties_applied_at, phase FROM contest_settings WHERE id = 1"
    )
    if row is None:
        return {
            "enabled": False,
            "submission_open": False,
            "voting_open": False,
            "penalties_applied_at": None,
            "phase": "submission",
        }
    submission_open = bool(row.get("submission_open", False))
    voting_open = bool(row["voting_open"])
    return {
        "enabled": bool(row["enabled"]),
        "submission_open": submission_open,
        "voting_open": voting_open,
        "penalties_applied_at": row.get("penalties_applied_at"),
        "phase": _derive_contest_phase(
            submission_open=submission_open,
            voting_open=voting_open,
            raw_phase=row.get("phase"),
        ),
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
              AND status = 'active'
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
              AND status = 'active'
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
        "SELECT entry_id FROM contest_votes WHERE voter_user_id = $1 AND status = 'active' ORDER BY created_at ASC",
        voter_user_id,
    )
    return [int(r["entry_id"]) for r in rows]


async def _count_active_votes(conn: asyncpg.Connection, voter_user_id: int) -> int:
    count = await conn.fetchval(
        "SELECT COUNT(*)::INT FROM contest_votes WHERE voter_user_id = $1 AND status = 'active'",
        voter_user_id,
    )
    return int(count or 0)


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
        settings = await _get_contest_settings()
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
                WHERE status = 'active'
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
            votes_count = int(item.get("votes_count") or 0)
            penalty_votes = int(item.get("penalty_votes") or 0)
            item["net_votes"] = votes_count - penalty_votes
            item["author_label"] = _author_label(
                owner_user_id=int(item.get("owner_user_id") or 0),
                owner_username_last_seen=item.get("owner_username_last_seen"),
            )
            logger.info(
                "Approved entry prepared id=%s display_number=%s image_url=%s storage_chat_id=%s storage_message_id=%s",
                item.get("id"),
                item.get("display_number"),
                item.get("image_url"),
                item.get("storage_chat_id"),
                item.get("storage_message_id"),
            )
            if not _is_admin(current_user_id):
                item.pop("votes_count", None)
                item.pop("penalty_votes", None)
            items.append(item)

        phase = settings.get("phase") or "submission"
        if phase == "results":
            _apply_results_tie_break(items)
            for item in items:
                if not _is_admin(current_user_id):
                    item.pop("tie_break_bonus", None)
                    item.pop("effective_net_votes", None)
                item["net_votes"] = int(item.get("effective_net_votes") or item.get("net_votes") or 0)
        else:
            for item in items:
                item["place"] = None
                item["reward_label"] = None
                item.pop("tie_break_bonus", None)
                item.pop("effective_net_votes", None)

        return JSONResponse(
            content=jsonable_encoder(
                {
                    "ok": True,
                    "phase": phase,
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
        logger.debug("Contest entry image requested entry_id=%s token_present=%s", entry_id, bool(t))
        cached = _cache_get_entry_image(entry_id)
        if cached:
            payload, content_type = cached
            logger.debug("Contest entry image cache hit entry_id=%s content_type=%s bytes=%s", entry_id, content_type, len(payload))
            return Response(content=payload, media_type=content_type)

        row = await get_pool().fetchrow(
            """
            SELECT id, status, COALESCE(is_deleted, FALSE) AS is_deleted, telegram_file_id, storage_chat_id, storage_message_id, storage_message_ids
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

        file_id = str(entry.get("telegram_file_id") or "").strip() or None
        resolved_file_id = await _resolve_entry_file_id(
            storage_chat_id=int(storage_chat_id_raw),
            storage_message_id=storage_message_id,
            telegram_file_id=file_id,
        )
        payload, content_type = await _download_image_by_file_id(resolved_file_id, entry_id=entry_id)
        _cache_set_entry_image(entry_id, payload, content_type)
        logger.info("Contest entry image served entry_id=%s storage_message_id=%s content_type=%s bytes=%s", entry_id, storage_message_id, content_type, len(payload))
        return Response(content=payload, media_type=content_type)
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except RuntimeError as exc:
        logger.warning("Contest entry image telegram fetch failed entry_id=%s reason=%s", entry_id, exc)
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
        is_channel_member = await _check_channel_subscription(auth["user_id"])
        async with get_pool().acquire() as conn:
            settings = await _get_contest_settings(conn)
            payload = await _build_vote_state_payload(
                conn,
                user_id=auth["user_id"],
                settings=settings,
                is_channel_member=is_channel_member,
            )
        return JSONResponse(content=jsonable_encoder(payload))
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
            active_votes_count = await _count_active_votes(conn, auth["user_id"])
            confirmed_entry_ids = await _get_confirmed_entry_ids(conn, auth["user_id"])
            available_votes = max(0, MAX_VOTES_PER_USER - active_votes_count)
            if available_votes <= 0:
                return JSONResponse({"ok": True, "confirmed": True, "entry_ids": confirmed_entry_ids})
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

                await _ensure_votable_entry(conn, entry_id, auth["user_id"])

                active_votes_count = await _count_active_votes(conn, auth["user_id"])
                available_votes = max(0, MAX_VOTES_PER_USER - active_votes_count)
                draft_count = await conn.fetchval(
                    "SELECT COUNT(*)::INT FROM contest_vote_drafts WHERE voter_user_id = $1",
                    auth["user_id"],
                )
                already_active = await conn.fetchval(
                    "SELECT 1 FROM contest_votes WHERE voter_user_id = $1 AND entry_id = $2 AND status = 'active'",
                    auth["user_id"],
                    entry_id,
                )
                already = await conn.fetchval(
                    "SELECT 1 FROM contest_vote_drafts WHERE voter_user_id = $1 AND entry_id = $2",
                    auth["user_id"],
                    entry_id,
                )
                if already or already_active:
                    raise VoteError("Работа уже в выборе.", "duplicate_vote", 409)
                if int(draft_count or 0) >= available_votes:
                    raise VoteError(
                        f"Можно выбрать только {available_votes} работ в текущей сессии.",
                        "draft_limit_reached",
                        400,
                    )

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
                active_votes_count = await _count_active_votes(conn, auth["user_id"])
                if active_votes_count >= MAX_VOTES_PER_USER:
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

        current_active_ids: list[int] = []
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                settings = await _get_contest_settings(conn)
                if not bool(settings["enabled"]) or not bool(settings["voting_open"]):
                    raise VoteError("Голосование сейчас закрыто.", "voting_closed", 403)

                draft_entry_ids = await _get_draft_entry_ids(conn, auth["user_id"])
                unique_ids = list(dict.fromkeys(draft_entry_ids))
                active_votes_count = await _count_active_votes(conn, auth["user_id"])
                available_votes = max(0, MAX_VOTES_PER_USER - active_votes_count)
                if available_votes <= 0:
                    raise VoteError("Доступных голосов для подтверждения нет.", "vote_limit_reached", 400)
                if len(unique_ids) != available_votes:
                    raise VoteError(
                        f"Нужно выбрать ровно {available_votes} разные работы.",
                        "exactly_available_required",
                        400,
                    )

                if active_votes_count + len(unique_ids) > MAX_VOTES_PER_USER:
                    raise VoteError("Можно иметь не больше 3 активных голосов.", "vote_limit_reached", 400)

                for entry_id in unique_ids:
                    await _ensure_votable_entry(conn, entry_id, auth["user_id"])
                    duplicate_active = await conn.fetchval(
                        "SELECT 1 FROM contest_votes WHERE voter_user_id = $1 AND entry_id = $2 AND status = 'active'",
                        auth["user_id"],
                        entry_id,
                    )
                    if duplicate_active:
                        raise VoteError("Нельзя повторно голосовать за ту же работу.", "duplicate_vote", 409)

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
                            status,
                            ip_hash,
                            user_agent_hash,
                            is_suspicious,
                            suspicion_reason,
                            confirmed_at,
                            created_at,
                            updated_at
                        )
                        VALUES ($1, $2, 'active', $3, $4, $5, $6, NOW(), NOW(), NOW())
                        ON CONFLICT (voter_user_id, entry_id)
                        DO UPDATE SET
                            status = 'active',
                            ip_hash = EXCLUDED.ip_hash,
                            user_agent_hash = EXCLUDED.user_agent_hash,
                            is_suspicious = EXCLUDED.is_suspicious,
                            suspicion_reason = EXCLUDED.suspicion_reason,
                            confirmed_at = NOW(),
                            updated_at = NOW()
                        """,
                        auth["user_id"],
                        entry_id,
                        ip_hash,
                        ua_hash,
                        suspicious,
                        suspicion_reason,
                    )

                current_active_ids = await _get_confirmed_entry_ids(conn, auth["user_id"])
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
                    ON CONFLICT (voter_user_id)
                    DO UPDATE SET
                        confirmed_at = NOW(),
                        selected_entry_ids = EXCLUDED.selected_entry_ids,
                        ip_hash = EXCLUDED.ip_hash,
                        user_agent_hash = EXCLUDED.user_agent_hash,
                        is_suspicious = EXCLUDED.is_suspicious,
                        suspicion_reason = EXCLUDED.suspicion_reason
                    """,
                    auth["user_id"],
                    current_active_ids,
                    ip_hash,
                    ua_hash,
                    suspicious,
                    suspicion_reason,
                )

                await conn.execute("DELETE FROM contest_vote_drafts WHERE voter_user_id = $1", auth["user_id"])

        async with get_pool().acquire() as conn:
            settings = await _get_contest_settings(conn)
            state_payload = await _build_vote_state_payload(
                conn,
                user_id=auth["user_id"],
                settings=settings,
                is_channel_member=is_member,
            )

        return JSONResponse(content=jsonable_encoder(state_payload))
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to confirm votes")
        return _internal_error_response("Не удалось подтвердить голоса.")


@router.get("/votes/state")
async def votes_state(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        is_channel_member = await _check_channel_subscription(auth["user_id"])
        async with get_pool().acquire() as conn:
            settings = await _get_contest_settings(conn)
            payload = await _build_vote_state_payload(
                conn,
                user_id=auth["user_id"],
                settings=settings,
                is_channel_member=is_channel_member,
            )
        payload["max_votes"] = MAX_VOTES_PER_USER
        payload["votes_used"] = payload["active_votes_count"]
        payload["voted_entry_ids"] = payload["confirmed_entry_ids"]
        return JSONResponse(content=jsonable_encoder(payload))
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


@router.get("/custom-emoji/{custom_emoji_id}/meta")
async def custom_emoji_meta(custom_emoji_id: str, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        _extract_auth(x_telegram_init_data)
        meta = await _resolve_custom_emoji_info(custom_emoji_id)
        if not meta:
            return JSONResponse(
                {
                    "ok": True,
                    "custom_emoji_id": str(custom_emoji_id),
                    "render_mode": "fallback",
                    "asset_url": build_signed_custom_emoji_asset_url(custom_emoji_id),
                    "mime_type": "application/octet-stream",
                    "width": 100,
                    "height": 100,
                }
            )

        return JSONResponse(
            {
                "ok": True,
                "custom_emoji_id": meta.custom_emoji_id,
                "render_mode": meta.render_mode,
                "asset_url": build_signed_custom_emoji_asset_url(meta.custom_emoji_id),
                "mime_type": meta.mime_type,
                "width": meta.width,
                "height": meta.height,
                "meta": asdict(meta),
            }
        )
    except Exception:
        logger.exception("Failed to load custom emoji meta for id=%s", custom_emoji_id)
        return _internal_error_response("Не удалось загрузить метаданные premium emoji.")


@router.get("/custom-emoji/{custom_emoji_id}/asset")
async def custom_emoji_asset(custom_emoji_id: str, exp: str = "", sig: str = "") -> Response:
    try:
        if not _is_valid_custom_emoji_asset_signature(custom_emoji_id, exp, sig):
            return Response(content=b"", media_type="application/octet-stream", status_code=403)
        meta = await _resolve_custom_emoji_info(custom_emoji_id)
        if not meta:
            return Response(content=b"", media_type="application/octet-stream", status_code=404)
        payload, mime_type = await _download_custom_emoji_asset(meta)
        headers = {
            "Cache-Control": "public, max-age=300",
            "X-Custom-Emoji-Render-Mode": meta.render_mode,
            "X-Custom-Emoji-Source-Format": meta.source_format,
        }
        return Response(content=payload, media_type=mime_type, headers=headers)
    except Exception:
        logger.exception("Failed to load custom emoji asset for id=%s", custom_emoji_id)
        return Response(content=b"", media_type="application/octet-stream", status_code=404)


@router.get("/rules")
async def contest_rules(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        _extract_auth(x_telegram_init_data)
        row = await get_pool().fetchrow("SELECT rules_text, rules_entities_json FROM contest_settings WHERE id = 1")
        rules_text = str(row["rules_text"] if row else "" or "").strip()
        rules_entities_json = row["rules_entities_json"] if row else None
        rules_html = await render_telegram_text_with_entities_to_html(rules_text, rules_entities_json)
        return JSONResponse(
            {
                "ok": True,
                "rules_text": rules_text,
                "rules_entities_json": rules_entities_json,
                "rules_html": rules_html,
            }
        )
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
                    WHERE status = 'active'
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
            item["tie_break_bonus"] = 0
            item["effective_net_votes"] = item["net_votes"]
            items.append(item)

        if (settings.get("phase") or "submission") == "results":
            approved_items = [item for item in items if item.get("status") == "approved"]
            _apply_results_tie_break(approved_items)
            for item in approved_items:
                item["net_votes"] = int(item.get("effective_net_votes") or item.get("net_votes") or 0)

        leaders = [dict(item) for item in items if item.get("status") == "approved"]
        _apply_results_tie_break(leaders)
        for item in leaders:
            item["net_votes"] = int(item.get("effective_net_votes") or item.get("net_votes") or 0)
        leaders = leaders[:5]

        payload = {
            "ok": True,
            "settings": settings,
            "confirmations_count": int(confirmations_count or 0),
            "items": items,
            "leaders": leaders,
        }
        return JSONResponse(jsonable_encoder(payload))
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to build admin overview")
        return _internal_error_response("Не удалось загрузить админ-обзор.")


@router.get("/admin/entries/{entry_id}")
async def admin_entry_detail(entry_id: int, x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        _require_admin(auth["user_id"])
        async with get_pool().acquire() as conn:
            row = await conn.fetchrow(
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
                    e.status,
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
                    WHERE status = 'active'
                    GROUP BY entry_id
                ) v ON v.entry_id = e.id
                WHERE e.id = $1
                  AND COALESCE(e.is_deleted, FALSE) = FALSE
                """,
                entry_id,
            )

            if not row:
                raise VoteError("Работа не найдена.", "entry_not_found", 404)

            item = dict(row)
            item["net_votes"] = int(item.get("confirmed_votes_count") or 0) - int(item.get("penalty_votes") or 0)
            item["tie_break_bonus"] = 0
            item["effective_net_votes"] = int(item["net_votes"])

            if item.get("status") == "approved":
                approved_rows = await conn.fetch(
                    """
                    SELECT
                        e.id,
                        e.penalty_votes,
                        COALESCE(v.confirmed_votes_count, 0) AS confirmed_votes_count
                    FROM contest_entries e
                    LEFT JOIN (
                        SELECT entry_id, COUNT(*)::INT AS confirmed_votes_count
                        FROM contest_votes
                        WHERE status = 'active'
                        GROUP BY entry_id
                    ) v ON v.entry_id = e.id
                    WHERE e.status = 'approved'
                      AND COALESCE(e.is_deleted, FALSE) = FALSE
                    """
                )
                approved_items = [dict(approved_row) for approved_row in approved_rows]
                for approved_item in approved_items:
                    approved_item["net_votes"] = int(approved_item.get("confirmed_votes_count") or 0) - int(
                        approved_item.get("penalty_votes") or 0
                    )
                _apply_results_tie_break(approved_items)
                matched = next((approved_item for approved_item in approved_items if int(approved_item.get("id") or 0) == entry_id), None)
                if matched:
                    item["tie_break_bonus"] = int(matched.get("tie_break_bonus") or 0)
                    item["effective_net_votes"] = int(matched.get("effective_net_votes") or item["effective_net_votes"])

        return JSONResponse({"ok": True, "item": item})
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("Failed to load admin entry detail id=%s", entry_id)
        return _internal_error_response("Не удалось загрузить детали работы.")


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
                  AND v.status = 'active'
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


async def _set_stage(
    *,
    auth_user_id: int,
    submission_open: bool | None = None,
    voting_open: bool | None = None,
    enabled: bool | None = None,
    phase: str | None = None,
) -> JSONResponse:
    _require_admin(auth_user_id)
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            settings = await _get_contest_settings(conn)
            new_enabled = settings["enabled"] if enabled is None else enabled
            new_submission = settings["submission_open"] if submission_open is None else submission_open
            new_voting = settings["voting_open"] if voting_open is None else voting_open
            new_phase = _derive_contest_phase(
                submission_open=bool(new_submission),
                voting_open=bool(new_voting),
                raw_phase=phase or settings.get("phase"),
            )

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
                    phase = $4,
                    updated_at = NOW()
                WHERE id = 1
                """,
                bool(new_enabled),
                bool(new_submission),
                bool(new_voting),
                new_phase,
            )
    return JSONResponse(
        {
            "ok": True,
            "enabled": new_enabled,
            "submission_open": new_submission,
            "voting_open": new_voting,
            "phase": new_phase,
        }
    )


@router.post("/admin/submission/open")
async def admin_submission_open(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        return await _set_stage(auth_user_id=auth["user_id"], submission_open=True, phase="submission")
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
        return await _set_stage(auth_user_id=auth["user_id"], enabled=True, submission_open=False, voting_open=True, phase="voting")
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("admin_voting_open failed")
        return _internal_error_response("Не удалось обновить стадию конкурса.")


@router.post("/admin/voting/close")
async def admin_voting_close(x_telegram_init_data: str = Header(default="")) -> JSONResponse:
    try:
        auth = _extract_auth(x_telegram_init_data)
        return await _set_stage(auth_user_id=auth["user_id"], voting_open=False, phase="results")
    except VoteError as exc:
        return JSONResponse({"ok": False, "error_code": exc.code, "message": exc.message}, status_code=exc.status)
    except Exception:
        logger.exception("admin_voting_close failed")
        return _internal_error_response("Не удалось обновить стадию конкурса.")


@router.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}

def _derive_contest_phase(*, submission_open: bool, voting_open: bool, raw_phase: str | None = None) -> str:
    phase = (raw_phase or "").strip().lower()
    if phase in CONTEST_PHASES:
        return phase
    if voting_open:
        return "voting"
    if submission_open:
        return "submission"
    return "results"


def _reward_label_for_place(place: int) -> str:
    if place == 1:
        return "100к грибов"
    if place == 2:
        return "50к грибов"
    if place == 3:
        return "25к грибов"
    return "Утешительный приз: 5к грибов"


def _author_label(owner_user_id: int, owner_username_last_seen: str | None) -> str:
    username = (owner_username_last_seen or "").strip().lstrip("@")
    if username:
        return f"@{username}"
    return str(owner_user_id)


def _submitted_sort_key(value: Any) -> float:
    if value is None:
        return float("inf")
    if hasattr(value, "timestamp"):
        try:
            return float(value.timestamp())
        except Exception:
            return float("inf")
    return float("inf")


def _apply_results_tie_break(items: list[dict[str, Any]]) -> None:
    if not items:
        return

    for item in items:
        item["tie_break_bonus"] = 0

    grouped: dict[int, list[dict[str, Any]]] = {}
    for item in items:
        grouped.setdefault(int(item.get("net_votes") or 0), []).append(item)

    for tied_items in grouped.values():
        if len(tied_items) <= 1:
            continue
        winner = sorted(
            tied_items,
            key=lambda entry: (
                int(entry.get("penalty_votes") or 0),
                _submitted_sort_key(entry.get("submitted_at") or entry.get("created_at")),
                int(entry.get("id") or 0),
            ),
        )[0]
        winner["tie_break_bonus"] = 1

    for item in items:
        item["effective_net_votes"] = int(item.get("net_votes") or 0) + int(item.get("tie_break_bonus") or 0)

    items.sort(
        key=lambda entry: (
            -int(entry.get("effective_net_votes") or 0),
            int(entry.get("penalty_votes") or 0),
            _submitted_sort_key(entry.get("submitted_at") or entry.get("created_at")),
            int(entry.get("id") or 0),
        )
    )

    for index, item in enumerate(items, start=1):
        item["place"] = index
        item["reward_label"] = _reward_label_for_place(index)


async def _build_vote_state_payload(
    conn: asyncpg.Connection,
    *,
    user_id: int,
    settings: dict[str, Any],
    is_channel_member: bool,
) -> dict[str, Any]:
    active_votes_count = await _count_active_votes(conn, user_id)
    confirmed_entry_ids = await _get_confirmed_entry_ids(conn, user_id)
    draft_entry_ids = await _get_draft_entry_ids(conn, user_id)
    available_votes = max(0, MAX_VOTES_PER_USER - active_votes_count)
    has_own_approved = bool(
        await conn.fetchval(
            "SELECT 1 FROM contest_entries WHERE owner_user_id = $1 AND status = 'approved' AND COALESCE(is_deleted, FALSE) = FALSE",
            user_id,
        )
    )
    return {
        "ok": True,
        "user_id": user_id,
        "is_admin": _is_admin(user_id),
        "enabled": settings["enabled"],
        "submission_open": settings["submission_open"],
        "voting_open": settings["voting_open"],
        "phase": settings["phase"],
        "votes_required": VOTE_CONFIRM_REQUIRED,
        "active_votes_count": active_votes_count,
        "available_votes": available_votes,
        "votes_remaining": available_votes,
        "is_channel_member": is_channel_member,
        "has_own_approved_entry": has_own_approved,
        "confirmed": available_votes <= 0,
        "confirmed_entry_ids": confirmed_entry_ids,
        "draft_entry_ids": draft_entry_ids,
    }