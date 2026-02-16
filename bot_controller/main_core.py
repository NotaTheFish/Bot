import asyncio
import json
import logging
import os
import random
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    MessageEntity,
    ReplyKeyboardMarkup,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo


def _get_env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _get_env_int(name: str, default: int = 0) -> int:
    raw = _get_env_str(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


BOT_TOKEN = _get_env_str("BOT_TOKEN")
ADMIN_ID = _get_env_int("ADMIN_ID", 0)
DATABASE_URL = _get_env_str("DATABASE_URL")
DELIVERY_MODE = _get_env_str("DELIVERY_MODE", "bot").lower() or "bot"
STORAGE_CHAT_ID = _get_env_int("STORAGE_CHAT_ID", 0)
STORAGE_CHAT_META_KEY = "storage_chat_id"
LAST_STORAGE_MESSAGE_ID_META_KEY = "last_storage_message_id"
LAST_STORAGE_MESSAGE_IDS_META_KEY = "last_storage_message_ids"
LAST_STORAGE_CHAT_ID_META_KEY = "last_storage_chat_id"
MEDIA_GROUP_BUFFER_TIMEOUT_SECONDS = max(0.5, float(_get_env_str("MEDIA_GROUP_BUFFER_TIMEOUT_SECONDS", "1.5")))


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@")


SELLER_USERNAME = _normalize_username(_get_env_str("SELLER_USERNAME", ""))
CONFIGURED_BOT_USERNAME = _normalize_username(_get_env_str("BOT_USERNAME", ""))
SUPPORT_PAYLOAD = "support"
CONTACT_PAYLOAD_PREFIX = "contact_"
CONTACT_CTA_TEXT = "✉️ Написать продавцу"

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан или имеет неверный формат.")
if ADMIN_ID <= 0:
    raise RuntimeError("ADMIN_ID не задан или имеет неверное значение.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан. Для Railway используйте PostgreSQL plugin.")

TZ = ZoneInfo("Europe/Berlin")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)
BOT_USERNAME = ""
BOT_ID = 0
db_pool: Optional[asyncpg.Pool] = None
media_group_buffers: dict[str, dict] = {}
userbot_tasks_schema_ready = False
userbot_tasks_schema_lock = asyncio.Lock()
userbot_tasks_columns_cache: Optional[set[str]] = None
broadcast_now_lock = asyncio.Lock()

GLOBAL_BROADCAST_COOLDOWN_SECONDS = _get_env_int("GLOBAL_BROADCAST_COOLDOWN_SECONDS", 600)
MIN_USER_MESSAGES_BETWEEN_POSTS = _get_env_int("MIN_USER_MESSAGES_BETWEEN_POSTS", 5)
BROADCAST_MAX_PER_DAY = _get_env_int("BROADCAST_MAX_PER_DAY", 20)
MIN_SECONDS_BETWEEN_CHATS = max(0.0, float(_get_env_str("MIN_SECONDS_BETWEEN_CHATS", "0.3")))
MAX_SECONDS_BETWEEN_CHATS = max(MIN_SECONDS_BETWEEN_CHATS, float(_get_env_str("MAX_SECONDS_BETWEEN_CHATS", "0.7")))


def _get_env_int_list(name: str) -> list[int]:
    raw = _get_env_str(name, "")
    if not raw:
        return []
    values: list[int] = []
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        values.append(int(item))
    return values


TARGET_CHAT_IDS = _get_env_int_list("TARGET_CHAT_IDS")
ANTI_DUPLICATE_HOURS = _get_env_int("ANTI_DUPLICATE_HOURS", 6)
QUIET_HOURS_START = _get_env_str("QUIET_HOURS_START", "")
QUIET_HOURS_END = _get_env_str("QUIET_HOURS_END", "")
MANUAL_CONFIRM_FIRST_BROADCAST = _get_env_str("MANUAL_CONFIRM_FIRST_BROADCAST", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
REPORT_ONLY_ON_ERRORS = _get_env_str("REPORT_ONLY_ON_ERRORS", "0").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if DELIVERY_MODE not in {"bot", "userbot"}:
    raise RuntimeError("DELIVERY_MODE должен быть 'bot' или 'userbot'.")


class AdminStates(StatesGroup):
    waiting_post_content = State()
    waiting_daily_time = State()
    waiting_weekly_time = State()
    waiting_edit_time = State()
    waiting_buyer_reply_template = State()


class SupportStates(StatesGroup):
    waiting_message = State()


class ContactStates(StatesGroup):
    waiting_message = State()


CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS chats (
        chat_id BIGINT PRIMARY KEY,
        disabled INTEGER NOT NULL DEFAULT 0,
        last_bot_post_msg_id BIGINT,
        user_messages_since_last_post INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        last_success_post_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS post (
        id INTEGER PRIMARY KEY,
        source_chat_id BIGINT,
        source_message_id BIGINT,
        storage_chat_id BIGINT,
        storage_message_ids BIGINT[] NOT NULL DEFAULT '{}',
        has_media INTEGER NOT NULL DEFAULT 0,
        media_group_id TEXT,
        contact_token TEXT UNIQUE,
        CONSTRAINT post_single_row CHECK (id = 1)
    )
    """,
    """
    INSERT INTO post (id, source_chat_id, source_message_id, storage_chat_id, storage_message_ids, has_media, media_group_id, contact_token)
    VALUES (1, NULL, NULL, NULL, '{}', 0, NULL, NULL)
    ON CONFLICT (id) DO NOTHING
    """,
    """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS schedules (
        id BIGSERIAL PRIMARY KEY,
        schedule_type TEXT NOT NULL CHECK (schedule_type IN ('daily', 'hourly', 'weekly')),
        time_text TEXT,
        weekday INTEGER,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bot_settings (
        id INTEGER PRIMARY KEY,
        buyer_reply_storage_chat_id BIGINT,
        buyer_reply_message_id BIGINT
    )
    """,
    """
    INSERT INTO bot_settings (id, buyer_reply_storage_chat_id, buyer_reply_message_id)
    VALUES (1, NULL, NULL)
    ON CONFLICT (id) DO NOTHING
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_schedules_unique
    ON schedules(schedule_type, COALESCE(time_text, ''), COALESCE(weekday, -1))
    """,
    """
    CREATE TABLE IF NOT EXISTS broadcast_attempts (
        id BIGSERIAL PRIMARY KEY,
        created_at TEXT NOT NULL,
        chat_id BIGINT,
        status TEXT NOT NULL,
        reason TEXT,
        error_text TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS userbot_tasks (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      last_error TEXT,
      storage_chat_id BIGINT NOT NULL,
      storage_message_id BIGINT,
      dedupe_key TEXT,
      storage_message_ids BIGINT[] NOT NULL DEFAULT '{}',
      target_chat_ids BIGINT[] NOT NULL DEFAULT '{}',
      sent_count INT NOT NULL DEFAULT 0,
      error_count INT NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
    ON userbot_tasks(status, run_at)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_userbot_tasks_dedupe_active
    ON userbot_tasks(dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'processing')
    """,
]

USERBOT_TASKS_MIGRATIONS_SQL = [
    """
    ALTER TABLE userbot_tasks
    ADD COLUMN IF NOT EXISTS storage_chat_id BIGINT
    """,
    """
    ALTER TABLE userbot_tasks
    ADD COLUMN IF NOT EXISTS storage_message_id BIGINT
    """,
    """
    ALTER TABLE userbot_tasks
    ADD COLUMN IF NOT EXISTS storage_message_ids BIGINT[]
    """,
    """
    ALTER TABLE userbot_tasks
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT
    """,
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'userbot_tasks' AND column_name = 'source_chat_id'
        ) THEN
            UPDATE userbot_tasks
            SET storage_chat_id = source_chat_id
            WHERE storage_chat_id IS NULL;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'userbot_tasks' AND column_name = 'source_message_id'
        ) THEN
            UPDATE userbot_tasks
            SET storage_message_id = source_message_id
            WHERE storage_message_id IS NULL;
        END IF;
    END
    $$;
    """,
    """
    ALTER TABLE userbot_tasks
    ALTER COLUMN storage_chat_id SET NOT NULL
    """,
    """
    UPDATE userbot_tasks
    SET storage_message_ids = ARRAY[storage_message_id]
    WHERE (storage_message_ids IS NULL OR array_length(storage_message_ids, 1) IS NULL)
      AND storage_message_id IS NOT NULL
    """,
    """
    UPDATE userbot_tasks
    SET storage_message_ids = '{}'
    WHERE storage_message_ids IS NULL
    """,
    """
    ALTER TABLE userbot_tasks
    ALTER COLUMN storage_message_ids SET NOT NULL
    """,
    """
    ALTER TABLE userbot_tasks
    ALTER COLUMN storage_message_id DROP NOT NULL
    """,
    """
    UPDATE userbot_tasks
    SET target_chat_ids = '{}'
    WHERE target_chat_ids IS NULL
    """,
    """
    ALTER TABLE userbot_tasks
    ALTER COLUMN target_chat_ids SET NOT NULL
    """,
    """
    ALTER TABLE userbot_tasks
    ALTER COLUMN target_chat_ids SET DEFAULT '{}'
    """,
    """
    ALTER TABLE userbot_tasks
    DROP COLUMN IF EXISTS source_chat_id
    """,
    """
    ALTER TABLE userbot_tasks
    DROP COLUMN IF EXISTS source_message_id
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
    ON userbot_tasks(status, run_at)
    """,
    """
    ALTER TABLE userbot_tasks
    ALTER COLUMN run_at TYPE TIMESTAMPTZ
    USING run_at AT TIME ZONE 'UTC'
    """,
    """
    UPDATE userbot_tasks
    SET status = CASE status
        WHEN 'running' THEN 'processing'
        WHEN 'failed' THEN 'error'
        ELSE status
    END
    """,
    """
    DROP INDEX IF EXISTS idx_userbot_tasks_dedupe_active
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_userbot_tasks_dedupe_active
    ON userbot_tasks(dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'processing')
    """,
]

USERBOT_TASKS_BASE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS userbot_tasks (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      last_error TEXT,
      storage_chat_id BIGINT NOT NULL,
      storage_message_id BIGINT,
      dedupe_key TEXT,
      storage_message_ids BIGINT[] NOT NULL DEFAULT '{}',
      target_chat_ids BIGINT[] NOT NULL DEFAULT '{}',
      sent_count INT NOT NULL DEFAULT 0,
      error_count INT NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
    ON userbot_tasks(status, run_at)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_userbot_tasks_dedupe_active
    ON userbot_tasks(dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'processing')
    """,
]

POST_MIGRATIONS_SQL = [
    """
    ALTER TABLE post
    ADD COLUMN IF NOT EXISTS storage_message_ids BIGINT[]
    """,
    """
    UPDATE post
    SET storage_message_ids = ARRAY[source_message_id]
    WHERE (storage_message_ids IS NULL OR array_length(storage_message_ids, 1) IS NULL)
      AND source_message_id IS NOT NULL
    """,
    """
    UPDATE post
    SET storage_message_ids = '{}'
    WHERE storage_message_ids IS NULL
    """,
    """
    ALTER TABLE post
    ALTER COLUMN storage_message_ids SET NOT NULL
    """,
    """
    ALTER TABLE post
    ADD COLUMN IF NOT EXISTS storage_chat_id BIGINT
    """,
    """
    UPDATE post
    SET storage_chat_id = source_chat_id
    WHERE storage_chat_id IS NULL
      AND source_chat_id IS NOT NULL
    """,
    """
    ALTER TABLE post
    ADD COLUMN IF NOT EXISTS contact_token TEXT
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_post_contact_token
    ON post(contact_token)
    WHERE contact_token IS NOT NULL
    """,
]

SCHEDULES_MIGRATIONS_SQL = [
    """
    ALTER TABLE schedules
    ADD COLUMN IF NOT EXISTS schedule_type TEXT
    """,
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'schedules' AND column_name = 'kind'
        ) THEN
            UPDATE schedules
            SET schedule_type = kind
            WHERE schedule_type IS NULL
              AND kind IS NOT NULL;
        END IF;
    END
    $$;
    """,
    """
    ALTER TABLE schedules
    ALTER COLUMN schedule_type SET NOT NULL
    """,
    """
    ALTER TABLE schedules
    DROP CONSTRAINT IF EXISTS schedules_schedule_type_check
    """,
    """
    ALTER TABLE schedules
    ADD CONSTRAINT schedules_schedule_type_check CHECK (schedule_type IN ('daily', 'hourly', 'weekly'))
    """,
    """
    ALTER TABLE schedules
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE
    """,
    """
    UPDATE schedules
    SET enabled = TRUE
    WHERE enabled IS NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_schedules_unique
    ON schedules(schedule_type, COALESCE(time_text, ''), COALESCE(weekday, -1))
    """,
]


async def get_db_pool() -> asyncpg.Pool:
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return db_pool


async def init_db() -> None:
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for query in CREATE_TABLES_SQL:
                await conn.execute(query)
            for query in SCHEDULES_MIGRATIONS_SQL:
                await conn.execute(query)
            for query in USERBOT_TASKS_MIGRATIONS_SQL:
                await conn.execute(query)
            for query in POST_MIGRATIONS_SQL:
                await conn.execute(query)


async def ensure_userbot_tasks_schema(pool: asyncpg.Pool) -> None:
    global userbot_tasks_schema_ready, userbot_tasks_columns_cache
    if userbot_tasks_schema_ready:
        return
    async with userbot_tasks_schema_lock:
        if userbot_tasks_schema_ready:
            return
        async with pool.acquire() as conn:
            async with conn.transaction():
                for query in USERBOT_TASKS_BASE_SQL:
                    await conn.execute(query)
                for query in USERBOT_TASKS_MIGRATIONS_SQL:
                    await conn.execute(query)
        userbot_tasks_columns_cache = None
        userbot_tasks_schema_ready = True


async def get_userbot_tasks_columns(pool: asyncpg.Pool) -> set[str]:
    global userbot_tasks_columns_cache
    if userbot_tasks_columns_cache is not None:
        return userbot_tasks_columns_cache
    rows = await pool.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'userbot_tasks'
        """
    )
    userbot_tasks_columns_cache = {str(row["column_name"]) for row in rows}
    return userbot_tasks_columns_cache


def admin_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📌 Добавить пост")],
            [KeyboardButton(text="✏️ Изменить пост")],
            [KeyboardButton(text="📝 Изменить автоответ покупателю")],
            [KeyboardButton(text="📊 Статус")],
            [KeyboardButton(text="🚀 Запустить сейчас")],
        ],
        resize_keyboard=True,
    )


def is_admin_user(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id == ADMIN_ID)


def ensure_admin(event_message: Message) -> bool:
    return bool(event_message.from_user and is_admin_user(event_message.from_user.id))


async def save_chat(chat_id: int) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        INSERT INTO chats(chat_id, user_messages_since_last_post)
        VALUES ($1, $2)
        ON CONFLICT (chat_id) DO NOTHING
        """,
        chat_id,
        MIN_USER_MESSAGES_BETWEEN_POSTS,
    )


async def save_broadcast_attempt(
    status: str,
    chat_id: Optional[int] = None,
    reason: Optional[str] = None,
    error_text: Optional[str] = None,
) -> None:
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO broadcast_attempts(created_at, chat_id, status, reason, error_text)
                VALUES ($1, $2, $3, $4, $5)
                """,
                datetime.now(timezone.utc).isoformat(),
                chat_id,
                status,
                reason,
                (error_text or "")[:500] or None,
            )
            await conn.execute(
                """
                DELETE FROM broadcast_attempts
                WHERE id NOT IN (
                    SELECT id
                    FROM broadcast_attempts
                    ORDER BY created_at DESC
                    LIMIT 100
                )
                """
            )


async def get_recent_broadcast_attempts(limit: int = 10) -> list[dict]:
    pool = await get_db_pool()
    rows = await pool.fetch(
        """
        SELECT created_at, chat_id, status, reason, error_text
        FROM broadcast_attempts
        ORDER BY created_at DESC
        LIMIT $1
        """,
        limit,
    )
    return [
        {
            "created_at": row["created_at"],
            "chat_id": row["chat_id"],
            "status": row["status"],
            "reason": row["reason"],
            "error_text": row["error_text"],
        }
        for row in rows
    ]




async def userbot_targets_stats() -> Optional[dict]:
    pool = await get_db_pool()
    try:
        totals = await pool.fetchrow(
            """
            SELECT
              COUNT(*)::bigint AS total,
              COALESCE(SUM(CASE WHEN enabled THEN 1 ELSE 0 END), 0)::bigint AS enabled,
              COALESCE(SUM(CASE WHEN NOT enabled THEN 1 ELSE 0 END), 0)::bigint AS disabled
            FROM userbot_targets
            """
        )
        recent_disabled_rows = await pool.fetch(
            """
            SELECT chat_id, COALESCE(last_error, '-') AS last_error
            FROM userbot_targets
            WHERE enabled = FALSE
            ORDER BY COALESCE(last_error_at, last_seen_at) DESC
            LIMIT 5
            """
        )
    except Exception:
        return None

    return {
        "total": int(totals["total"] if totals else 0),
        "enabled": int(totals["enabled"] if totals else 0),
        "disabled": int(totals["disabled"] if totals else 0),
        "recent_disabled": [
            {
                "chat_id": int(row["chat_id"]),
                "last_error": str(row["last_error"]),
            }
            for row in recent_disabled_rows
        ],
    }


async def detect_userbot_targets_source() -> str:
    if TARGET_CHAT_IDS:
        return "WORKER_ENV"
    return "WORKER_DB"

async def remove_chat(chat_id: int) -> None:
    pool = await get_db_pool()
    await pool.execute("DELETE FROM chats WHERE chat_id = $1", chat_id)


async def get_chats() -> list[int]:
    pool = await get_db_pool()
    rows = await pool.fetch("SELECT chat_id FROM chats")
    return [row["chat_id"] for row in rows]


async def get_chat_rows() -> list[dict]:
    pool = await get_db_pool()
    rows = await pool.fetch(
        """
        SELECT chat_id, disabled, last_bot_post_msg_id, user_messages_since_last_post, last_error, last_success_post_at
        FROM chats
        """
    )
    return [
        {
            "chat_id": row["chat_id"],
            "disabled": bool(row["disabled"]),
            "last_bot_post_msg_id": row["last_bot_post_msg_id"],
            "user_messages_since_last_post": row["user_messages_since_last_post"] or 0,
            "last_error": row["last_error"],
            "last_success_post_at": row["last_success_post_at"],
        }
        for row in rows
    ]


async def increment_user_message_counter(chat_id: int) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        UPDATE chats
        SET user_messages_since_last_post = user_messages_since_last_post + 1
        WHERE chat_id = $1 AND disabled = 0
        """,
        chat_id,
    )


async def mark_chat_disabled(chat_id: int, error_text: str) -> None:
    pool = await get_db_pool()
    await pool.execute(
        "UPDATE chats SET disabled = 1, last_error = $1 WHERE chat_id = $2",
        error_text[:500],
        chat_id,
    )


async def mark_chat_send_success(chat_id: int, message_id: int) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        UPDATE chats
        SET last_bot_post_msg_id = $1,
            user_messages_since_last_post = 0,
            last_error = NULL,
            last_success_post_at = $2
        WHERE chat_id = $3
        """,
        message_id,
        datetime.now(timezone.utc).isoformat(),
        chat_id,
    )


async def set_meta(key: str, value: str) -> None:
    pool = await get_db_pool()
    await pool.execute(
        "INSERT INTO meta(key, value) VALUES ($1, $2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        key,
        value,
    )


async def get_meta(key: str) -> Optional[str]:
    pool = await get_db_pool()
    return await pool.fetchval("SELECT value FROM meta WHERE key = $1", key)


async def set_storage_chat_id(storage_chat_id: int) -> None:
    await set_meta(STORAGE_CHAT_META_KEY, str(storage_chat_id))


async def get_storage_chat_id() -> int:
    stored_value = await get_meta(STORAGE_CHAT_META_KEY)
    if stored_value:
        try:
            parsed = int(stored_value)
            if parsed != 0:
                return parsed
        except ValueError:
            pass
    return STORAGE_CHAT_ID


async def get_last_storage_message() -> tuple[Optional[int], Optional[int]]:
    last_message_raw = await get_meta(LAST_STORAGE_MESSAGE_ID_META_KEY)
    if not last_message_raw:
        return None, None

    try:
        last_message_id = int(last_message_raw)
    except ValueError:
        return None, None

    last_chat_raw = await get_meta(LAST_STORAGE_CHAT_ID_META_KEY)
    if not last_chat_raw:
        return None, last_message_id

    try:
        last_chat_id = int(last_chat_raw)
    except ValueError:
        return None, last_message_id

    return last_chat_id, last_message_id


async def set_last_storage_messages(storage_chat_id: int, message_ids: list[int]) -> None:
    normalized_ids = [int(message_id) for message_id in message_ids]
    if normalized_ids:
        await set_meta(LAST_STORAGE_MESSAGE_ID_META_KEY, str(normalized_ids[-1]))
    else:
        await set_meta(LAST_STORAGE_MESSAGE_ID_META_KEY, "")
    await set_meta(LAST_STORAGE_MESSAGE_IDS_META_KEY, json.dumps(normalized_ids))
    await set_meta(LAST_STORAGE_CHAT_ID_META_KEY, str(storage_chat_id))


async def get_last_storage_messages() -> tuple[Optional[int], list[int]]:
    message_ids_raw = await get_meta(LAST_STORAGE_MESSAGE_IDS_META_KEY)
    message_ids: list[int] = []

    if message_ids_raw:
        try:
            parsed = json.loads(message_ids_raw)
            if isinstance(parsed, list):
                message_ids = [int(item) for item in parsed]
        except (ValueError, TypeError):
            logger.warning("Failed to parse %s", LAST_STORAGE_MESSAGE_IDS_META_KEY)

    if not message_ids:
        _, legacy_message_id = await get_last_storage_message()
        if legacy_message_id is not None:
            message_ids = [legacy_message_id]

    last_chat_raw = await get_meta(LAST_STORAGE_CHAT_ID_META_KEY)
    if not last_chat_raw:
        return None, message_ids

    try:
        return int(last_chat_raw), message_ids
    except ValueError:
        return None, message_ids


async def get_broadcast_meta() -> dict:
    now = datetime.now(TZ)
    day_key = now.strftime("%Y-%m-%d")
    stored_day_key = await get_meta("day_key")
    daily_count_raw = await get_meta("daily_broadcast_count")
    daily_count = int(daily_count_raw) if (daily_count_raw and daily_count_raw.isdigit()) else 0

    if stored_day_key != day_key:
        await set_meta("day_key", day_key)
        await set_meta("daily_broadcast_count", "0")
        daily_count = 0

    return {
        "last_broadcast_at": await get_meta("last_broadcast_at"),
        "daily_broadcast_count": daily_count,
        "day_key": day_key,
        "manual_confirmed_day": await get_meta("manual_confirmed_day"),
    }


async def get_post() -> dict:
    pool = await get_db_pool()
    row = await pool.fetchrow(
        "SELECT source_chat_id, source_message_id, storage_chat_id, storage_message_ids, has_media, media_group_id, contact_token FROM post WHERE id = 1"
    )
    if not row:
        return {
            "source_chat_id": None,
            "source_message_id": None,
            "storage_chat_id": None,
            "source_message_ids": [],
            "has_media": False,
            "media_group_id": None,
            "contact_token": None,
        }
    source_message_ids = list(row["storage_message_ids"] or [])
    if not source_message_ids and row["source_message_id"] is not None:
        source_message_ids = [row["source_message_id"]]
    return {
        "source_chat_id": row["source_chat_id"],
        "source_message_id": row["source_message_id"],
        "storage_chat_id": row["storage_chat_id"],
        "source_message_ids": source_message_ids,
        "has_media": bool(row["has_media"]),
        "media_group_id": row["media_group_id"],
        "contact_token": row["contact_token"],
    }


async def save_post(
    source_chat_id: int,
    source_message_ids: list[int],
    has_media: bool,
    media_group_id: Optional[str],
    contact_token: Optional[str],
) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        UPDATE post
        SET source_chat_id = $1,
            source_message_id = $2,
            storage_chat_id = $1,
            storage_message_ids = $3::BIGINT[],
            has_media = $4,
            media_group_id = $5,
            contact_token = $6
        WHERE id = 1
        """,
        source_chat_id,
        (source_message_ids[0] if source_message_ids else None),
        source_message_ids,
        int(has_media),
        media_group_id,
        contact_token,
    )


async def get_buyer_reply_template() -> tuple[Optional[int], Optional[int]]:
    pool = await get_db_pool()
    row = await pool.fetchrow("SELECT buyer_reply_storage_chat_id, buyer_reply_message_id FROM bot_settings WHERE id = 1")
    if not row:
        return None, None
    return row["buyer_reply_storage_chat_id"], row["buyer_reply_message_id"]


async def save_buyer_reply_template(storage_chat_id: int, message_id: int) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        INSERT INTO bot_settings (id, buyer_reply_storage_chat_id, buyer_reply_message_id)
        VALUES (1, $1, $2)
        ON CONFLICT (id)
        DO UPDATE SET
            buyer_reply_storage_chat_id = excluded.buyer_reply_storage_chat_id,
            buyer_reply_message_id = excluded.buyer_reply_message_id
        """,
        storage_chat_id,
        message_id,
    )


async def create_userbot_task(
    storage_chat_id: int,
    storage_message_ids: list[int],
    target_chat_ids: list[int] | None,
    run_at: datetime,
) -> Optional[int]:
    def _logical_schedule_bucket(run_at_value: datetime) -> str:
        return run_at_value.strftime("%Y%m%d%H%M")

    normalized_target_chat_ids = sorted(set(target_chat_ids or []))
    normalized_storage_message_ids = sorted(storage_message_ids or [])
    run_at_utc = run_at if run_at.tzinfo else run_at.replace(tzinfo=timezone.utc)
    run_at_utc = run_at_utc.astimezone(timezone.utc)
    schedule_bucket = _logical_schedule_bucket(run_at_utc)

    storage_message_id = normalized_storage_message_ids[0] if normalized_storage_message_ids else None
    dedupe_key = (
        f"v2:{storage_chat_id}:"
        f"{','.join(str(message_id) for message_id in normalized_storage_message_ids)}:"
        f"{schedule_bucket}"
    )

    pool = await get_db_pool()
    await ensure_userbot_tasks_schema(pool)
    existing_columns = await get_userbot_tasks_columns(pool)

    insert_columns: list[str] = ["storage_chat_id", "storage_message_ids", "run_at", "status", "target_chat_ids"]
    insert_values: list[object] = [
        storage_chat_id,
        normalized_storage_message_ids,
        run_at_utc,
        "pending",
        normalized_target_chat_ids,
    ]

    if "storage_message_id" in existing_columns:
        insert_columns.append("storage_message_id")
        insert_values.append(storage_message_id)
    if "attempts" in existing_columns:
        insert_columns.append("attempts")
        insert_values.append(0)
    if "last_error" in existing_columns:
        insert_columns.append("last_error")
        insert_values.append(None)

    dedupe_enabled = "dedupe_key" in existing_columns and "status" in existing_columns
    if dedupe_enabled:
        insert_columns.append("dedupe_key")
        insert_values.append(dedupe_key)

    placeholders: list[str] = []
    for index, column in enumerate(insert_columns, start=1):
        if column in {"storage_message_ids", "target_chat_ids"}:
            placeholders.append(f"${index}::BIGINT[]")
        elif column == "run_at":
            placeholders.append(f"${index}::TIMESTAMPTZ")
        else:
            placeholders.append(f"${index}")

    query = (
        f"INSERT INTO userbot_tasks({', '.join(insert_columns)}) "
        f"VALUES ({', '.join(placeholders)})"
    )
    if dedupe_enabled:
        query += " " + (
            "ON CONFLICT (dedupe_key) "
            "WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'processing') "
            "DO NOTHING"
        )
    query += " RETURNING id"

    row = await pool.fetchrow(query, *insert_values)
    if row:
        return int(row["id"])

    if not dedupe_enabled:
        return None

    existing_task_id = await pool.fetchval(
        """
        SELECT id
        FROM userbot_tasks
        WHERE dedupe_key = $1
          AND status IN ('pending', 'processing')
        ORDER BY id DESC
        LIMIT 1
        """,
        dedupe_key,
    )
    if existing_task_id:
        logger.info(
            "Userbot task deduplicated by unique index: existing task id=%s dedupe_key=%s",
            existing_task_id,
            dedupe_key,
        )
        return int(existing_task_id)
    return None


async def find_schedule(schedule_type: str, time_text: Optional[str], weekday: Optional[int]) -> Optional[int]:
    pool = await get_db_pool()
    return await pool.fetchval(
        """
        SELECT id
        FROM schedules
        WHERE schedule_type = $1
          AND enabled = TRUE
          AND time_text IS NOT DISTINCT FROM $2
          AND weekday IS NOT DISTINCT FROM $3
        LIMIT 1
        """,
        schedule_type,
        time_text,
        weekday,
    )


async def add_schedule(schedule_type: str, time_text: Optional[str], weekday: Optional[int]) -> Optional[int]:
    pool = await get_db_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO schedules(schedule_type, time_text, weekday, enabled, created_at)
        VALUES ($1, $2, $3, TRUE, $4)
        ON CONFLICT (schedule_type, COALESCE(time_text, ''), COALESCE(weekday, -1)) DO NOTHING
        RETURNING id
        """,
        schedule_type,
        time_text,
        weekday,
        datetime.now(TZ).isoformat(),
    )
    return row["id"] if row else None


async def get_schedules() -> list[dict]:
    pool = await get_db_pool()
    rows = await pool.fetch(
        """
        SELECT id, schedule_type, time_text, weekday
        FROM schedules
        WHERE enabled = TRUE
        ORDER BY schedule_type, weekday NULLS FIRST, time_text
        """
    )
    return [
        {"id": row["id"], "schedule_type": row["schedule_type"], "time_text": row["time_text"], "weekday": row["weekday"]}
        for row in rows
    ]


async def get_schedule(schedule_id: int) -> Optional[dict]:
    pool = await get_db_pool()
    row = await pool.fetchrow(
        "SELECT id, schedule_type, time_text, weekday FROM schedules WHERE id = $1 AND enabled = TRUE",
        schedule_id,
    )
    if not row:
        return None
    return {"id": row["id"], "schedule_type": row["schedule_type"], "time_text": row["time_text"], "weekday": row["weekday"]}


async def update_schedule_time(schedule_id: int, time_text: str) -> None:
    pool = await get_db_pool()
    try:
        await pool.execute("UPDATE schedules SET time_text = $1 WHERE id = $2", time_text, schedule_id)
    except asyncpg.UniqueViolationError:
        raise ValueError("duplicate_schedule")


async def delete_schedule(schedule_id: int) -> None:
    pool = await get_db_pool()
    await pool.execute("DELETE FROM schedules WHERE id = $1", schedule_id)

def parse_time(value: str) -> Optional[str]:
    try:
        parsed = datetime.strptime(value.strip(), "%H:%M")
        return parsed.strftime("%H:%M")
    except ValueError:
        return None


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def is_chat_ready_by_activity(chat: dict) -> bool:
    if chat["last_bot_post_msg_id"] is None or chat.get("last_success_post_at") is None:
        return True
    return chat["user_messages_since_last_post"] >= MIN_USER_MESSAGES_BETWEEN_POSTS


def has_privacy_mode_symptoms(chats: list[dict]) -> bool:
    if not chats:
        return False
    active_chats = [c for c in chats if not c["disabled"]]
    if not active_chats:
        return False
    return all(
        c["last_bot_post_msg_id"] is not None and c["user_messages_since_last_post"] == 0
        for c in active_chats
    )


def is_quiet_hours_now() -> bool:
    start = parse_time(QUIET_HOURS_START)
    end = parse_time(QUIET_HOURS_END)
    if not start or not end:
        return False

    now_local = datetime.now(TZ).time()
    start_t = datetime.strptime(start, "%H:%M").time()
    end_t = datetime.strptime(end, "%H:%M").time()

    if start_t <= end_t:
        return start_t <= now_local < end_t
    return now_local >= start_t or now_local < end_t


def schedule_label(item: dict) -> str:
    if item["schedule_type"] == "daily":
        return f"📅 Ежедневно {item['time_text']}"
    if item["schedule_type"] == "hourly":
        return "🕐 Каждый час"
    weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    day_name = weekdays[item["weekday"]] if item["weekday"] is not None else "?"
    return f"📆 {day_name} {item['time_text']}"


def _generate_contact_token() -> str:
    return uuid.uuid4().hex[:16]


def _resolve_bot_username() -> str:
    return BOT_USERNAME or CONFIGURED_BOT_USERNAME


def _contains_contact_cta(text: Optional[str]) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if "start=contact_" in lowered:
        return True
    return text.rstrip().casefold().endswith("написать продавцу")


def _build_contact_cta_entities(prefix_text: str, cta_text: str, contact_url: str) -> list[MessageEntity]:
    def _utf16_len(value: str) -> int:
        return len(value.encode("utf-16-le")) // 2

    offset = _utf16_len(prefix_text)
    return [
        MessageEntity(
            type="text_link",
            offset=offset,
            length=_utf16_len(cta_text),
            url=contact_url,
        )
    ]


def _merge_entities(original_entities: Optional[list[MessageEntity]], extra_entities: list[MessageEntity]) -> list[MessageEntity]:
    merged: list[MessageEntity] = []
    if original_entities:
        merged.extend(original_entities)
    merged.extend(extra_entities)
    return merged


async def _send_buyer_reply(user_id: int) -> None:
    storage_chat_id, message_id = await get_buyer_reply_template()
    if storage_chat_id and message_id:
        try:
            await bot.copy_message(chat_id=user_id, from_chat_id=storage_chat_id, message_id=message_id)
            return
        except Exception as exc:
            logger.warning("Failed to send buyer reply template copy, fallback to text: %s", exc)
    await bot.send_message(user_id, "Спасибо! Ваше сообщение отправлено продавцу ✅")


async def _save_buyer_reply_template_from_message(message: Message) -> None:
    storage_chat_id = await get_storage_chat_id()
    if storage_chat_id == 0:
        raise RuntimeError("STORAGE_CHAT_ID не настроен")

    copied = await bot.copy_message(
        chat_id=storage_chat_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
    )
    await save_buyer_reply_template(storage_chat_id, copied.message_id)


async def _send_post_with_cta(
    source_message: Message,
    storage_chat_id: int,
    token: str,
    force_cta_only: bool = False,
) -> int:
    bot_username = _resolve_bot_username()
    payload = f"{CONTACT_PAYLOAD_PREFIX}{token}"
    contact_url = f"https://t.me/{bot_username}?start={payload}"

    raw_text = source_message.text or ""
    raw_caption = source_message.caption or ""
    has_media = bool(source_message.media_group_id or source_message.photo or source_message.video or source_message.document or source_message.animation)
    content_text = raw_caption if has_media else raw_text
    original_entities = list(source_message.caption_entities or []) if has_media else list(source_message.entities or [])

    should_append_cta = bool(bot_username) and not force_cta_only and not _contains_contact_cta(content_text)

    if not force_cta_only and not should_append_cta:
        copied = await bot.copy_message(
            chat_id=storage_chat_id,
            from_chat_id=source_message.chat.id,
            message_id=source_message.message_id,
        )
        return copied.message_id

    new_tail = CONTACT_CTA_TEXT
    if should_append_cta and content_text:
        updated = f"{content_text}\n\n{new_tail}"
        prefix = f"{content_text}\n\n"
    elif should_append_cta and not content_text:
        updated = new_tail
        prefix = ""
    else:
        updated = new_tail
        prefix = ""

    cta_entities = _build_contact_cta_entities(prefix, CONTACT_CTA_TEXT, contact_url)
    entities = _merge_entities(original_entities, cta_entities)

    if has_media:
        copied = await bot.copy_message(
            chat_id=storage_chat_id,
            from_chat_id=source_message.chat.id,
            message_id=source_message.message_id,
            caption=updated,
            caption_entities=entities,
        )
    else:
        copied = await bot.send_message(storage_chat_id, updated, entities=entities)
    return copied.message_id



async def send_post_preview(message: Message) -> None:
    post = await get_post()
    source_chat_id = post["source_chat_id"]
    source_message_ids = post["source_message_ids"]

    if not source_chat_id or not source_message_ids:
        await message.answer("Текущий пост:\n\n(пусто)")
        return

    await bot.copy_message(
        chat_id=message.chat.id,
        from_chat_id=source_chat_id,
        message_id=source_message_ids[0],
    )




async def _delete_previous_storage_post(storage_chat_id: int) -> None:
    previous_storage_chat_id, previous_storage_message_ids = await get_last_storage_messages()
    if not previous_storage_message_ids:
        return

    delete_chat_id = previous_storage_chat_id or storage_chat_id
    for previous_storage_message_id in previous_storage_message_ids:
        logger.info(
            "Trying to delete previous storage post chat_id=%s message_id=%s",
            delete_chat_id,
            previous_storage_message_id,
        )
        try:
            await bot.delete_message(delete_chat_id, previous_storage_message_id)
        except TelegramBadRequest as exc:
            logger.warning(
                "Skipping previous storage post deletion chat_id=%s message_id=%s: %s",
                delete_chat_id,
                previous_storage_message_id,
                exc,
            )


async def _publish_post_messages(messages: list[Message], state: FSMContext, media_group_id: Optional[str]) -> None:
    if not messages:
        return

    storage_chat_id = await get_storage_chat_id()
    if storage_chat_id == 0:
        await messages[-1].answer("Ошибка: STORAGE_CHAT_ID не настроен.")
        return

    token = _generate_contact_token()
    sorted_messages = sorted(messages, key=lambda msg: msg.message_id)
    copied_ids: list[int] = []

    first_message = sorted_messages[0]
    first_has_media = bool(
        first_message.media_group_id
        or first_message.photo
        or first_message.video
        or first_message.document
        or first_message.animation
    )
    first_content = first_message.caption if first_has_media else first_message.text
    bot_username = _resolve_bot_username()
    if not bot_username and not _contains_contact_cta(first_content):
        await messages[-1].answer("❌ BOT_USERNAME не настроен, не могу добавить кликабельный CTA в пост.")
        return

    will_add_cta = bool(bot_username) and not _contains_contact_cta(first_content)
    cta_appendix = f"\n\n{CONTACT_CTA_TEXT}" if first_content else CONTACT_CTA_TEXT
    final_content = (first_content or "") + (cta_appendix if will_add_cta else "")
    max_len = 1024 if first_has_media else 4096
    if len(final_content) > max_len:
        if first_has_media:
            await messages[-1].answer(
                f"❌ Слишком длинная подпись (caption). Лимит 1024 символа. Сейчас: {len(final_content)}. "
                "Сократите текст или сделайте пост текстовым сообщением."
            )
        else:
            await messages[-1].answer(
                f"❌ Слишком длинный текст. Лимит 4096 символов. Сейчас: {len(final_content)}. "
                "Сократите текст."
            )
        return

    previous_storage_chat_id, previous_storage_message_ids = await get_last_storage_messages()

    try:
        if len(sorted_messages) == 1:
            copied_ids.append(await _send_post_with_cta(sorted_messages[0], storage_chat_id, token))
        else:
            payload = f"{CONTACT_PAYLOAD_PREFIX}{token}"
            contact_url = f"https://t.me/{bot_username}?start={payload}"

            for idx, item in enumerate(sorted_messages):
                if idx == 0 and will_add_cta and bot_username:
                    original_caption = item.caption or ""
                    updated_caption = f"{original_caption}\n\n{CONTACT_CTA_TEXT}" if original_caption else CONTACT_CTA_TEXT
                    prefix = f"{original_caption}\n\n" if original_caption else ""
                    entities = _merge_entities(
                        list(item.caption_entities or []),
                        _build_contact_cta_entities(prefix, CONTACT_CTA_TEXT, contact_url),
                    )
                    copied = await bot.copy_message(
                        chat_id=storage_chat_id,
                        from_chat_id=item.chat.id,
                        message_id=item.message_id,
                        caption=updated_caption,
                        caption_entities=entities,
                    )
                else:
                    copied = await bot.copy_message(
                        chat_id=storage_chat_id,
                        from_chat_id=item.chat.id,
                        message_id=item.message_id,
                    )
                copied_ids.append(copied.message_id)
    except Exception as exc:
        logger.exception("Failed to publish new storage post")
        await messages[-1].answer(f"❌ Не удалось опубликовать новый пост в storage: {exc}")
        return

    logger.info("Published new storage post chat_id=%s message_ids=%s", storage_chat_id, copied_ids)
    try:
        await set_last_storage_messages(storage_chat_id, copied_ids)
        await save_post(
            source_chat_id=storage_chat_id,
            source_message_ids=copied_ids,
            has_media=any(msg.photo or msg.video or msg.animation or msg.document for msg in messages),
            media_group_id=media_group_id,
            contact_token=token,
        )
    except Exception as exc:
        logger.exception("Failed to persist storage metadata")
        await messages[-1].answer(f"❌ Новый пост отправлен, но не удалось сохранить его в БД: {exc}")
        return

    if previous_storage_message_ids:
        delete_chat_id = previous_storage_chat_id or storage_chat_id
        for previous_storage_message_id in previous_storage_message_ids:
            try:
                await bot.delete_message(delete_chat_id, previous_storage_message_id)
            except TelegramBadRequest as exc:
                logger.warning(
                    "Skipping previous storage post deletion chat_id=%s message_id=%s: %s",
                    delete_chat_id,
                    previous_storage_message_id,
                    exc,
                )

    data = await state.get_data()
    mode = data.get("mode")
    await state.clear()

    if mode == "edit":
        await messages[-1].answer("Пост обновлён. Существующие расписания сохранены ✅")
    else:
        await messages[-1].answer("Пост сохранён ✅")

    await send_post_actions(messages[-1])



async def _flush_media_group(media_group_id: str, state: FSMContext) -> None:
    buffer_data = media_group_buffers.pop(media_group_id, None)
    if not buffer_data:
        return
    await _publish_post_messages(buffer_data["messages"], state, media_group_id=media_group_id)

async def send_schedule_list(message: Message) -> None:
    schedules = await get_schedules()
    title = "Расписания:" if schedules else "Расписаний пока нет."
    rows = []
    for item in schedules:
        rows.append(
            [
                InlineKeyboardButton(
                    text=schedule_label(item), callback_data=f"schedule:item:{item['id']}"
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="➕ Добавить ещё", callback_data="schedule:add_time")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="schedule:back_to_post")])
    await message.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def send_edit_post_menu(message: Message) -> None:
    await message.answer(
        "Что вы хотите изменить?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⏰ Изменить время", callback_data="edit:time")],
                [InlineKeyboardButton(text="📝 Изменить пост", callback_data="edit:post")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="edit:back")],
            ]
        ),
    )


@dp.callback_query(F.data == "edit:menu")
async def edit_menu_callback(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.clear()
    await send_edit_post_menu(callback.message)
    await callback.answer()


async def send_post_actions(message: Message) -> None:
    await send_post_preview(message)
    await message.answer(
        "Выберите действие:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить время", callback_data="schedule:add_time")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="schedule:back_main")],
            ]
        ),
    )


async def send_admin_report(report: dict) -> None:
    lines = [
        "📊 Отчёт рассылки",
        f"Всего чатов: {report['total_chats']}",
        f"Отправлено: {report['sent']}",
        f"Готовы по активности: {report['activity_ready']}",
        f"Пропущено (5+ сообщений): {report['skipped_by_activity']}",
        f"Пропущено (антидубль): {report['skipped_by_duplicate']}",
        f"Пропущено (выключены): {report['skipped_disabled']}",
        f"Ошибки/права: {report['errors']}",
    ]
    if report.get("privacy_warning"):
        lines.extend(
            [
                "⚠️ Похоже, счётчики сообщений не растут.",
                "Проверьте BotFather → /setprivacy → Disable или выдайте боту права администратора.",
            ]
        )
    try:
        await bot.send_message(ADMIN_ID, "\n".join(lines))
    except Exception as exc:
        logger.warning("Failed to send admin report: %s", exc)


def should_send_admin_report(report: dict) -> bool:
    if not REPORT_ONLY_ON_ERRORS:
        return False
    return report.get("errors", 0) > 0


async def is_manual_confirmation_required(meta: dict) -> bool:
    if not MANUAL_CONFIRM_FIRST_BROADCAST:
        return False
    if meta['manual_confirmed_day'] == meta['day_key']:
        return False
    return True


async def broadcast_once() -> None:
    post = await get_post()
    source_chat_id = post["source_chat_id"]
    source_message_ids = post["source_message_ids"]

    if not source_chat_id or not source_message_ids:
        logger.info("Broadcast skipped: empty post")
        return

    meta = await get_broadcast_meta()
    if await is_manual_confirmation_required(meta):
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✅ Запустить сегодня", callback_data="broadcast:confirm_today")]]
        )
        await bot.send_message(
            ADMIN_ID,
            "Требуется ручное подтверждение первой рассылки дня. Нажмите кнопку для запуска.",
            reply_markup=keyboard,
        )
        logger.info("Broadcast skipped: waiting manual daily confirmation")
        return

    if is_quiet_hours_now():
        logger.info("Broadcast skipped: quiet hours window")
        return

    if meta['daily_broadcast_count'] >= BROADCAST_MAX_PER_DAY > 0:
        await bot.send_message(ADMIN_ID, "⚠️ Достигнут дневной лимит запусков рассылки.")
        logger.info("Broadcast skipped: daily limit reached")
        return

    last_broadcast_dt = parse_iso_datetime(meta["last_broadcast_at"])
    if last_broadcast_dt:
        delta_seconds = (datetime.now(timezone.utc) - last_broadcast_dt).total_seconds()
        if delta_seconds < GLOBAL_BROADCAST_COOLDOWN_SECONDS:
            logger.info("Broadcast skipped: global cooldown %.1fs", delta_seconds)
            return

    report = {
        "total_chats": 0,
        "sent": 0,
        "activity_ready": 0,
        "skipped_by_activity": 0,
        "skipped_by_duplicate": 0,
        "skipped_disabled": 0,
        "errors": 0,
        "privacy_warning": False,
    }
    broadcast_recorded = False

    if DELIVERY_MODE == "userbot":
        run_at = datetime.now(timezone.utc)
        target_chat_ids_to_store = TARGET_CHAT_IDS if TARGET_CHAT_IDS else None
        task_id = await create_userbot_task(source_chat_id, source_message_ids, target_chat_ids_to_store, run_at)
        if task_id:
            await save_broadcast_attempt(status="queued", chat_id=None, reason="queued_for_userbot")
            broadcast_recorded = True
            logger.info(
                "Queued userbot task id=%s run_at=%s storage_chat_id=%s msg_ids=%s targets=%s",
                task_id,
                run_at.isoformat(),
                source_chat_id,
                source_message_ids,
                target_chat_ids_to_store if target_chat_ids_to_store is not None else "WORKER_ENV",
            )
            await bot.send_message(ADMIN_ID, "🧾 Userbot-рассылка поставлена в очередь.")
        else:
            logger.info("Userbot task is already pending/running for this payload")
            await bot.send_message(ADMIN_ID, "ℹ️ Такая userbot-задача уже в очереди.")
    else:
        chats = await get_chat_rows()
        report["total_chats"] = len(chats)
        report["privacy_warning"] = has_privacy_mode_symptoms(chats)

        for chat in chats:
            chat_id = chat["chat_id"]
            if chat["disabled"]:
                report["skipped_disabled"] += 1
                await save_broadcast_attempt(status="skipped", chat_id=chat_id, reason="disabled")
                continue

            if not is_chat_ready_by_activity(chat):
                report["skipped_by_activity"] += 1
                await save_broadcast_attempt(status="skipped", chat_id=chat_id, reason="activity_gate")
                continue
            report["activity_ready"] += 1

            if ANTI_DUPLICATE_HOURS > 0 and chat["last_success_post_at"]:
                last_chat_send = parse_iso_datetime(chat["last_success_post_at"])
                if last_chat_send:
                    hours_since = (datetime.now(timezone.utc) - last_chat_send).total_seconds() / 3600
                    if hours_since < ANTI_DUPLICATE_HOURS:
                        report["skipped_by_duplicate"] += 1
                        await save_broadcast_attempt(status="skipped", chat_id=chat_id, reason="anti_duplicate")
                        continue

            try:
                sent_messages = []
                for source_message_id in source_message_ids:
                    sent = await bot.copy_message(
                        chat_id=chat_id,
                        from_chat_id=source_chat_id,
                        message_id=source_message_id,
                    )
                    sent_messages.append(sent)

                if sent_messages:
                    await mark_chat_send_success(chat_id, sent_messages[-1].message_id)
                await save_broadcast_attempt(status="sent", chat_id=chat_id, reason="ok")
                report["sent"] += 1
                await asyncio.sleep(random.uniform(MIN_SECONDS_BETWEEN_CHATS, MAX_SECONDS_BETWEEN_CHATS))
            except TelegramRetryAfter as exc:
                await asyncio.sleep(exc.retry_after)
                report["errors"] += 1
                await save_broadcast_attempt(status="error", chat_id=chat_id, reason="retry_after", error_text=str(exc))
                logger.warning("RetryAfter for chat %s: %s", chat_id, exc)
            except TelegramForbiddenError as exc:
                await mark_chat_disabled(chat_id, str(exc))
                report["errors"] += 1
                await save_broadcast_attempt(status="error", chat_id=chat_id, reason="forbidden", error_text=str(exc))
                logger.warning("Forbidden for chat %s: %s", chat_id, exc)
            except Exception as exc:
                report["errors"] += 1
                await set_meta(f"last_error_chat_{chat_id}", str(exc)[:500])
                await save_broadcast_attempt(status="error", chat_id=chat_id, reason="exception", error_text=str(exc))
                logger.warning("Failed to send to chat %s: %s", chat_id, exc)

        broadcast_recorded = report["sent"] > 0

    if broadcast_recorded:
        await set_meta("last_broadcast_at", datetime.now(timezone.utc).isoformat())
        await set_meta("daily_broadcast_count", str(meta["daily_broadcast_count"] + 1))

    if should_send_admin_report(report):
        await send_admin_report(report)



def register_schedule_job(item: dict) -> None:
    job_id = f"sched:{item['id']}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    if item["schedule_type"] == "daily" and item["time_text"]:
        hour, minute = map(int, item["time_text"].split(":"))
        trigger = CronTrigger(hour=hour, minute=minute, timezone=TZ)
    elif item["schedule_type"] == "hourly":
        trigger = CronTrigger(minute=0, timezone=TZ)
    elif item["schedule_type"] == "weekly" and item["time_text"] is not None and item["weekday"] is not None:
        hour, minute = map(int, item["time_text"].split(":"))
        trigger = CronTrigger(day_of_week=item["weekday"], hour=hour, minute=minute, timezone=TZ)
    else:
        return

    scheduler.add_job(broadcast_once, trigger=trigger, id=job_id, replace_existing=True)


def remove_schedule_job(schedule_id: int) -> None:
    job_id = f"sched:{schedule_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)


@dp.my_chat_member()
async def track_chat_membership(update: ChatMemberUpdated):
    chat = update.chat
    if chat.type not in {"group", "supergroup"}:
        return

    old_status = update.old_chat_member.status
    new_status = update.new_chat_member.status

    if new_status in {"member", "administrator", "creator"}:
        await save_chat(chat.id)
    elif new_status in {"left", "kicked"} and old_status in {"member", "administrator", "creator"}:
        await remove_chat(chat.id)


@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def remember_chat_from_messages(message: Message):
    await save_chat(message.chat.id)

    if not message.from_user:
        return
    if message.from_user.is_bot:
        return
    if BOT_ID and message.from_user.id == BOT_ID:
        return

    await increment_user_message_counter(message.chat.id)


@dp.message(F.text == "📊 Статус")
async def admin_status(message: Message):
    if not ensure_admin(message):
        return

    meta = await get_broadcast_meta()
    total = 0
    disabled = 0
    active_ready = 0
    blocked_by_activity = 0
    privacy_warning = False

    if DELIVERY_MODE != "userbot":
        chats = await get_chat_rows()
        total = len(chats)
        disabled = sum(1 for c in chats if c["disabled"])
        active_ready = sum(
            1
            for c in chats
            if not c["disabled"] and is_chat_ready_by_activity(c)
        )
        blocked_by_activity = sum(
            1
            for c in chats
            if not c["disabled"] and not is_chat_ready_by_activity(c)
        )
        privacy_warning = has_privacy_mode_symptoms(chats)

    recent_attempts = await get_recent_broadcast_attempts(limit=5)

    attempt_lines = []
    for item in recent_attempts:
        at = item["created_at"].replace("T", " ")[:19]
        chat_text = str(item["chat_id"]) if item["chat_id"] is not None else "-"
        reason = item["reason"] or "-"
        err = f" ({item['error_text'][:80]})" if item.get("error_text") else ""
        attempt_lines.append(f"• {at} | chat {chat_text} | {item['status']} | {reason}{err}")

    warning_lines = []
    if privacy_warning:
        warning_lines = [
            "⚠️ Похоже, бот не видит сообщения в чатах (privacy mode).",
            "Подсказка: BotFather → /setprivacy → Disable или выдайте боту админ-права.",
        ]

    lines = [
        "📊 Статус рассылки",
        f"Режим доставки: {DELIVERY_MODE}",
        f"Глобальная пауза: {GLOBAL_BROADCAST_COOLDOWN_SECONDS} сек.",
        f"Лимит запусков/сутки: {BROADCAST_MAX_PER_DAY}",
        f"Запусков сегодня: {meta['daily_broadcast_count']}",
        f"Последняя рассылка UTC: {meta['last_broadcast_at'] or '-'}",
        f"Тихие часы: {parse_time(QUIET_HOURS_START) or '-'} → {parse_time(QUIET_HOURS_END) or '-'}",
    ]

    if DELIVERY_MODE == "userbot":
        targets_source = await detect_userbot_targets_source()
        lines.append(f"Targets source: {targets_source}")
        stats = await userbot_targets_stats()
        if stats is not None:
            lines.append(
                f"userbot targets: total={stats['total']} enabled={stats['enabled']} disabled={stats['disabled']}"
            )
            for item in stats["recent_disabled"]:
                lines.append(f"• disabled {item['chat_id']}: {item['last_error'][:80]}")
    else:
        lines.extend([
            f"Всего чатов: {total}",
            f"Отключено: {disabled}",
            f"Готовы по правилу активности: {active_ready}",
            f"Заблокированы правилом активности: {blocked_by_activity}",
            *warning_lines,
        ])

    lines.extend([
        "",
        "Последние попытки рассылки:",
        *(attempt_lines or ["• пока нет"]),
    ])

    await message.answer("\n".join(lines))


@dp.message(F.text == "🚀 Запустить сейчас")
async def admin_broadcast_now(message: Message) -> None:
    if not ensure_admin(message):
        return

    if broadcast_now_lock.locked():
        await message.answer("⏳ Расссылка уже запускается, подождите...")
        return

    async with broadcast_now_lock:
        await message.answer("🚀 Запускаю рассылку сейчас...")
        logger.info("Manual broadcast triggered by admin_id=%s", message.from_user.id if message.from_user else None)

        try:
            await broadcast_once()
        except Exception as exc:
            logger.exception("Manual broadcast failed")
            await message.answer(f"❌ Ошибка запуска: {exc}")
            return

        logger.info("Manual broadcast finished")
        await message.answer("✅ Готово. Проверьте 📊 Статус и логи воркера.")



@dp.callback_query(F.data == "broadcast:confirm_today")
async def confirm_daily_broadcast(callback: CallbackQuery):
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return

    day_key = datetime.now(TZ).strftime("%Y-%m-%d")
    await set_meta("manual_confirmed_day", day_key)
    await callback.answer("Подтверждено")

    if callback.message:
        await callback.message.answer("Подтверждение получено. Запускаю рассылку ✅")
    await broadcast_once()


@dp.message(CommandStart())
async def on_start(message: Message, state: FSMContext):
    args = (message.text or "").split(maxsplit=1)
    payload = args[1] if len(args) > 1 else ""

    if payload.startswith(CONTACT_PAYLOAD_PREFIX) and message.chat.type == "private":
        token = payload[len(CONTACT_PAYLOAD_PREFIX) :]
        if not token:
            await message.answer("Ссылка устарела или неверна.")
            return

        pool = await get_db_pool()
        token_exists = await pool.fetchval("SELECT 1 FROM post WHERE contact_token = $1", token)
        if not token_exists:
            await message.answer("Ссылка устарела или неверна.")
            return

        await state.set_state(ContactStates.waiting_message)
        await state.update_data(contact_token=token)
        await message.answer("Напишите сообщение продавцу одним сообщением. Я перешлю.")
        return

    if payload == SUPPORT_PAYLOAD and message.chat.type == "private":
        await state.set_state(SupportStates.waiting_message)
        if SELLER_USERNAME:
            await message.answer(
                f"Напишите сообщение здесь, я тут же передам его продавцу или свяжитесь с ним лично @{SELLER_USERNAME}"
            )
        else:
            await message.answer("Напишите сообщение здесь, я тут же передам его продавцу.")
        return

    if ensure_admin(message):
        await message.answer("Главное меню:", reply_markup=admin_menu_keyboard())
    else:
        await message.answer("Здравствуйте! Используйте кнопку из поста для связи с продавцом.")


@dp.message(F.text == "📌 Добавить пост")
async def add_post_start(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    await state.set_state(AdminStates.waiting_post_content)
    await state.update_data(mode="add")
    await message.answer("Отправьте пост (текст или фото с подписью).")


@dp.message(F.text == "✏️ Изменить пост")
async def edit_post_start(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    await state.clear()
    await send_edit_post_menu(message)


@dp.message(F.text.in_({"/settings", "/admin", "📝 Изменить автоответ покупателю"}))
async def buyer_reply_settings_start(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    await state.set_state(AdminStates.waiting_buyer_reply_template)
    await message.answer("Отправьте новое сообщение-автоответ (можно с премиум эмодзи).")


@dp.message(AdminStates.waiting_buyer_reply_template)
async def buyer_reply_settings_save(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return

    try:
        await _save_buyer_reply_template_from_message(message)
    except Exception as exc:
        logger.error("Buyer reply template save failed: %s", exc)
        await message.answer("Не удалось сохранить автоответ. Проверьте STORAGE_CHAT_ID и попробуйте снова.")
        return

    await state.clear()
    await message.answer("Автоответ обновлён ✅")


@dp.callback_query(F.data == "edit:time")
async def edit_time_selected(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.clear()
    await send_schedule_list(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "edit:post")
async def edit_post_selected(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_post_content)
    await state.update_data(mode="edit")
    await callback.message.answer("Отправьте новый пост (текст или фото с подписью).")
    await callback.answer()


@dp.callback_query(F.data == "edit:back")
async def edit_back_selected(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await state.clear()
    await callback.message.answer("Главное меню:", reply_markup=admin_menu_keyboard())
    await callback.answer()


@dp.message(AdminStates.waiting_post_content)
async def receive_post_content(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return

    if not message.media_group_id:
        await _publish_post_messages([message], state, media_group_id=None)
        return

    media_group_id = str(message.media_group_id)
    buffer_data = media_group_buffers.get(media_group_id)
    if not buffer_data:
        buffer_data = {"messages": []}
        media_group_buffers[media_group_id] = buffer_data

    buffer_data["messages"].append(message)

    pending_task = buffer_data.get("task")
    if pending_task:
        pending_task.cancel()

    buffer_data["task"] = asyncio.create_task(_flush_media_group_with_delay(media_group_id, state))


async def _flush_media_group_with_delay(media_group_id: str, state: FSMContext) -> None:
    try:
        await asyncio.sleep(MEDIA_GROUP_BUFFER_TIMEOUT_SECONDS)
        await _flush_media_group(media_group_id, state)
    except asyncio.CancelledError:
        pass


@dp.callback_query(F.data == "schedule:add_time")
async def schedule_add_time_menu(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Ежедневно", callback_data="schedule:type:daily")],
            [InlineKeyboardButton(text="🕐 Каждый час", callback_data="schedule:type:hourly")],
            [InlineKeyboardButton(text="📆 Еженедельно", callback_data="schedule:type:weekly")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="schedule:back_to_post")],
        ]
    )
    await callback.message.answer("Выберите тип расписания:", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data == "schedule:back_main")
async def back_main(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await callback.message.answer("Главное меню:", reply_markup=admin_menu_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "schedule:back_to_post")
async def back_to_post(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await send_post_actions(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "schedule:type:daily")
async def schedule_daily(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await state.set_state(AdminStates.waiting_daily_time)
    await callback.message.answer("Введите время в формате HH:MM. Пример: 09:30")
    await callback.answer()


@dp.callback_query(F.data == "schedule:type:hourly")
async def schedule_hourly(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    schedule_id = await add_schedule("hourly", None, None)
    if schedule_id is None:
        await callback.message.answer("⚠️ Такое время уже добавлено")
        await send_schedule_list(callback.message)
        await callback.answer()
        return

    item = await get_schedule(schedule_id)
    if item:
        register_schedule_job(item)
    await callback.message.answer("Добавлено расписание: каждый час ✅")
    await send_schedule_list(callback.message)
    await callback.answer()


@dp.callback_query(F.data == "schedule:type:weekly")
async def schedule_weekly(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Понедельник", callback_data="schedule:weekday:0")],
            [InlineKeyboardButton(text="Вторник", callback_data="schedule:weekday:1")],
            [InlineKeyboardButton(text="Среда", callback_data="schedule:weekday:2")],
            [InlineKeyboardButton(text="Четверг", callback_data="schedule:weekday:3")],
            [InlineKeyboardButton(text="Пятница", callback_data="schedule:weekday:4")],
            [InlineKeyboardButton(text="Суббота", callback_data="schedule:weekday:5")],
            [InlineKeyboardButton(text="Воскресенье", callback_data="schedule:weekday:6")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="schedule:add_time")],
        ]
    )
    await callback.message.answer("Выберите день недели:", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("schedule:weekday:"))
async def schedule_weekday_selected(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return

    weekday = int(callback.data.split(":")[-1])
    await state.set_state(AdminStates.waiting_weekly_time)
    await state.update_data(weekly_day=weekday)
    await callback.message.answer("Введите время для еженедельной публикации в формате HH:MM")
    await callback.answer()


@dp.message(AdminStates.waiting_daily_time)
async def save_daily_schedule(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return

    parsed = parse_time(message.text or "")
    if not parsed:
        await message.answer("Неверный формат. Используйте HH:MM, например 14:00")
        return

    schedule_id = await add_schedule("daily", parsed, None)
    if schedule_id is None:
        await state.clear()
        await message.answer("⚠️ Такое время уже добавлено")
        await send_schedule_list(message)
        return

    item = await get_schedule(schedule_id)
    if item:
        register_schedule_job(item)
    await state.clear()

    await message.answer(f"Запланировано на {parsed} (Europe/Berlin) ✅")
    await send_schedule_list(message)


@dp.message(AdminStates.waiting_weekly_time)
async def save_weekly_schedule(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return

    parsed = parse_time(message.text or "")
    if not parsed:
        await message.answer("Неверный формат. Используйте HH:MM")
        return

    data = await state.get_data()
    weekday = data.get("weekly_day")
    if weekday is None:
        await state.clear()
        await message.answer("Не удалось определить день недели. Начните заново.")
        return

    schedule_id = await add_schedule("weekly", parsed, int(weekday))
    if schedule_id is None:
        await state.clear()
        await message.answer("⚠️ Такое время уже добавлено")
        await send_schedule_list(message)
        return

    item = await get_schedule(schedule_id)
    if item:
        register_schedule_job(item)

    await state.clear()
    await message.answer(f"Запланировано на {parsed} (Europe/Berlin) ✅")
    await send_schedule_list(message)


@dp.callback_query(F.data.startswith("schedule:item:"))
async def schedule_item_menu(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return

    schedule_id = int(callback.data.split(":")[-1])
    item = await get_schedule(schedule_id)
    if not item:
        await callback.message.answer("Расписание не найдено.")
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"schedule:delete:{schedule_id}")],
            [InlineKeyboardButton(text="✏️ Изменить", callback_data=f"schedule:edit:{schedule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="schedule:list")],
        ]
    )
    await callback.message.answer(f"Выбрано: {schedule_label(item)}", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data == "schedule:list")
async def schedule_back_to_list(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await send_schedule_list(callback.message)
    await callback.answer()


@dp.callback_query(F.data.startswith("schedule:delete:"))
async def schedule_delete(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return

    schedule_id = int(callback.data.split(":")[-1])
    await delete_schedule(schedule_id)
    remove_schedule_job(schedule_id)
    await callback.message.answer("Расписание удалено ✅")
    await send_schedule_list(callback.message)
    await callback.answer()


@dp.callback_query(F.data.startswith("schedule:edit:"))
async def schedule_edit(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return

    schedule_id = int(callback.data.split(":")[-1])
    item = await get_schedule(schedule_id)
    if not item:
        await callback.message.answer("Расписание не найдено.")
        await callback.answer()
        return

    if item["schedule_type"] == "hourly":
        await callback.message.answer("Для типа 'Каждый час' изменение времени не требуется.")
        await callback.answer()
        return

    await state.set_state(AdminStates.waiting_edit_time)
    await state.update_data(edit_schedule_id=schedule_id)
    await callback.message.answer("Введите новое время в формате HH:MM")
    await callback.answer()


@dp.message(AdminStates.waiting_edit_time)
async def schedule_edit_save(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return

    parsed = parse_time(message.text or "")
    if not parsed:
        await message.answer("Неверный формат. Используйте HH:MM")
        return

    data = await state.get_data()
    schedule_id = data.get("edit_schedule_id")
    if not schedule_id:
        await state.clear()
        await message.answer("Сессия редактирования устарела, попробуйте снова.")
        return

    try:
        await update_schedule_time(int(schedule_id), parsed)
    except ValueError:
        await state.clear()
        await message.answer("⚠️ Такое время уже добавлено")
        await send_schedule_list(message)
        return

    updated = await get_schedule(int(schedule_id))
    if updated:
        register_schedule_job(updated)

    await state.clear()
    await message.answer(f"Запланировано на {parsed} (Europe/Berlin) ✅")
    await send_schedule_list(message)


@dp.message(SupportStates.waiting_message, F.chat.type == "private")
async def support_message_forward(message: Message, state: FSMContext):
    if ensure_admin(message):
        await state.clear()
        return

    sender = message.from_user
    sender_name = f"@{sender.username}" if sender and sender.username else "без username"

    try:
        if message.photo:
            await bot.send_photo(
                ADMIN_ID,
                photo=message.photo[-1].file_id,
                caption=f"Сообщение поддержки от {sender_name} (id={sender.id})\n{message.caption or ''}",
            )
        else:
            await bot.send_message(
                ADMIN_ID,
                f"Сообщение поддержки от {sender_name} (id={sender.id}):\n{message.text or '[без текста]'}",
            )
        await message.answer("Сообщение отправлено продавцу ✅")
        await state.clear()
    except Exception as exc:
        logger.error("Support forward error: %s", exc)
        await message.answer("Не удалось отправить сообщение продавцу.")


@dp.message(ContactStates.waiting_message, F.chat.type == "private")
async def contact_message_forward(message: Message, state: FSMContext):
    if ensure_admin(message):
        await state.clear()
        return

    data = await state.get_data()
    token = data.get("contact_token")
    if not token:
        await state.clear()
        await message.answer("Ссылка устарела или неверна.")
        return

    sender = message.from_user
    username = f"@{sender.username}" if sender and sender.username else "без username"
    full_name = (sender.full_name if sender else "") or "без имени"
    body = message.text or message.caption or "[без текста]"

    try:
        await bot.send_message(
            ADMIN_ID,
            "\n".join(
                [
                    "📩 Новое сообщение покупателя",
                    f"token: {token}",
                    f"user_id: {sender.id if sender else 'unknown'}",
                    f"username: {username}",
                    f"full_name: {full_name}",
                    "",
                    body,
                ]
            ),
        )
        logger.info("Forwarded buyer message user_id=%s token=%s", sender.id if sender else None, token)
        await _send_buyer_reply(message.chat.id)
        await state.clear()
    except Exception as exc:
        logger.error("Contact forward error user_id=%s token=%s: %s", sender.id if sender else None, token, exc)
        await message.answer("Не удалось отправить сообщение продавцу.")


@dp.message(F.chat.type == "private")
async def save_storage_chat_from_forward(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return

    current_state = await state.get_state()
    if current_state is not None:
        return

    if not message.forward_from_chat:
        return

    await set_storage_chat_id(message.forward_from_chat.id)
    await message.answer("Storage чат сохранён")


async def restore_scheduler() -> None:
    pool = await get_db_pool()
    rows = await pool.fetch(
        """
        SELECT id, schedule_type, time_text, weekday
        FROM schedules
        WHERE enabled = TRUE
        ORDER BY schedule_type, weekday NULLS FIRST, time_text
        """
    )

    for row in rows:
        register_schedule_job(
            {
                "id": row["id"],
                "schedule_type": row["schedule_type"],
                "time_text": row["time_text"],
                "weekday": row["weekday"],
            }
        )


async def main() -> None:
    global BOT_USERNAME, BOT_ID
    try:
        await init_db()
        me = await bot.get_me()
        BOT_USERNAME = me.username or ""
        BOT_ID = me.id
        scheduler.start()
        await restore_scheduler()
        await dp.start_polling(bot)
    finally:
        with suppress(Exception):
            scheduler.shutdown(wait=False)
        if db_pool is not None:
            await db_pool.close()
        with suppress(Exception):
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
