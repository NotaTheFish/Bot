import asyncio
import html
import json
import logging
import os
import random
import re
import uuid
from contextlib import suppress
from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import DefaultKeyBuilder, StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
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


def parse_int_env(name: str, default: int | None = None) -> int | None:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _get_on_off(name: str, default: str = "off") -> bool:
    return _get_env_str(name, default).lower() == "on"


BOT_TOKEN = _get_env_str("BOT_TOKEN")
DATABASE_URL = _get_env_str("DATABASE_URL")
DELIVERY_MODE = _get_env_str("DELIVERY_MODE", "bot").lower() or "bot"
STORAGE_CHAT_ID = parse_int_env("STORAGE_CHAT_ID")
TZ_NAME = _get_env_str("TZ", "Europe/Berlin")
ADMIN_NOTIFICATIONS = _get_env_str("ADMIN_NOTIFICATIONS", "errors").lower() or "errors"
STORAGE_CHAT_META_KEY = "storage_chat_id"
LAST_STORAGE_MESSAGE_ID_META_KEY = "last_storage_message_id"
LAST_STORAGE_MESSAGE_IDS_META_KEY = "last_storage_message_ids"
LAST_STORAGE_CHAT_ID_META_KEY = "last_storage_chat_id"
MEDIA_GROUP_BUFFER_TIMEOUT_SECONDS = max(0.5, float(_get_env_str("MEDIA_GROUP_BUFFER_TIMEOUT_SECONDS", "1.5")))
STORAGE_KEEP_LAST_POSTS = max(1, _get_env_int("STORAGE_KEEP_LAST_POSTS", 1))
STORAGE_CLEANUP_EVERY_SECONDS = max(60, _get_env_int("STORAGE_CLEANUP_EVERY_SECONDS", 600))


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@")


SELLER_USERNAME = _normalize_username(_get_env_str("SELLER_USERNAME", ""))
CONFIGURED_BOT_USERNAME = _normalize_username(_get_env_str("BOT_USERNAME", ""))
SUPPORT_PAYLOAD = "support"
CONTACT_PAYLOAD_PREFIX = "contact_"
CONTACT_CTA_TEXT = "✉️ Написать продавцу"
BUYER_CONTACT_BUTTON_TEXT = "✉️ Связаться с продавцом"
BUYER_CONTACT_COOLDOWN_SECONDS = _get_env_int("BUYER_CONTACT_COOLDOWN_SECONDS", 60)
CONTACT_CTA_MODE = "off"

def _parse_int_list_csv(raw: str) -> list[int]:
    raw = (raw or "").strip()
    if not raw:
        return []
    result: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        result.append(int(part))
    return result


_admin_ids = _parse_int_list_csv(os.getenv("ADMIN_IDS", ""))

if not _admin_ids:
    legacy_admin_id = int(os.getenv("ADMIN_ID", "0") or "0")
    if legacy_admin_id > 0:
        _admin_ids = [legacy_admin_id]

ADMIN_IDS_LIST = _admin_ids
ADMIN_IDS = ADMIN_IDS_LIST
ADMIN_ID = ADMIN_IDS_LIST[0] if ADMIN_IDS_LIST else 0


if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан или имеет неверный формат.")
if not ADMIN_IDS_LIST:
    raise RuntimeError("ADMIN_IDS/ADMIN_ID не задан(ы) или имеют неверное значение.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан. Для Railway используйте PostgreSQL plugin.")
if not STORAGE_CHAT_ID:
    raise RuntimeError("STORAGE_CHAT_ID не задан или имеет неверное значение.")

TZ = ZoneInfo(TZ_NAME)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Loaded %s admin ids: %s", len(ADMIN_IDS_LIST), ADMIN_IDS_LIST)
logger.info("STORAGE_CHAT_ID=%s", STORAGE_CHAT_ID)

class UpdateLoggingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, Message):
            fsm_ctx = data.get("state")
            fsm_state = await fsm_ctx.get_state() if isinstance(fsm_ctx, FSMContext) else None
            logger.info(
                "update message chat_id=%s from_user_id=%s text=%r message_thread_id=%s message_id=%s media_group_id=%s content_type=%s fsm_state=%s",
                event.chat.id,
                event.from_user.id if event.from_user else None,
                event.text,
                event.message_thread_id,
                event.message_id,
                event.media_group_id,
                event.content_type,
                fsm_state,
            )
        return await handler(event, data)


bot = Bot(BOT_TOKEN)


class ThreadAgnosticMemoryStorage(MemoryStorage):
    def __init__(self, *, key_builder: DefaultKeyBuilder | None = None) -> None:
        super().__init__()
        self.key_builder = key_builder or DefaultKeyBuilder()

    @staticmethod
    def _without_thread(key: StorageKey) -> StorageKey:
        if key.thread_id is None:
            return key
        return replace(key, thread_id=None)

    async def set_state(self, key: StorageKey, state: State | str | None = None) -> None:
        await super().set_state(self._without_thread(key), state)

    async def get_state(self, key: StorageKey) -> str | None:
        return await super().get_state(self._without_thread(key))

    async def set_data(self, key: StorageKey, data: dict) -> None:
        await super().set_data(self._without_thread(key), data)

    async def get_data(self, key: StorageKey) -> dict:
        return await super().get_data(self._without_thread(key))

    async def get_value(self, storage_key: StorageKey, dict_key: str, default=None):
        return await super().get_value(self._without_thread(storage_key), dict_key, default)


fsm_key_builder = DefaultKeyBuilder()
storage = ThreadAgnosticMemoryStorage(key_builder=fsm_key_builder)
dp = Dispatcher(storage=storage)
dp.message.middleware(UpdateLoggingMiddleware())
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
QUIET_MODE = _get_env_str("QUIET_MODE", "0").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if DELIVERY_MODE not in {"bot", "userbot"}:
    raise RuntimeError("DELIVERY_MODE должен быть 'bot' или 'userbot'.")


class AdminStates(StatesGroup):
    waiting_daily_time = State()
    waiting_weekly_time = State()
    waiting_edit_time = State()
    waiting_buyer_reply_template = State()
    waiting_autoreply_text = State()
    waiting_autoreply_offline_threshold = State()
    waiting_autoreply_cooldown = State()
    waiting_storage_post = State()


class SupportStates(StatesGroup):
    waiting_message = State()


class ContactStates(StatesGroup):
    waiting_message = State()


def buyer_contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BUYER_CONTACT_BUTTON_TEXT)]],
        resize_keyboard=True,
    )


def is_buyer_contact_text(text: str | None) -> bool:
    normalized = (text or "").strip()
    if not normalized:
        return False
    if normalized == BUYER_CONTACT_BUTTON_TEXT:
        return True
    return normalized.startswith("✉️") and "Связаться" in normalized


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
      error_count INT NOT NULL DEFAULT 0,
      skipped_breakdown JSONB
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
    """
    CREATE TABLE IF NOT EXISTS buyer_contact_cooldown (
        user_id BIGINT PRIMARY KEY,
        next_allowed_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS worker_autoreply_settings (
        id INTEGER PRIMARY KEY DEFAULT 1,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        reply_text TEXT NOT NULL DEFAULT 'Здравствуйте! Я технический аккаунт рассылки. Ответим вам позже.',
        trigger_mode TEXT NOT NULL DEFAULT 'both',
        offline_threshold_minutes INTEGER NOT NULL DEFAULT 60,
        cooldown_seconds INTEGER NOT NULL DEFAULT 3600,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT worker_autoreply_settings_single_row CHECK (id = 1)
    )
    """,
    """
    INSERT INTO worker_autoreply_settings (id)
    VALUES (1)
    ON CONFLICT (id) DO NOTHING
    """,
    """
    CREATE TABLE IF NOT EXISTS worker_autoreply_contacts (
        user_id BIGINT PRIMARY KEY,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_replied_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS worker_state (
        id INTEGER PRIMARY KEY DEFAULT 1,
        last_outgoing_at TIMESTAMPTZ
    )
    """,
    """
    INSERT INTO worker_state (id, last_outgoing_at)
    VALUES (1, NULL)
    ON CONFLICT (id) DO NOTHING
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
    ALTER TABLE userbot_tasks
    ADD COLUMN IF NOT EXISTS skipped_breakdown JSONB
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
      error_count INT NOT NULL DEFAULT 0,
      skipped_breakdown JSONB
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
            [
                KeyboardButton(text="📌 Создать пост (в Storage)"),
                KeyboardButton(text="📊 Статус"),
            ],
            [
                KeyboardButton(text="🤖 Автоответчик"),
                KeyboardButton(text="📝 Изменить автоответ"),
            ],
            [
                KeyboardButton(text="✅ Запустить рассылку"),
                KeyboardButton(text="⛔ Остановить рассылку"),
            ],
        ],
        resize_keyboard=True,
    )


def is_admin_user(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in ADMIN_IDS_LIST)


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
    storage_message_ids = list(row["storage_message_ids"] or [])
    if not storage_message_ids and row["source_message_id"] is not None:
        storage_message_ids = [row["source_message_id"]]
    return {
        "source_chat_id": row["source_chat_id"],
        "source_message_id": row["source_message_id"],
        "storage_chat_id": row["storage_chat_id"],
        "source_message_ids": storage_message_ids,
        "storage_message_ids": storage_message_ids,
        "has_media": bool(row["has_media"]),
        "media_group_id": row["media_group_id"],
        "contact_token": row["contact_token"],
    }


async def save_post(
    storage_chat_id: int,
    storage_message_ids: list[int],
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
        storage_chat_id,
        (storage_message_ids[0] if storage_message_ids else None),
        storage_message_ids,
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


async def get_cooldown_remaining(user_id: int) -> int:
    pool = await get_db_pool()
    row = await pool.fetchrow(
        """
        SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM (next_allowed_at - NOW())))::INT) AS remaining
        FROM buyer_contact_cooldown
        WHERE user_id = $1
        """,
        user_id,
    )
    if not row:
        return 0
    return int(row["remaining"] or 0)


async def set_next_allowed(user_id: int, cooldown_seconds: int) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        INSERT INTO buyer_contact_cooldown(user_id, next_allowed_at)
        VALUES ($1, NOW() + ($2::INT * INTERVAL '1 second'))
        ON CONFLICT(user_id)
        DO UPDATE SET next_allowed_at = excluded.next_allowed_at
        """,
        user_id,
        max(0, cooldown_seconds),
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


def format_admin_datetime(value: Optional[str | datetime]) -> str:
    if value is None:
        return "-"

    parsed: Optional[datetime]
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = parse_iso_datetime(value)

    if parsed is None:
        return "-"

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") + f" ({TZ.key})"


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


def _contains_contact_cta(text: Optional[str], *, bot_username: str = "") -> bool:
    if not text:
        return False
    lowered = text.lower()
    if CONTACT_CTA_TEXT.casefold() in text.casefold():
        return True
    if "start=contact_" in lowered:
        return True
    normalized_bot_username = _normalize_username(bot_username)
    if normalized_bot_username and f"@{normalized_bot_username}" in lowered and "продав" in lowered:
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


def build_contact_cta(
    content_text: str,
    original_entities: Optional[list[MessageEntity]],
    token: str,
    *,
    bot_username: str,
    seller_username: str,
    mode: str = "mention",
    force_cta_only: bool = False,
    include_start_hint: bool = False,
) -> tuple[str, list[MessageEntity], bool]:
    _ = (token, bot_username, seller_username, mode, include_start_hint)
    base_text = "" if force_cta_only else (content_text or "")
    base_entities: list[MessageEntity] = [] if force_cta_only else list(original_entities or [])
    return base_text, base_entities, False


def _merge_entities(original_entities: Optional[list[MessageEntity]], extra_entities: list[MessageEntity]) -> list[MessageEntity]:
    merged: list[MessageEntity] = []
    if original_entities:
        merged.extend(original_entities)
    merged.extend(extra_entities)
    return merged


_BANNED_URL_SUBSTRINGS = ("http://", "https://", "t.me/")
_BANNED_URL_PATTERN = re.compile(r"https?://\S+|t\.me/\S+", re.IGNORECASE)


def _has_banned_url(text: str) -> bool:
    lowered = (text or "").lower()
    return any(item in lowered for item in _BANNED_URL_SUBSTRINGS)


def _sanitize_text_without_urls(text: str) -> str:
    cleaned = _BANNED_URL_PATTERN.sub("", text or "")
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _drop_text_link_entities(entities: Optional[list[MessageEntity]]) -> list[MessageEntity]:
    return [entity for entity in (entities or []) if entity.type != "text_link"]


def _ensure_safe_publish_text(
    text: str,
    entities: Optional[list[MessageEntity]],
    *,
    fallback_text: str,
    fallback_entities: Optional[list[MessageEntity]],
    context: str,
) -> tuple[str, list[MessageEntity]]:
    if not _has_banned_url(text):
        return text, list(entities or [])

    safe_text = _sanitize_text_without_urls(fallback_text)
    if not safe_text:
        safe_text = "✉️ Для связи напишите администратору"
    safe_entities = _drop_text_link_entities(fallback_entities)
    logger.warning("Unsafe URL detected in %s; switched to fallback text without URL/text_link.", context)
    return safe_text, safe_entities


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

    raw_text = source_message.text or ""
    raw_caption = source_message.caption or ""
    has_media = bool(source_message.media_group_id or source_message.photo or source_message.video or source_message.document or source_message.animation)
    content_text = raw_caption if has_media else raw_text
    original_entities = list(source_message.caption_entities or []) if has_media else list(source_message.entities or [])

    if not force_cta_only and _contains_contact_cta(content_text, bot_username=bot_username):
        copied = await bot.copy_message(
            chat_id=storage_chat_id,
            from_chat_id=source_message.chat.id,
            message_id=source_message.message_id,
        )
        return copied.message_id

    cta_mode = "deep_link" if CONTACT_CTA_MODE == "deep_link" and bot_username else "mention"
    updated, entities, _ = build_contact_cta(
        content_text,
        original_entities,
        token,
        bot_username=bot_username,
        seller_username=SELLER_USERNAME,
        mode=cta_mode,
        force_cta_only=force_cta_only,
    )
    fallback_text, fallback_entities, _ = build_contact_cta(
        content_text,
        original_entities,
        token,
        bot_username=bot_username,
        seller_username=SELLER_USERNAME,
        mode="mention",
        force_cta_only=force_cta_only,
    )
    updated, entities = _ensure_safe_publish_text(
        updated,
        entities,
        fallback_text=fallback_text,
        fallback_entities=fallback_entities,
        context="single-message publish",
    )

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
    storage_chat_id = post["storage_chat_id"] or post["source_chat_id"]
    storage_message_ids = post["storage_message_ids"]

    if not storage_chat_id or not storage_message_ids:
        await message.answer("Текущий пост:\n\n(пусто)")
        return

    for message_id in storage_message_ids:
        await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=storage_chat_id,
            message_id=message_id,
        )




async def _delete_previous_storage_post(
    storage_chat_id: int,
    previous_storage_chat_id: Optional[int],
    previous_storage_message_ids: list[int],
) -> list[str]:
    warnings: list[str] = []
    if not previous_storage_message_ids:
        return warnings

    delete_chat_id = previous_storage_chat_id or storage_chat_id
    pool = await get_db_pool()
    active_rows = await pool.fetch(
        """
        SELECT storage_message_ids
        FROM userbot_tasks
        WHERE status IN ('pending', 'processing')
          AND storage_chat_id = $1
        """,
        delete_chat_id,
    )
    active_ids: set[int] = set()
    for row in active_rows:
        for msg_id in row["storage_message_ids"] or []:
            active_ids.add(int(msg_id))

    for previous_storage_message_id in previous_storage_message_ids:
        msg_id = int(previous_storage_message_id)

        if msg_id in active_ids:
            warnings.append("⚠️ Старый пост не удалён: используется активной рассылкой")
            logger.info(
                "Skipping previous storage post deletion because task is active chat_id=%s message_id=%s",
                delete_chat_id,
                msg_id,
            )
            continue

        logger.info(
            "Trying to delete previous storage post chat_id=%s message_id=%s",
            delete_chat_id,
            msg_id,
        )
        try:
            await bot.delete_message(delete_chat_id, msg_id)
        except (TelegramBadRequest, TelegramForbiddenError) as exc:
            warnings.append(f"⚠️ Старый пост не удалён: {exc}")
            logger.warning(
                "Skipping previous storage post deletion chat_id=%s message_id=%s: %s",
                delete_chat_id,
                msg_id,
                exc,
            )
    return warnings


async def _pin_storage_post(storage_chat_id: int, old_message_ids: list[int], new_message_ids: list[int]) -> Optional[str]:
    old_first_id = int(old_message_ids[0]) if old_message_ids else None
    new_first_id = int(new_message_ids[0]) if new_message_ids else None
    if not new_first_id:
        return None

    if old_first_id:
        with suppress(TelegramBadRequest, TelegramForbiddenError):
            await bot.unpin_chat_message(storage_chat_id, old_first_id)

    try:
        await bot.pin_chat_message(storage_chat_id, new_first_id, disable_notification=True)
    except TelegramForbiddenError:
        return "⚠️ Не смог закрепить: нет прав"
    except TelegramBadRequest as exc:
        logger.warning("Pin storage post failed chat_id=%s message_id=%s: %s", storage_chat_id, new_first_id, exc)
        return "⚠️ Не смог закрепить: нет прав"
    return None


async def _publish_post_messages(messages: list[Message], state: FSMContext, media_group_id: Optional[str]) -> None:
    _ = media_group_id
    if not messages:
        return

    storage_chat_id = await get_storage_chat_id()
    if storage_chat_id == 0:
        await messages[-1].answer("Ошибка: STORAGE_CHAT_ID не настроен.")
        return

    sorted_messages = sorted(messages, key=lambda msg: msg.message_id)
    storage_message_ids = [int(msg.message_id) for msg in sorted_messages]

    first_message = sorted_messages[0]
    has_media = bool(first_message.photo or first_message.video or first_message.animation or first_message.document)
    previous_post = await get_post()
    previous_storage_chat_id = previous_post.get("storage_chat_id") or previous_post.get("source_chat_id")
    previous_storage_message_ids = [int(i) for i in (previous_post.get("storage_message_ids") or previous_post.get("source_message_ids") or [])]

    try:
        deletion_warnings = await _delete_previous_storage_post(
            storage_chat_id=storage_chat_id,
            previous_storage_chat_id=int(previous_storage_chat_id) if previous_storage_chat_id else None,
            previous_storage_message_ids=previous_storage_message_ids,
        )
        await set_last_storage_messages(storage_chat_id, storage_message_ids)
        await save_post(
            storage_chat_id=storage_chat_id,
            storage_message_ids=storage_message_ids,
            has_media=has_media,
            media_group_id=str(first_message.media_group_id) if first_message.media_group_id else None,
            contact_token=None,
        )
        logger.info(
            "saved storage post chat_id=%s ids=%s media_group_id=%s",
            storage_chat_id,
            storage_message_ids,
            media_group_id,
        )
        pin_warning = await _pin_storage_post(storage_chat_id, previous_storage_message_ids, storage_message_ids)
        logger.info("pinned storage post chat_id=%s first_message_id=%s", storage_chat_id, storage_message_ids[0])
    except Exception as exc:
        logger.exception("Failed to persist storage metadata")
        await messages[-1].answer(f"❌ Не удалось сохранить пост в БД: {exc}")
        return

    await state.clear()
    state_after = await state.get_state()
    logger.info(
        "storage_post state cleared chat_id=%s user_id=%s media_group_id=%s state_after=%s",
        messages[-1].chat.id,
        messages[-1].from_user.id if messages[-1].from_user else None,
        media_group_id,
        state_after,
    )
    response_lines = ["✅ Сохранено и закреплено"]
    if pin_warning:
        response_lines.append(pin_warning)
    if deletion_warnings:
        response_lines.extend(dict.fromkeys(deletion_warnings))
    await messages[-1].answer("\n".join(response_lines))
    await send_post_actions(messages[-1])



async def _flush_media_group(media_group_id: str, state: FSMContext) -> None:
    buffer_data = media_group_buffers.pop(media_group_id, None)
    if not buffer_data:
        logger.info("storage_post flush skipped media_group_id=%s (buffer missing)", media_group_id)
        return
    messages = buffer_data.get("messages", [])
    logger.info("storage_post flush media_group_id=%s messages=%s", media_group_id, len(messages))
    await _publish_post_messages(messages, state, media_group_id=media_group_id)

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




async def cancel_active_userbot_tasks() -> int:
    pool = await get_db_pool()
    await ensure_userbot_tasks_schema(pool)
    result = await pool.execute(
        """
        UPDATE userbot_tasks
        SET status = 'canceled',
            last_error = COALESCE(last_error, 'canceled_by_admin')
        WHERE status IN ('pending', 'processing')
        """
    )
    try:
        return int(str(result).split()[-1])
    except Exception:
        return 0


async def safe_cleanup_storage_posts() -> None:
    post = await get_post()
    storage_chat_id = int(post.get("storage_chat_id") or post.get("source_chat_id") or 0)
    latest_ids = [int(x) for x in (post.get("storage_message_ids") or post.get("source_message_ids") or [])]
    if storage_chat_id == 0:
        return

    pool = await get_db_pool()
    active_rows = await pool.fetch(
        """
        SELECT storage_message_ids
        FROM userbot_tasks
        WHERE status IN ('pending', 'processing')
        """
    )
    active_ids: set[int] = set()
    for row in active_rows:
        for msg_id in row["storage_message_ids"] or []:
            active_ids.add(int(msg_id))

    rows = await pool.fetch(
        """
        SELECT DISTINCT unnest(storage_message_ids) AS message_id
        FROM userbot_tasks
        WHERE storage_chat_id = $1
        """,
        storage_chat_id,
    )
    candidate_ids = sorted({int(r["message_id"]) for r in rows if r["message_id"] is not None})
    if latest_ids:
        keep_ids = set(sorted(latest_ids)[-STORAGE_KEEP_LAST_POSTS:])
    else:
        keep_ids = set()

    for message_id in candidate_ids:
        if message_id in active_ids or message_id in keep_ids:
            continue
        try:
            await bot.delete_message(storage_chat_id, message_id)
        except TelegramBadRequest as exc:
            logger.warning("Safe cleanup skipped storage message deletion chat_id=%s message_id=%s: %s", storage_chat_id, message_id, exc)

async def send_admin_report(report: dict) -> None:
    if report.get("errors", 0) <= 0:
        return
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
    await notify_admin("\n".join(lines), level="error")


def should_send_admin_report(report: dict) -> bool:
    return report.get("errors", 0) > 0


async def notify_admin(text: str, level: str = "info"):
    if ADMIN_NOTIFICATIONS == "off":
        return
    if ADMIN_NOTIFICATIONS == "errors" and level != "error":
        return
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        logger.exception("Failed to notify admin")


async def is_manual_confirmation_required(meta: dict) -> bool:
    if not MANUAL_CONFIRM_FIRST_BROADCAST:
        return False
    if meta['manual_confirmed_day'] == meta['day_key']:
        return False
    return True


async def broadcast_once() -> None:
    post = await get_post()
    source_chat_id = post["storage_chat_id"] or post["source_chat_id"]
    source_message_ids = post["storage_message_ids"] or post["source_message_ids"]

    if not source_chat_id or not source_message_ids:
        logger.info("Broadcast skipped: empty post")
        return

    meta = await get_broadcast_meta()
    if await is_manual_confirmation_required(meta):
        logger.info("Broadcast skipped: waiting manual daily confirmation")
        return

    if is_quiet_hours_now():
        logger.info("Broadcast skipped: quiet hours window")
        return

    if meta['daily_broadcast_count'] >= BROADCAST_MAX_PER_DAY > 0:
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
        await cancel_active_userbot_tasks()
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
            if QUIET_MODE:
                logger.info("Userbot task queued (quiet mode).")
            else:
                await notify_admin("🧾 Userbot-рассылка поставлена в очередь.", level="info")
        else:
            logger.info("Userbot task is already pending/running for this payload")
            if QUIET_MODE:
                logger.info("Duplicate userbot task ignored (quiet mode).")
            else:
                await notify_admin("ℹ️ Такая userbot-задача уже в очереди.", level="info")
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


@dp.message(F.chat.type.in_({"group", "supergroup"}) & ~F.text.regexp(r"^/"))
async def remember_chat_from_messages(message: Message, state: FSMContext):
    if await state.get_state() == AdminStates.waiting_storage_post.state:
        return

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
        at = format_admin_datetime(item["created_at"])
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
        f"Последняя рассылка ({TZ.key}): {format_admin_datetime(meta['last_broadcast_at'])}",
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


@dp.message(F.text.in_({"✅ Запустить рассылку", "🚀 Запустить сейчас"}))
async def admin_broadcast_now(message: Message) -> None:
    if not ensure_admin(message):
        return

    if broadcast_now_lock.locked():
        await message.answer("⏳ Расссылка уже запускается, подождите...")
        return

    async with broadcast_now_lock:
        if message.chat.type == "private":
            await message.answer("Ок, запустил")
        day_key = datetime.now(TZ).strftime("%Y-%m-%d")
        await set_meta("manual_confirmed_day", day_key)
        logger.info("Manual broadcast triggered by admin_id=%s", message.from_user.id if message.from_user else None)

        try:
            await broadcast_once()
        except Exception as exc:
            logger.exception("Manual broadcast failed")
            await message.answer(f"❌ Ошибка запуска: {exc}")
            return

        logger.info("Manual broadcast finished")



@dp.callback_query(F.data == "broadcast:confirm_today")
async def confirm_daily_broadcast(callback: CallbackQuery):
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return

    day_key = datetime.now(TZ).strftime("%Y-%m-%d")
    await set_meta("manual_confirmed_day", day_key)
    await callback.answer("Подтверждено")

    if callback.message:
        with suppress(Exception):
            await callback.message.edit_text("Подтверждение получено. Запускаю рассылку ✅")
    await broadcast_once()


async def _start_support_flow(message: Message, state: FSMContext, concise: bool = False) -> None:
    await state.set_state(SupportStates.waiting_message)
    if concise:
        await message.answer("Напишите сообщение одним сообщением — я перешлю продавцу.")
        return
    if SELLER_USERNAME:
        await message.answer(
            f"Напишите сообщение здесь, я тут же передам его продавцу или свяжитесь с ним лично @{SELLER_USERNAME}"
        )
    else:
        await message.answer("Напишите сообщение одним сообщением — я перешлю продавцу.")


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
        await _start_support_flow(message, state, concise=True)
        return

    if message.chat.type == "private" and message.from_user and is_admin_user(message.from_user.id):
        await state.clear()
        await message.answer("Главное меню:", reply_markup=admin_menu_keyboard())
    elif message.chat.type == "private":
        await message.answer("Здравствуйте!")
    else:
        await message.answer("Здравствуйте!")


@dp.message(Command("menu"))
async def on_menu(message: Message, state: FSMContext):
    if message.chat.type == "private" and message.from_user and is_admin_user(message.from_user.id):
        await state.clear()
        await message.answer("Главное меню:", reply_markup=admin_menu_keyboard())


@dp.message(F.text.in_({"/", "Меню", "меню"}))
async def on_menu_fallback(message: Message, state: FSMContext):
    if message.chat.type == "private" and message.from_user and is_admin_user(message.from_user.id):
        await state.clear()
        await message.answer("Главное меню:", reply_markup=admin_menu_keyboard())


@dp.message(F.chat.type == "private", lambda message: is_buyer_contact_text(message.text))
async def buyer_contact_start(message: Message, state: FSMContext):
    if ensure_admin(message):
        return
    if not message.from_user:
        await message.answer("Не удалось определить пользователя.")
        return

    current_state = await state.get_state()
    if current_state == ContactStates.waiting_message.state:
        await message.answer("Вы уже в режиме отправки. Просто напишите сообщение.")
        return

    remaining = await get_cooldown_remaining(message.from_user.id)
    if remaining > 0:
        await message.answer(f"⏳ Подождите {remaining} сек. и попробуйте снова.")
        return

    await set_next_allowed(message.from_user.id, BUYER_CONTACT_COOLDOWN_SECONDS)
    await _start_support_flow(message, state, concise=True)




@dp.message(F.text == "⛔ Остановить рассылку")
async def stop_userbot_broadcast(message: Message) -> None:
    if not ensure_admin(message):
        return
    if DELIVERY_MODE != "userbot":
        await message.answer("Команда доступна только в режиме userbot.")
        return

    canceled = await cancel_active_userbot_tasks()
    await message.answer(f"Остановлено активных задач: {canceled} ✅")

@dp.message(F.text == "📌 Создать пост (в Storage)")
async def storage_post_instructions(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    await state.clear()
    await message.answer(
        "Перейдите в чат Storage и отправьте команду /create_post. "
        "Затем перешлите туда ваш рекламный пост (или альбом) от вашего аккаунта/жены."
    )


@dp.message(F.text == "✏️ Изменить пост")
async def edit_post_start(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    await message.answer("Используйте кнопку: 📌 Создать пост (в Storage)")


@dp.message(F.text.in_({"/settings", "/admin", "📝 Изменить автоответ", "📝 Изменить автоответ покупателю"}))
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
    await state.clear()
    await callback.message.answer("Используйте кнопку: 📌 Создать пост (в Storage)")
    await callback.answer()


@dp.message(Command(commands=["create_post", "post"]))
@dp.message(F.text.regexp(r"^/create_post(?:@\w+)?(?:\s|$)"))
async def create_post_in_storage(message: Message, state: FSMContext):
    logger.info(
        "HANDLER create_post fired chat_id=%s message_thread_id=%s text=%r",
        message.chat.id,
        message.message_thread_id,
        message.text,
    )
    try:
        if not ensure_admin(message):
            await message.answer("Недостаточно прав")
            return
        if int(message.chat.id) != int(STORAGE_CHAT_ID):
            await message.answer("используйте команду в Storage")
            return

        await state.set_state(AdminStates.waiting_storage_post)
        logger.info(
            "STATE set to waiting_storage_post for chat_id=%s user_id=%s current=%s",
            message.chat.id,
            message.from_user.id if message.from_user else None,
            await state.get_state(),
        )
        await message.answer("Пришлите пост одним сообщением или альбомом. /cancel чтобы отменить.")
    except Exception as e:
        logger.exception("create_post failed: %s", e)
        await message.answer(f"Ошибка: {e}")


def _is_valid_storage_post_message(message: Message) -> tuple[bool, str | None]:
    if (
        message.text
        or message.caption
        or message.photo
        or message.video
        or message.document
        or message.animation
        or message.audio
        or message.voice
        or message.sticker
    ):
        return True, None
    return False, "Этот тип сообщения не поддерживается, пришлите текст/медиа/альбом."


@dp.message(AdminStates.waiting_storage_post)
async def handle_storage_post(message: Message, state: FSMContext):
    if message.chat.id != STORAGE_CHAT_ID:
        return
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        return

    state_before = await state.get_state()
    logger.info(
        "storage_post handler enter chat_id=%s user_id=%s thread_id=%s state_before=%s",
        message.chat.id,
        message.from_user.id,
        message.message_thread_id,
        state_before,
    )

    logger.info(
        "HANDLER storage_post fired chat_id=%s user_id=%s text=%r caption=%r content_type=%s has_photo=%s has_video=%s has_doc=%s media_group_id=%s msg_id=%s",
        message.chat.id,
        message.from_user.id,
        message.text,
        message.caption,
        message.content_type,
        bool(message.photo),
        bool(message.video),
        bool(message.document),
        message.media_group_id,
        message.message_id,
    )

    is_valid, validation_error = _is_valid_storage_post_message(message)
    if not is_valid:
        logger.info(
            "storage_post invalid content_type=%s chat_id=%s user_id=%s msg_id=%s",
            message.content_type,
            message.chat.id,
            message.from_user.id,
            message.message_id,
        )
        await message.answer(validation_error or "Этот тип сообщения не поддерживается, пришлите текст/медиа/альбом.")
        return

    if message.media_group_id:
        logger.info(
            "storage_post branch=album chat_id=%s user_id=%s thread_id=%s media_group_id=%s message_id=%s",
            message.chat.id,
            message.from_user.id,
            message.message_thread_id,
            message.media_group_id,
            message.message_id,
        )
        buffer_data = media_group_buffers.get(message.media_group_id)
        if not buffer_data:
            task = asyncio.create_task(_flush_media_group_with_delay(message.media_group_id, state))
            buffer_data = {"messages": [], "task": task}
            media_group_buffers[message.media_group_id] = buffer_data
            logger.info("storage_post album created buffer media_group_id=%s", message.media_group_id)
        elif buffer_data.get("task") is None:
            buffer_data["task"] = asyncio.create_task(_flush_media_group_with_delay(message.media_group_id, state))
            logger.info("storage_post album restored flush task media_group_id=%s", message.media_group_id)
        buffer_data["messages"].append(message)
        return

    logger.info(
        "storage_post branch=single chat_id=%s user_id=%s thread_id=%s message_id=%s",
        message.chat.id,
        message.from_user.id,
        message.message_thread_id,
        message.message_id,
    )
    await _publish_post_messages([message], state, media_group_id=None)


@dp.callback_query(F.data == "edit:back")
async def edit_back_selected(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await state.clear()
    await callback.message.answer("Главное меню:", reply_markup=admin_menu_keyboard())
    await callback.answer()


async def _flush_media_group_with_delay(media_group_id: str, state: FSMContext) -> None:
    try:
        await asyncio.sleep(MEDIA_GROUP_BUFFER_TIMEOUT_SECONDS)
        logger.info("storage_post flush delayed media_group_id=%s", media_group_id)
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


async def _notify_admin_about_buyer_message(message: Message, token: str) -> None:
    sender = message.from_user
    sender_id = sender.id if sender else None
    username = f"@{sender.username}" if sender and sender.username else "отсутствует"
    full_name = (sender.full_name if sender else "") or "без имени"
    safe_name = html.escape(full_name)
    user_link = (
        f'<a href="tg://user?id={sender_id}">{safe_name}</a>' if sender_id else safe_name
    )
    body = message.text or message.caption or "[без текста]"
    contact_markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✉️ Написать",
                    url=f"tg://user?id={sender_id}",
                )
            ]
        ]
    ) if sender_id else None

    try:
        await bot.forward_message(ADMIN_ID, message.chat.id, message.message_id)
    except TelegramBadRequest as exc:
        description = (exc.message or "").lower()
        if "forward" in description and ("restricted" in description or "can't" in description):
            await bot.copy_message(
                chat_id=ADMIN_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        else:
            raise

    await bot.send_message(
        ADMIN_ID,
        "\n".join(
            [
                "📩 Новое сообщение покупателя",
                f"token: {token}",
                f"Имя: {user_link}",
                f"ID: {sender_id if sender_id is not None else 'unknown'}",
                f"Username: {username}",
                "",
                body,
            ]
        ),
        parse_mode="HTML",
        reply_markup=contact_markup,
    )


@dp.message(SupportStates.waiting_message, F.chat.type == "private")
async def support_message_forward(message: Message, state: FSMContext):
    if ensure_admin(message):
        await state.clear()
        return

    try:
        await _notify_admin_about_buyer_message(message, SUPPORT_PAYLOAD)
        await _send_buyer_reply(message.chat.id)
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

    try:
        await _notify_admin_about_buyer_message(message, token)
        logger.info("Forwarded buyer message user_id=%s token=%s", sender.id if sender else None, token)
        await _send_buyer_reply(message.chat.id)
        await state.clear()
    except Exception as exc:
        logger.error("Contact forward error user_id=%s token=%s: %s", sender.id if sender else None, token, exc)
        await message.answer("Не удалось отправить сообщение продавцу.")


@dp.message(Command("current_post"))
async def current_post_in_storage(message: Message):
    if int(message.chat.id) != int(STORAGE_CHAT_ID):
        return
    if not ensure_admin(message):
        return
    await send_post_preview(message)


@dp.message(F.chat.type == "private")
async def private_message_fallback(message: Message, state: FSMContext):
    if ensure_admin(message):
        current_state = await state.get_state()
        if current_state is not None:
            return

        return

    current_state = await state.get_state()
    if current_state in {
        SupportStates.waiting_message.state,
        ContactStates.waiting_message.state,
    }:
        return

    await message.answer("Здравствуйте!")


VALID_AUTOREPLY_MODES = {"first_message_only", "offline_over_minutes", "both"}


async def get_worker_autoreply_settings() -> dict:
    pool = await get_db_pool()
    row = await pool.fetchrow(
        """
        SELECT enabled, reply_text, trigger_mode, offline_threshold_minutes, cooldown_seconds
        FROM worker_autoreply_settings
        WHERE id = 1
        """
    )
    if not row:
        return {
            "enabled": True,
            "reply_text": "Здравствуйте! Я технический аккаунт рассылки. Ответим вам позже.",
            "trigger_mode": "both",
            "offline_threshold_minutes": 60,
            "cooldown_seconds": 3600,
        }
    mode = str(row["trigger_mode"] or "both")
    if mode not in VALID_AUTOREPLY_MODES:
        mode = "both"
    return {
        "enabled": bool(row["enabled"]),
        "reply_text": str(row["reply_text"] or "Здравствуйте! Я технический аккаунт рассылки. Ответим вам позже."),
        "trigger_mode": mode,
        "offline_threshold_minutes": max(0, int(row["offline_threshold_minutes"] or 60)),
        "cooldown_seconds": max(0, int(row["cooldown_seconds"] or 3600)),
    }


async def update_worker_autoreply_settings(**kwargs) -> None:
    allowed = {"enabled", "reply_text", "trigger_mode", "offline_threshold_minutes", "cooldown_seconds"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return
    parts = []
    values = []
    idx = 1
    for key, value in updates.items():
        parts.append(f"{key} = ${idx}")
        values.append(value)
        idx += 1
    parts.append("updated_at = NOW()")
    pool = await get_db_pool()
    await pool.execute(
        f"UPDATE worker_autoreply_settings SET {', '.join(parts)} WHERE id = 1",
        *values,
    )


def _autoreply_settings_text(settings: dict) -> str:
    return "\n".join([
        "🤖 Настройки автоответчика worker",
        f"Статус: {'включен' if settings['enabled'] else 'выключен'}",
        f"Режим: {settings['trigger_mode']}",
        f"offline_threshold_minutes: {settings['offline_threshold_minutes']}",
        f"cooldown_seconds: {settings['cooldown_seconds']}",
        "",
        f"Текст:\n{settings['reply_text']}",
    ])


def worker_autoreply_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Включить", callback_data="wa:enable"), InlineKeyboardButton(text="⛔ Выключить", callback_data="wa:disable")],
        [InlineKeyboardButton(text="📝 Текст", callback_data="wa:text")],
        [InlineKeyboardButton(text="🎯 Режим: first_message_only", callback_data="wa:mode:first_message_only")],
        [InlineKeyboardButton(text="🎯 Режим: offline_over_minutes", callback_data="wa:mode:offline_over_minutes")],
        [InlineKeyboardButton(text="🎯 Режим: both", callback_data="wa:mode:both")],
        [InlineKeyboardButton(text="⏱ Offline threshold", callback_data="wa:offline")],
        [InlineKeyboardButton(text="⏳ Cooldown", callback_data="wa:cooldown")],
    ])


@dp.message(F.text == "🤖 Автоответчик")
async def worker_autoreply_menu(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    await state.clear()
    settings = await get_worker_autoreply_settings()
    await message.answer(_autoreply_settings_text(settings), reply_markup=worker_autoreply_keyboard())


@dp.callback_query(F.data.in_({"wa:enable", "wa:disable"}))
async def worker_autoreply_toggle(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await update_worker_autoreply_settings(enabled=callback.data == "wa:enable")
    settings = await get_worker_autoreply_settings()
    await callback.message.answer(_autoreply_settings_text(settings), reply_markup=worker_autoreply_keyboard())
    await callback.answer("Сохранено")


@dp.callback_query(F.data.startswith("wa:mode:"))
async def worker_autoreply_mode(callback: CallbackQuery):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    mode = callback.data.split(":", 2)[-1]
    if mode not in VALID_AUTOREPLY_MODES:
        await callback.answer("Некорректный режим", show_alert=True)
        return
    await update_worker_autoreply_settings(trigger_mode=mode)
    settings = await get_worker_autoreply_settings()
    await callback.message.answer(_autoreply_settings_text(settings), reply_markup=worker_autoreply_keyboard())
    await callback.answer("Сохранено")


@dp.callback_query(F.data == "wa:text")
async def worker_autoreply_text_start(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_autoreply_text)
    await callback.message.answer("Отправьте новый текст автоответчика.")
    await callback.answer()


@dp.callback_query(F.data == "wa:offline")
async def worker_autoreply_offline_start(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_autoreply_offline_threshold)
    await callback.message.answer("Введите offline_threshold_minutes (целое число).")
    await callback.answer()


@dp.callback_query(F.data == "wa:cooldown")
async def worker_autoreply_cooldown_start(callback: CallbackQuery, state: FSMContext):
    if not callback.message or not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_autoreply_cooldown)
    await callback.message.answer("Введите cooldown_seconds (целое число).")
    await callback.answer()


@dp.message(AdminStates.waiting_autoreply_text)
async def worker_autoreply_text_save(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    text = (message.text or message.caption or "").strip()
    if not text:
        await message.answer("Текст не может быть пустым.")
        return
    await update_worker_autoreply_settings(reply_text=text)
    await state.clear()
    settings = await get_worker_autoreply_settings()
    await message.answer(_autoreply_settings_text(settings), reply_markup=worker_autoreply_keyboard())


@dp.message(AdminStates.waiting_autoreply_offline_threshold)
async def worker_autoreply_offline_save(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    try:
        value = max(0, int((message.text or "").strip()))
    except ValueError:
        await message.answer("Нужно целое число.")
        return
    await update_worker_autoreply_settings(offline_threshold_minutes=value)
    await state.clear()
    settings = await get_worker_autoreply_settings()
    await message.answer(_autoreply_settings_text(settings), reply_markup=worker_autoreply_keyboard())


@dp.message(AdminStates.waiting_autoreply_cooldown)
async def worker_autoreply_cooldown_save(message: Message, state: FSMContext):
    if not ensure_admin(message):
        return
    try:
        value = max(0, int((message.text or "").strip()))
    except ValueError:
        await message.answer("Нужно целое число.")
        return
    await update_worker_autoreply_settings(cooldown_seconds=value)
    await state.clear()
    settings = await get_worker_autoreply_settings()
    await message.answer(_autoreply_settings_text(settings), reply_markup=worker_autoreply_keyboard())


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
        await bot.set_my_commands([], scope=BotCommandScopeDefault())
        await bot.set_my_commands(
            [BotCommand(command="create_post", description="Создать/обновить пост в Storage")],
            scope=BotCommandScopeChat(chat_id=STORAGE_CHAT_ID),
        )
        logger.info("Bot commands set for storage scope chat_id=%s", STORAGE_CHAT_ID)
        scheduler.add_job(safe_cleanup_storage_posts, "interval", seconds=STORAGE_CLEANUP_EVERY_SECONDS, id="storage_safe_cleanup", replace_existing=True)
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
