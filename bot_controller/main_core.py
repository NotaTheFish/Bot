import asyncio
import logging
import os
import random
from contextlib import suppress
from datetime import datetime
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
TZ_NAME = _get_env_str("TZ", "Europe/Berlin")
DELIVERY_MODE = _get_env_str("DELIVERY_MODE", "bot").lower() or "bot"
STORAGE_CHAT_ID = _get_env_int("STORAGE_CHAT_ID", 0)
STORAGE_CHAT_META_KEY = "storage_chat_id"
LAST_STORAGE_MESSAGE_ID_META_KEY = "last_storage_message_id"
LAST_STORAGE_CHAT_ID_META_KEY = "last_storage_chat_id"


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@")


SELLER_USERNAME = _normalize_username(_get_env_str("SELLER_USERNAME", ""))
SUPPORT_PAYLOAD = "support"
SUPPORT_BUTTON_TEXT = "✉️ Написать продавцу"
SEND_SUPPORT_MESSAGE_WITH_BROADCAST = _get_env_str("SEND_SUPPORT_MESSAGE_WITH_BROADCAST", "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан или имеет неверный формат.")
if ADMIN_ID <= 0:
    raise RuntimeError("ADMIN_ID не задан или имеет неверное значение.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан. Для Railway используйте PostgreSQL plugin.")

try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ = ZoneInfo("Europe/Berlin")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)
BOT_USERNAME = ""
BOT_ID = 0
db_pool: Optional[asyncpg.Pool] = None

GLOBAL_BROADCAST_COOLDOWN_SECONDS = _get_env_int("GLOBAL_BROADCAST_COOLDOWN_SECONDS", 600)
MIN_USER_MESSAGES_BETWEEN_POSTS = _get_env_int("MIN_USER_MESSAGES_BETWEEN_POSTS", 5)
BROADCAST_MAX_PER_DAY = _get_env_int("BROADCAST_MAX_PER_DAY", 20)
MIN_SECONDS_BETWEEN_CHATS = max(0.0, float(_get_env_str("MIN_SECONDS_BETWEEN_CHATS", "0.3")))
MAX_SECONDS_BETWEEN_CHATS = max(MIN_SECONDS_BETWEEN_CHATS, float(_get_env_str("MAX_SECONDS_BETWEEN_CHATS", "0.7")))
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


class SupportStates(StatesGroup):
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
        has_media INTEGER NOT NULL DEFAULT 0,
        media_group_id TEXT,
        CONSTRAINT post_single_row CHECK (id = 1)
    )
    """,
    """
    INSERT INTO post (id, source_chat_id, source_message_id, has_media, media_group_id)
    VALUES (1, NULL, NULL, 0, NULL)
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
      run_at TIMESTAMP NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      last_error TEXT,
      source_chat_id BIGINT NOT NULL,
      source_message_id BIGINT NOT NULL,
      target_chat_ids BIGINT[] NOT NULL,
      sent_count INT NOT NULL DEFAULT 0,
      error_count INT NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
    ON userbot_tasks(status, run_at)
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


def admin_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📌 Добавить пост")],
            [KeyboardButton(text="✏️ Изменить пост")],
            [KeyboardButton(text="📊 Статус")],
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
                datetime.utcnow().isoformat(),
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
        datetime.utcnow().isoformat(),
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


async def set_last_storage_message(storage_chat_id: int, message_id: int) -> None:
    await set_meta(LAST_STORAGE_CHAT_ID_META_KEY, str(storage_chat_id))
    await set_meta(LAST_STORAGE_MESSAGE_ID_META_KEY, str(message_id))


async def get_broadcast_meta() -> dict:
    now = datetime.utcnow()
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
        "SELECT source_chat_id, source_message_id, has_media, media_group_id FROM post WHERE id = 1"
    )
    if not row:
        return {
            "source_chat_id": None,
            "source_message_id": None,
            "has_media": False,
            "media_group_id": None,
        }
    return {
        "source_chat_id": row["source_chat_id"],
        "source_message_id": row["source_message_id"],
        "has_media": bool(row["has_media"]),
        "media_group_id": row["media_group_id"],
    }


async def save_post(source_chat_id: int, source_message_id: int, has_media: bool, media_group_id: Optional[str]) -> None:
    pool = await get_db_pool()
    await pool.execute(
        """
        UPDATE post
        SET source_chat_id = $1, source_message_id = $2, has_media = $3, media_group_id = $4
        WHERE id = 1
        """,
        source_chat_id,
        source_message_id,
        int(has_media),
        media_group_id,
    )


async def create_userbot_task(source_chat_id: int, source_message_id: int, target_chat_ids: list[int]) -> Optional[int]:
    if not target_chat_ids:
        return None

    pool = await get_db_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO userbot_tasks(source_chat_id, source_message_id, target_chat_ids, run_at)
        VALUES ($1, $2, $3::BIGINT[], NOW())
        RETURNING id
        """,
        source_chat_id,
        source_message_id,
        target_chat_ids,
    )
    return int(row["id"]) if row else None


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
        return datetime.fromisoformat(value)
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


def support_keyboard() -> InlineKeyboardMarkup:
    deep_link = f"https://t.me/{BOT_USERNAME}?start={SUPPORT_PAYLOAD}"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=SUPPORT_BUTTON_TEXT, url=deep_link)]]
    )


async def send_post_preview(message: Message) -> None:
    post = await get_post()
    source_chat_id = post["source_chat_id"]
    source_message_id = post["source_message_id"]

    if not source_chat_id or not source_message_id:
        await message.answer("Текущий пост:\n\n(пусто)")
        return

    await bot.copy_message(
        chat_id=message.chat.id,
        from_chat_id=source_chat_id,
        message_id=source_message_id,
    )


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
    source_message_id = post["source_message_id"]

    if not source_chat_id or not source_message_id:
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
        delta_seconds = (datetime.utcnow() - last_broadcast_dt).total_seconds()
        if delta_seconds < GLOBAL_BROADCAST_COOLDOWN_SECONDS:
            logger.info("Broadcast skipped: global cooldown %.1fs", delta_seconds)
            return

    chats = await get_chat_rows()
    support_enabled = SEND_SUPPORT_MESSAGE_WITH_BROADCAST and bool(BOT_USERNAME)
    keyboard = support_keyboard() if support_enabled else None
    report = {
        "total_chats": len(chats),
        "sent": 0,
        "activity_ready": 0,
        "skipped_by_activity": 0,
        "skipped_by_duplicate": 0,
        "skipped_disabled": 0,
        "errors": 0,
        "privacy_warning": has_privacy_mode_symptoms(chats),
    }

    target_chat_ids: list[int] = []

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
                hours_since = (datetime.utcnow() - last_chat_send).total_seconds() / 3600
                if hours_since < ANTI_DUPLICATE_HOURS:
                    report["skipped_by_duplicate"] += 1
                    await save_broadcast_attempt(status="skipped", chat_id=chat_id, reason="anti_duplicate")
                    continue

        if DELIVERY_MODE == "userbot":
            target_chat_ids.append(chat_id)
            report["sent"] += 1
            await save_broadcast_attempt(status="queued", chat_id=chat_id, reason="queued_for_userbot")
            continue

        try:
            sent = await bot.copy_message(
                chat_id=chat_id,
                from_chat_id=source_chat_id,
                message_id=source_message_id,
            )
            if keyboard:
                await bot.send_message(chat_id=chat_id, text="Связь с поддержкой 👇", reply_markup=keyboard)

            if sent:
                await mark_chat_send_success(chat_id, sent.message_id)
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

    if DELIVERY_MODE == "userbot" and target_chat_ids:
        task_id = await create_userbot_task(source_chat_id, source_message_id, target_chat_ids)
        logger.info("Queued userbot task id=%s chats=%s", task_id, len(target_chat_ids))
        await bot.send_message(ADMIN_ID, f"🧾 Userbot-рассылка поставлена в очередь: {len(target_chat_ids)} чатов.")

    if report["sent"] > 0:
        await set_meta("last_broadcast_at", datetime.utcnow().isoformat())
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

    await message.answer(
        "\n".join(
            [
                "📊 Статус рассылки",
                f"Всего чатов: {total}",
                f"Отключено: {disabled}",
                f"Готовы по правилу активности: {active_ready}",
                f"Заблокированы правилом активности: {blocked_by_activity}",
                f"Глобальная пауза: {GLOBAL_BROADCAST_COOLDOWN_SECONDS} сек.",
                f"Лимит запусков/сутки: {BROADCAST_MAX_PER_DAY}",
                f"Запусков сегодня: {meta['daily_broadcast_count']}",
                f"Последняя рассылка UTC: {meta['last_broadcast_at'] or '-'}",
                f"Тихие часы: {parse_time(QUIET_HOURS_START) or '-'} → {parse_time(QUIET_HOURS_END) or '-'}",
                *warning_lines,
                "",
                "Последние попытки рассылки:",
                *(attempt_lines or ["• пока нет"]),
            ]
        )
    )


@dp.callback_query(F.data == "broadcast:confirm_today")
async def confirm_daily_broadcast(callback: CallbackQuery):
    if not is_admin_user(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return

    day_key = datetime.utcnow().strftime("%Y-%m-%d")
    await set_meta("manual_confirmed_day", day_key)
    await callback.answer("Подтверждено")

    if callback.message:
        await callback.message.answer("Подтверждение получено. Запускаю рассылку ✅")
    await broadcast_once()


@dp.message(CommandStart())
async def on_start(message: Message, state: FSMContext):
    args = (message.text or "").split(maxsplit=1)
    payload = args[1] if len(args) > 1 else ""

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

    if message.media_group_id:
        await message.answer("Пока поддерживается только один пост = одно сообщение (без альбомов).")
        return

    storage_chat_id = await get_storage_chat_id()
    if storage_chat_id == 0:
        await message.answer("Ошибка: STORAGE_CHAT_ID не настроен.")
        return

    previous_storage_chat_id, previous_storage_message_id = await get_last_storage_message()
    if previous_storage_message_id is not None:
        delete_chat_id = previous_storage_chat_id or storage_chat_id
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

    copied = await bot.copy_message(
        chat_id=storage_chat_id,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
    )
    logger.info("Published new storage post chat_id=%s message_id=%s", storage_chat_id, copied.message_id)
    await set_last_storage_message(storage_chat_id, copied.message_id)

    await save_post(
        source_chat_id=storage_chat_id,
        source_message_id=copied.message_id,
        has_media=bool(message.photo or message.video or message.animation or message.document),
        media_group_id=message.media_group_id,
    )

    data = await state.get_data()
    mode = data.get("mode")
    await state.clear()

    if mode == "edit":
        await message.answer("Пост обновлён. Существующие расписания сохранены ✅")
    else:
        await message.answer("Пост сохранён ✅")

    await send_post_actions(message)


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

    await message.answer(f"Время {parsed} добавлено ✅")
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
    await message.answer("Еженедельное расписание добавлено ✅")
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
    await message.answer("Время обновлено ✅")
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
