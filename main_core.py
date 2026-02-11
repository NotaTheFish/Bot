import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from zoneinfo import ZoneInfo

# =========================
# ENV / CONFIG
# =========================

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

TZ_NAME = _get_env_str("TZ", "Europe/Berlin")
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ = ZoneInfo("Europe/Berlin")

DB_PATH = _get_env_str("DB_PATH", "bot.db")

SUPPORT_BUTTON_TEXT = _get_env_str("SUPPORT_BUTTON_TEXT", "Если у вас бан")
SUPPORT_START_PAYLOAD = _get_env_str("SUPPORT_START_PAYLOAD", "contact_admin")

BROADCAST_JOB_ID = "broadcast_job"
BOT_USERNAME = ""

# Валидируем env до создания Bot()
if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не задан или неверного формата. "
        "Проверь Railway → Variables → BOT_TOKEN (должен быть вида 123456:ABC...)."
    )
if ADMIN_ID <= 0:
    raise RuntimeError(
        "ADMIN_ID не задан или неверный. "
        "Проверь Railway → Variables → ADMIN_ID (число)."
    )

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

# =========================
# DB
# =========================

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS chats (
  chat_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS post (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  text TEXT NOT NULL DEFAULT '',
  photo_file_id TEXT,
  contact_username TEXT NOT NULL DEFAULT ''
);

INSERT OR IGNORE INTO post (id, text, photo_file_id, contact_username)
VALUES (1, '', NULL, '');

CREATE TABLE IF NOT EXISTS schedule (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  interval_minutes INTEGER,
  start_hour INTEGER,
  end_hour INTEGER,
  daily INTEGER,
  active INTEGER NOT NULL DEFAULT 0,
  created_at TEXT
);

INSERT OR IGNORE INTO schedule
(id, interval_minutes, start_hour, end_hour, daily, active, created_at)
VALUES (1, NULL, NULL, NULL, 1, 0, NULL);
"""

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES_SQL)
        await db.commit()

# =========================
# HELPERS
# =========================

def ensure_admin(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id == ADMIN_ID)

async def is_admin_in_chat(message: Message) -> bool:
    if message.chat.type not in ("group", "supergroup"):
        return False
    if not message.from_user:
        return False
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return member.status in ("administrator", "creator")

async def get_all_chats() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT chat_id FROM chats") as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]

async def set_post(text: str, photo_file_id: Optional[str], username: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE post
               SET text = ?, photo_file_id = ?, contact_username = ?
             WHERE id = 1
            """,
            (text.strip(), photo_file_id, username.strip().lstrip("@")),
        )
        await db.commit()

async def get_post() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT text, photo_file_id, contact_username FROM post WHERE id = 1"
        ) as cur:
            row = await cur.fetchone()

    if not row:
        return {"text": "", "photo_file_id": None, "contact_username": ""}

    return {"text": row[0], "photo_file_id": row[1], "contact_username": row[2]}

async def save_schedule(
    interval_minutes: int,
    start_hour: int,
    end_hour: int,
    daily: bool,
    active: bool,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE schedule
               SET interval_minutes = ?,
                   start_hour = ?,
                   end_hour = ?,
                   daily = ?,
                   active = ?,
                   created_at = ?
             WHERE id = 1
            """,
            (
                interval_minutes,
                start_hour,
                end_hour,
                1 if daily else 0,
                1 if active else 0,
                datetime.now(TZ).isoformat(),
            ),
        )
        await db.commit()

async def get_schedule() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT interval_minutes, start_hour, end_hour, daily, active
              FROM schedule
             WHERE id = 1
            """
        ) as cur:
            row = await cur.fetchone()

    if not row:
        return {
            "interval_minutes": None,
            "start_hour": None,
            "end_hour": None,
            "daily": True,
            "active": False,
        }

    return {
        "interval_minutes": row[0],
        "start_hour": row[1],
        "end_hour": row[2],
        "daily": bool(row[3]),
        "active": bool(row[4]),
    }

def build_inline_keyboard() -> InlineKeyboardMarkup:
    deep_link = f"https://t.me/{BOT_USERNAME}?start={SUPPORT_START_PAYLOAD}"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=SUPPORT_BUTTON_TEXT, url=deep_link)]]
    )

async def compose_post_text() -> str:
    post = await get_post()
    if not post["text"].strip():
        return ""

    contact_username = post["contact_username"]
    suffix = (
        f"\n\nЕсли заинтересовало, пишите сюда @{contact_username}"
        if contact_username
        else ""
    )
    return f"{post['text']}{suffix}"

# =========================
# BROADCAST
# =========================

async def broadcast() -> None:
    post = await get_post()
    text = await compose_post_text()
    if not text:
        await bot.send_message(ADMIN_ID, "Рассылка пропущена: пост пустой.")
        return

    keyboard = build_inline_keyboard()
    chat_ids = await get_all_chats()

    sent, failed = 0, 0
    for chat_id in chat_ids:
        try:
            if post["photo_file_id"]:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=post["photo_file_id"],
                    caption=text,
                    reply_markup=keyboard,
                )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    disable_web_page_preview=True,
                    reply_markup=keyboard,
                )
            sent += 1
            await asyncio.sleep(0.25)
        except Exception:
            failed += 1
            await asyncio.sleep(0.5)

    await bot.send_message(
        ADMIN_ID,
        f"Рассылка завершена. Успешно: {sent}, ошибок: {failed}, чатов: {len(chat_ids)}",
    )

def schedule_broadcast_job(
    interval_minutes: int,
    start_hour: int,
    end_hour: int,
    daily: bool,
) -> None:
    if scheduler.get_job(BROADCAST_JOB_ID):
        scheduler.remove_job(BROADCAST_JOB_ID)

    now = datetime.now(TZ)
    today_start = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=end_hour % 24, minute=0, second=0, microsecond=0)

    # Если end_hour == 24, то это фактически конец дня -> +1 день в 00:00
    if end_hour == 24:
        today_end = today_end + timedelta(days=1)

    if now <= today_start:
        first_run = today_start
    elif now < today_end:
        delta_minutes = int((now - today_start).total_seconds() // 60)
        steps = (delta_minutes // interval_minutes) + 1
        first_run = today_start + timedelta(minutes=steps * interval_minutes)
    else:
        first_run = today_start + timedelta(days=1)

    trigger_kwargs = {
        "minutes": interval_minutes,
        "start_date": first_run,
        "timezone": TZ,
    }

    if not daily:
        # Однодневный запуск только до конца окна
        if first_run.date() == now.date():
            end_date = today_end
        else:
            end_date = first_run.replace(hour=end_hour % 24, minute=0, second=0, microsecond=0)
            if end_hour == 24:
                end_date = end_date + timedelta(days=1)
        trigger_kwargs["end_date"] = end_date

    scheduler.add_job(
        broadcast,
        trigger=IntervalTrigger(**trigger_kwargs),
        id=BROADCAST_JOB_ID,
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )

def valid_schedule(interval_minutes: int, start_hour: int, end_hour: int) -> Optional[str]:
    if interval_minutes <= 0:
        return "Интервал должен быть > 0 минут."
    if not (0 <= start_hour <= 23 and 1 <= end_hour <= 24):
        return "Часы должны быть в диапазоне start: 0..23, end: 1..24."
    if start_hour >= end_hour:
        return "start_hour должен быть меньше end_hour."
    return None

# =========================
# COMMANDS
# =========================

@dp.message(Command("register"))
async def register_chat(message: Message):
    if not await is_admin_in_chat(message):
        await message.reply("Команда доступна только админам чата.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO chats (chat_id) VALUES (?)",
            (message.chat.id,),
        )
        await db.commit()

    await message.reply("Чат зарегистрирован для рассылки ✅")

@dp.message(Command("unregister"))
async def unregister_chat(message: Message):
    if not await is_admin_in_chat(message):
        await message.reply("Команда доступна только админам чата.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM chats WHERE chat_id = ?", (message.chat.id,))
        await db.commit()

    await message.reply("Чат удалён из рассылки ✅")

@dp.message(Command("setpost"))
async def setpost(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может менять пост.")
        return

    payload = (message.text or "").split(maxsplit=2)
    if len(payload) < 3:
        await message.reply(
            "Использование: /setpost @username текст поста\n"
            "Фото можно прикрепить в этом же сообщении."
        )
        return

    username = payload[1]
    text = payload[2]

    photo_file_id = message.photo[-1].file_id if message.photo else None

    await set_post(text=text, photo_file_id=photo_file_id, username=username)
    await message.reply("Пост сохранён и будет использоваться в следующих рассылках ✅")

@dp.message(Command("setphoto"))
async def setphoto(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может менять фото.")
        return

    if not message.photo:
        await message.reply("Пришлите команду /setphoto вместе с фото.")
        return

    post = await get_post()
    await set_post(
        text=post["text"],
        photo_file_id=message.photo[-1].file_id,
        username=post["contact_username"],
    )
    await message.reply("Фото для рассылки обновлено ✅")

@dp.message(Command("clearphoto"))
async def clearphoto(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может менять фото.")
        return

    post = await get_post()
    await set_post(text=post["text"], photo_file_id=None, username=post["contact_username"])
    await message.reply("Фото удалено. Теперь будет отправляться только текст ✅")

@dp.message(Command("schedule"))
async def set_schedule(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может менять расписание.")
        return

    parts = (message.text or "").split()
    if len(parts) != 5:
        await message.reply(
            "Использование: /schedule interval_minutes start_hour end_hour daily\n"
            "Пример: /schedule 90 8 22 yes"
        )
        return

    try:
        interval_minutes = int(parts[1])
        start_hour = int(parts[2])
        end_hour = int(parts[3])
    except ValueError:
        await message.reply("interval/start/end должны быть числами.")
        return

    daily = parts[4].lower() in {"yes", "y", "1", "true", "daily", "да"}

    error = valid_schedule(interval_minutes, start_hour, end_hour)
    if error:
        await message.reply(error)
        return

    schedule_broadcast_job(interval_minutes, start_hour, end_hour, daily)
    await save_schedule(interval_minutes, start_hour, end_hour, daily=daily, active=True)

    await message.reply(
        "Расписание обновлено ✅\n"
        f"Интервал: {interval_minutes} мин\n"
        f"Окно: {start_hour:02d}:00-{end_hour:02d}:00 ({TZ_NAME})\n"
        f"Ежедневно: {'да' if daily else 'нет (только ближайший день)'}"
    )

@dp.message(Command("stopschedule"))
async def stop_schedule(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может менять расписание.")
        return

    if scheduler.get_job(BROADCAST_JOB_ID):
        scheduler.remove_job(BROADCAST_JOB_ID)

    old_schedule = await get_schedule()
    await save_schedule(
        interval_minutes=old_schedule["interval_minutes"] or 60,
        start_hour=old_schedule["start_hour"] or 8,
        end_hour=old_schedule["end_hour"] or 23,
        daily=old_schedule["daily"],
        active=False,
    )

    await message.reply("Расписание остановлено ✅")

@dp.message(Command("broadcast"))
async def broadcast_now(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может делать рассылку.")
        return

    await message.reply("Запускаю рассылку…")
    await broadcast()

@dp.message(Command("status"))
async def status(message: Message):
    if not ensure_admin(message):
        await message.reply("Только владелец может смотреть статус.")
        return

    schedule_info = await get_schedule()
    post = await get_post()
    chats = await get_all_chats()

    await message.reply(
        "Статус:\n"
        f"- Чатов: {len(chats)}\n"
        f"- Пост: {'задан' if post['text'] else 'пустой'}\n"
        f"- Фото: {'есть' if post['photo_file_id'] else 'нет'}\n"
        f"- Контакт: @{post['contact_username'] or '-'}\n"
        f"- Расписание активно: {'да' if schedule_info['active'] else 'нет'}\n"
        f"- Интервал: {schedule_info['interval_minutes']} мин\n"
        f"- Окно: {schedule_info['start_hour']}:00-{schedule_info['end_hour']}:00\n"
        f"- Ежедневно: {'да' if schedule_info['daily'] else 'нет'}"
    )

@dp.message(Command("start"))
async def start_private(message: Message, command: CommandObject):
    if message.chat.type != "private":
        return

    if command.args == SUPPORT_START_PAYLOAD:
        await message.reply("Напишите сообщение в ответ на это — я передам его администратору.")
        return

    if ensure_admin(message):
        await message.reply(
            "Команды владельца:\n"
            "/setpost @username текст (можно с фото)\n"
            "/setphoto (вместе с фото)\n"
            "/clearphoto\n"
            "/schedule 90 8 22 yes\n"
            "/stopschedule\n"
            "/status\n"
            "/broadcast\n"
            "\nКоманды для чатов:\n"
            "/register (в группе, где вы админ)\n"
            "/unregister"
        )
        return

    await message.reply("Привет! Напишите сообщение, и я отправлю его администратору.")

@dp.message(F.chat.type == "private")
async def dm_forward(message: Message):
    if ensure_admin(message):
        return

    sender = message.from_user
    username = f"@{sender.username}" if sender and sender.username else "без_юзернейма"

    try:
        if message.text:
            await bot.send_message(
                ADMIN_ID,
                f"Сообщение от {username} (id={sender.id}):\n{message.text}",
            )
        elif message.photo:
            await bot.send_photo(
                ADMIN_ID,
                photo=message.photo[-1].file_id,
                caption=f"Фото от {username} (id={sender.id})",
            )
        else:
            await bot.send_message(
                ADMIN_ID,
                f"Пользователь {username} (id={sender.id}) отправил неподдерживаемый тип сообщения.",
            )

        await message.reply("Отправлено администратору ✅")
    except Exception:
        await message.reply("Не удалось отправить сообщение администратору.")

# =========================
# STARTUP
# =========================

async def restore_schedule_from_db() -> None:
    schedule_info = await get_schedule()
    if not schedule_info["active"]:
        return
    if not all(
        value is not None
        for value in (
            schedule_info["interval_minutes"],
            schedule_info["start_hour"],
            schedule_info["end_hour"],
        )
    ):
        return

    schedule_broadcast_job(
        interval_minutes=schedule_info["interval_minutes"],
        start_hour=schedule_info["start_hour"],
        end_hour=schedule_info["end_hour"],
        daily=schedule_info["daily"],
    )

async def main():
    global BOT_USERNAME

    await init_db()
    BOT_USERNAME = (await bot.get_me()).username or ""

    scheduler.start()
    await restore_schedule_from_db()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
