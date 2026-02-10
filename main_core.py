import asyncio
import pytz
import aiosqlite
from datetime import datetime, time, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler

BOT_TOKEN = "PASTE_TOKEN"
ADMIN_ID = 123456789  # ваш telegram user id
TZ = pytz.timezone("Asia/Yerevan")  # поменяйте при необходимости

DB_PATH = "bot.db"

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS chats (
  chat_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS post (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  text TEXT
);

INSERT OR IGNORE INTO post (id, text) VALUES (1, '');
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES_SQL)
        await db.commit()

async def is_admin_in_chat(message: Message) -> bool:
    # В группах/супергруппах проверяем админа
    if message.chat.type not in ("group", "supergroup"):
        return False
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return member.status in ("administrator", "creator")

async def get_all_chats():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT chat_id FROM chats") as cur:
            rows = await cur.fetchall()
    return [r[0] for r in rows]

async def set_post_text(text: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE post SET text=? WHERE id=1", (text,))
        await db.commit()

async def get_post_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT text FROM post WHERE id=1") as cur:
            row = await cur.fetchone()
    return row[0] if row else ""

async def broadcast():
    text = await get_post_text()
    if not text.strip():
        return

    chat_ids = await get_all_chats()
    sent = 0
    failed = 0

    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            sent += 1
            # антифлуд: чуть притормозим
            await asyncio.sleep(0.25)
        except Exception:
            failed += 1
            await asyncio.sleep(0.5)

    # отчёт админу
    await bot.send_message(
        ADMIN_ID,
        f"Рассылка завершена. Успешно: {sent}, ошибок: {failed}, чатов в списке: {len(chat_ids)}"
    )

def next_run_times_for_day(day: datetime):
    # 08:00 -> 21:30 каждые 90 минут
    start = datetime.combine(day.date(), time(8, 0), TZ)
    end = datetime.combine(day.date(), time(22, 0), TZ)
    t = start
    times = []
    while t < end:
        times.append(t)
        t += timedelta(minutes=90)
    return times

def schedule_daily_jobs(scheduler: AsyncIOScheduler):
    # планируем на сегодня и на завтра; затем можно обновлять раз в сутки
    now = datetime.now(TZ)
    for d in [now, now + timedelta(days=1)]:
        for run_dt in next_run_times_for_day(d):
            if run_dt > now:
                scheduler.add_job(
                    lambda: asyncio.create_task(broadcast()),
                    trigger="date",
                    run_date=run_dt
                )

@dp.message(Command("register"))
async def register_chat(message: Message):
    if not await is_admin_in_chat(message):
        await message.reply("Команда доступна только админам чата.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO chats (chat_id) VALUES (?)", (message.chat.id,))
        await db.commit()

    await message.reply("Чат зарегистрирован для рассылки ✅")

@dp.message(Command("unregister"))
async def unregister_chat(message: Message):
    if not await is_admin_in_chat(message):
        await message.reply("Команда доступна только админам чата.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM chats WHERE chat_id=?", (message.chat.id,))
        await db.commit()

    await message.reply("Чат удалён из рассылки ✅")

@dp.message(Command("setpost"))
async def setpost(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("Только владелец может менять пост.")
        return

    text = message.text.split(maxsplit=1)
    if len(text) < 2:
        await message.reply("Использование: /setpost ваш текст поста")
        return

    await set_post_text(text[1])
    await message.reply("Пост сохранён ✅")

@dp.message(Command("broadcast"))
async def broadcast_now(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("Только владелец может делать рассылку.")
        return
    await message.reply("Запускаю рассылку…")
    await broadcast()

@dp.message(Command("myid"))
async def myid(message: Message):
    await message.reply(f"Ваш ID: {message.from_user.id}\nChat ID: {message.chat.id}")

# Личка: “связаться со мной через бота”
@dp.message(F.chat.type == "private")
async def dm_forward(message: Message):
    # Пересылаем админу всё, что пишут в личку боту
    try:
        await bot.send_message(
            ADMIN_ID,
            f"Сообщение от @{message.from_user.username or 'без_юзернейма'} (id={message.from_user.id}):\n{message.text or '[не текст]'}"
        )
    except Exception:
        pass

    await message.reply("Сообщение отправлено владельцу ✅")

async def main():
    await init_db()

    scheduler = AsyncIOScheduler(timezone=TZ)
    schedule_daily_jobs(scheduler)
    scheduler.start()

    # раз в час обновляем план на “завтра/сегодня” (чтобы не кончились джобы)
    async def refresh_loop():
        while True:
            await asyncio.sleep(3600)
            schedule_daily_jobs(scheduler)

    asyncio.create_task(refresh_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
