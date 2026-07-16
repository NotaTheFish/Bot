import asyncio
import contextlib
from datetime import date

import asyncpg
from aiogram import Bot, F, Router
from aiogram.types import Message, MessageEntity

import db
import roulette
from config import (ANIM_DELAY, ANIM_FRAMES, CURRENCY_EMOJI, PREMIUM_EMOJI,
                    SPIN_COMMANDS, COIN_RATE)

router = Router()


def _premium_entities(text: str) -> list[MessageEntity] | None:
    """
    Премиум-эмодзи. Работают ТОЛЬКО если у бота куплен доп. юзернейм на Fragment.
    Если PREMIUM_EMOJI пуст — возвращаем None и всё летит обычными эмодзи.
    """
    if not PREMIUM_EMOJI:
        return None
    ents = []
    for emoji, cid in PREMIUM_EMOJI.items():
        start = 0
        while True:
            i = text.find(emoji, start)
            if i == -1:
                break
            offset = len(text[:i].encode("utf-16-le")) // 2
            length = len(emoji.encode("utf-16-le")) // 2
            ents.append(MessageEntity(type="custom_emoji", offset=offset,
                                      length=length, custom_emoji_id=cid))
            start = i + len(emoji)
    return ents or None


def _match(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(t == c or t.startswith(c + " ") for c in SPIN_COMMANDS)


@router.message(F.chat.type.in_({"group", "supergroup"}), F.text.func(_match))
async def spin(msg: Message, bot: Bot):
    uid = msg.from_user.id
    await db.upsert_user(uid, msg.from_user.username, msg.from_user.first_name)

    if await db.is_banned(uid):
        return await msg.reply("🚫 Ты заблокирован в системе.")

    chat = await db.pool().fetchrow(
        "SELECT * FROM rb_chats WHERE chat_id=$1 AND active", msg.chat.id)
    if not chat:
        return

    user = await db.get_user(uid)
    cur = user["currency"]
    amount = roulette.roll(cur)
    today = date.today()

    # Кто первый вставил строку в rb_spins за сегодня — тот и крутит.
    # UNIQUE(tg_id, spin_day) => гонка из десяти сообщений подряд не пройдёт.
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                spin_id = await conn.fetchval(
                    "INSERT INTO rb_spins (tg_id, chat_id, currency, amount, spin_day) "
                    "VALUES ($1,$2,$3,$4,$5) RETURNING id",
                    uid, msg.chat.id, cur, amount, today)
                total = await db.apply(conn, uid, cur, amount, "roulette",
                                       f"spin:{spin_id}", spin_id)
    except asyncpg.UniqueViolationError:
        last = await db.pool().fetchval(
            "SELECT amount FROM rb_spins WHERE tg_id=$1 AND spin_day=$2", uid, today)
        return await msg.reply(
            f"⏳ Ты уже крутил сегодня (выпало {last:,} {CURRENCY_EMOJI[cur]}).\n"
            f"Возвращайся завтра.".replace(",", " "))

    # анимация
    m = await msg.reply(roulette.frame(0, CURRENCY_EMOJI[cur]))
    for i in range(1, ANIM_FRAMES):
        await asyncio.sleep(ANIM_DELAY)
        with contextlib.suppress(Exception):
            await m.edit_text(roulette.frame(i, CURRENCY_EMOJI[cur]))

    await asyncio.sleep(ANIM_DELAY)
    name = msg.from_user.first_name
    card = roulette.result_card(name, amount, cur, CURRENCY_EMOJI[cur], total)
    try:
        await m.edit_text(card, entities=_premium_entities(card), parse_mode="HTML")
    except Exception:
        with contextlib.suppress(Exception):
            await m.edit_text(card)


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.lower().in_({"!баланс", "!шимм баланс", "!шим баланс"}))
async def bal(msg: Message):
    await db.upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    b = await db.balances(msg.from_user.id)
    me = await msg.bot.get_me()
    await msg.reply(
        f"💰 <b>{msg.from_user.first_name}</b>\n"
        f"🍄 {b['mushrooms']:,}\n🪙 {b['coins']:,}\n\n"
        f"Подробнее и вывод — в боте: @{me.username}".replace(",", " "))
