import asyncio
import contextlib
from datetime import date

import asyncpg
from aiogram import Bot, F, Router
from aiogram.types import Message

import db
import roulette
from config import (ANIM_DELAY, ANIM_FRAMES, COIN_RATE, FREE_ROULETTE,
                    FREE_ROULETTE_BUDGET, SPIN_COMMANDS, UNLIMITED_SPIN_IDS)
from services import settings, ui
from services.render import edit as r_edit, reply as r_reply

router = Router()


class BudgetExhausted(Exception):
    pass


class ChatBlocked(Exception):
    pass


async def _charge_budget(conn, chat_id: int, title: str, cost: int) -> None:
    """
    Списать cost (в грибах) с суточного бюджета. Бросает BudgetExhausted / ChatBlocked.

    Привязанный чат -> его собственный бюджет из rb_chats.
    Непривязанный  -> общий потолок FREE_ROULETTE_BUDGET на ВСЕ такие чаты сразу.
    Отдельный счётчик, потому что у случайного чата нет и не должно быть строки
    в rb_chats: иначе он вылезет в «Мою ссылку» и в маршрутизацию выводов.
    """
    bound = await conn.fetchrow(
        "SELECT * FROM rb_chats WHERE chat_id=$1 AND active FOR UPDATE", chat_id)

    if bound:
        if bound["budget_date"] != date.today():
            await conn.execute(
                "UPDATE rb_chats SET budget_date=CURRENT_DATE, budget_spent_mush=0 "
                "WHERE chat_id=$1", chat_id)
            spent = 0
        else:
            spent = bound["budget_spent_mush"]
        if spent + cost > bound["daily_budget_mush"]:
            raise BudgetExhausted
        await conn.execute(
            "UPDATE rb_chats SET budget_spent_mush = budget_spent_mush + $1 WHERE chat_id=$2",
            cost, chat_id)
        return

    # --- свободный чат ---
    if not FREE_ROULETTE:
        raise ChatBlocked
    fc = await conn.fetchrow("SELECT blocked FROM rb_free_chats WHERE chat_id=$1", chat_id)
    if fc and fc["blocked"]:
        raise ChatBlocked

    await conn.execute(
        "INSERT INTO rb_free_budget (day) VALUES (CURRENT_DATE) ON CONFLICT DO NOTHING")
    spent = await conn.fetchval(
        "SELECT spent_mush FROM rb_free_budget WHERE day=CURRENT_DATE FOR UPDATE")
    if spent + cost > FREE_ROULETTE_BUDGET:
        raise BudgetExhausted
    await conn.execute(
        "UPDATE rb_free_budget SET spent_mush = spent_mush + $1 WHERE day=CURRENT_DATE", cost)
    await conn.execute(
        """
        INSERT INTO rb_free_chats (chat_id, title, spins) VALUES ($1,$2,1)
        ON CONFLICT (chat_id) DO UPDATE
        SET title=EXCLUDED.title, spins=rb_free_chats.spins+1, last_seen=now()
        """, chat_id, title)


def _match(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(t == c or t.startswith(c + " ") for c in SPIN_COMMANDS)


@router.message(F.chat.type.in_({"group", "supergroup"}), F.text.func(_match))
async def spin(msg: Message, bot: Bot):
    uid = msg.from_user.id
    await db.upsert_user(uid, msg.from_user.username, msg.from_user.first_name)

    if await db.is_banned(uid):
        return await ui.reply(msg, "🚫 Ты заблокирован в системе.")

    user = await db.get_user(uid)
    cur = user["currency"]
    amount = roulette.roll(cur)
    today = date.today()

    # оформление берём из админки (эмодзи + премиум), а не из config
    e_cur = await settings.emoji(cur)
    e_rou = await settings.emoji("roulette")
    label = await settings.label(cur)
    em = await settings.emoji_map()

    # Кто первый вставил строку в rb_spins за сегодня — тот и крутит.
    # UNIQUE(tg_id, spin_day) => гонка из десяти сообщений подряд не пройдёт.
    # Прокрутка одна в сутки НА ЮЗЕРА, а не на чат: два чата ≠ две прокрутки.
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                cost = amount // COIN_RATE if cur == "coins" else amount
                await _charge_budget(conn, msg.chat.id, msg.chat.title or "", cost)

                if uid in UNLIMITED_SPIN_IDS:
                    # UNIQUE(tg_id, spin_day) не обходим — сносим сегодняшнюю строку
                    # и пишем заново. Индекс остаётся боевым для всех остальных.
                    # Леджер при этом полный: у каждой прокрутки свой spin:<id>.
                    await conn.execute(
                        "DELETE FROM rb_spins WHERE tg_id=$1 AND spin_day=$2", uid, today)

                spin_id = await conn.fetchval(
                    "INSERT INTO rb_spins (tg_id, chat_id, currency, amount, spin_day) "
                    "VALUES ($1,$2,$3,$4,$5) RETURNING id",
                    uid, msg.chat.id, cur, amount, today)
                total = await db.apply(conn, uid, cur, amount, "roulette",
                                       f"spin:{spin_id}", spin_id)
    except asyncpg.UniqueViolationError:
        last = await db.pool().fetchval(
            "SELECT amount FROM rb_spins WHERE tg_id=$1 AND spin_day=$2", uid, today)
        return await r_reply(
            msg, f"⏳ Ты уже крутил сегодня (выпало {last:,} {e_cur}).\n"
                 f"Возвращайся завтра.".replace(",", " "), em)
    except BudgetExhausted:
        # прокрутка НЕ засчитана — транзакция откатилась, завтра крутанёт
        return await ui.reply(msg, "🧯 Суточный лимит выплат в этом чате исчерпан.\n"
                               "Прокрутка не потрачена — заходи завтра.")

    # анимация
    m = await r_reply(msg, roulette.frame(0, e_rou), em)
    for i in range(1, ANIM_FRAMES):
        await asyncio.sleep(ANIM_DELAY)
        with contextlib.suppress(Exception):
            await r_edit(m, roulette.frame(i, e_rou), em)

    await asyncio.sleep(ANIM_DELAY)
    card = roulette.result_card(msg.from_user.first_name, amount, e_cur, label, total, e_rou)
    with contextlib.suppress(Exception):
        await r_edit(m, card, em)


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.lower().in_({"!баланс", "!шимм баланс", "!шим баланс"}))
async def bal(msg: Message):
    await db.upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    b = await db.balances(msg.from_user.id)
    me = await msg.bot.get_me()
    s = await settings.ctx()
    await r_reply(
        msg,
        f"{s['e_balance']} <b>{msg.from_user.first_name}</b>\n"
        f"<blockquote>{s['e_mushrooms']} {s['l_mushrooms']}: <b>{b['mushrooms']:,}</b>\n"
        f"{s['e_coins']} {s['l_coins']}: <b>{b['coins']:,}</b></blockquote>\n"
        f"Подробнее и вывод — в боте: @{me.username}".replace(",", " "),
        await settings.emoji_map())
