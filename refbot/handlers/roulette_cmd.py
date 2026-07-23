import asyncio
import contextlib
import logging
from datetime import date

import asyncpg
from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import Message

import db
import roulette
import time

from config import (ANIM_DELAY, ANIM_FRAMES, COIN_RATE, ROULETTE_DAILY_BUDGET,
                    SPIN_COMMANDS, SPIN_NAG_COOLDOWN, SUPER_ADMINS,
                    UNLIMITED_SPIN_IDS)
from services import settings, spin_queue, ui
from services.render import edit as r_edit, reply as r_reply
from services import reactions

router = Router()
log = logging.getLogger("roulette")


# ==================== ВКЛ/ВЫКЛ РУЛЕТКИ (/шимм, /отшимм) ====================
@router.message(Command("шимм", "shimm"), F.chat.type.in_({"group", "supergroup"}))
async def shimm_on(msg: Message):
    """
    Включить рулетку в этом чате. ТОЛЬКО главный админ бота (SUPER_ADMINS):
    рулетка тратит грибы из его кармана, ему и решать, где она крутит.
    """
    if msg.from_user.id not in SUPER_ADMINS:
        return  # молчим для всех, кроме владельца бота — команда не «светится»
    await db.pool().execute(
        """
        INSERT INTO rb_roulette_chats (chat_id, title, active, enabled_by, enabled_at)
        VALUES ($1, $2, TRUE, $3, now())
        ON CONFLICT (chat_id) DO UPDATE
        SET active = TRUE, title = EXCLUDED.title, enabled_by = EXCLUDED.enabled_by,
            deactivated_at = NULL, deactivated_by = NULL
        """, msg.chat.id, msg.chat.title or "", msg.from_user.id)
    await db.audit(msg.from_user.id, "roulette_on", {"chat_id": msg.chat.id})
    await ui.reply(msg, "🎰 Рулетка включена в этом чате. Игроки могут крутить !шайн.\n"
                        "Выключить: /отшимм")


@router.message(Command("отшимм", "otshimm"), F.chat.type.in_({"group", "supergroup"}))
async def shimm_off(msg: Message):
    """Выключить рулетку в этом чате. Только главный админ бота."""
    if msg.from_user.id not in SUPER_ADMINS:
        return
    row = await db.pool().fetchrow(
        "SELECT active FROM rb_roulette_chats WHERE chat_id=$1", msg.chat.id)
    if not row or not row["active"]:
        return await ui.reply(msg, "Рулетка тут и так не включена.")
    await db.pool().execute(
        "UPDATE rb_roulette_chats SET active=FALSE, deactivated_at=now(), deactivated_by=$1 "
        "WHERE chat_id=$2", msg.from_user.id, msg.chat.id)
    await db.audit(msg.from_user.id, "roulette_off", {"chat_id": msg.chat.id})
    await ui.reply(msg, "🎰 Рулетка выключена. Включить обратно: /шимм")


# {tg_id: когда последний раз сказали «уже крутил»}. Инстанс один, так что памяти хватит.
_nagged: dict[int, float] = {}


def _should_nag(uid: int) -> bool:
    """True — можно ответить. False — молчим, человек спамит."""
    now = time.monotonic()
    last = _nagged.get(uid, 0)
    if now - last < SPIN_NAG_COOLDOWN:
        return False
    _nagged[uid] = now
    if len(_nagged) > 5000:  # не даём словарю расти бесконечно
        cutoff = now - SPIN_NAG_COOLDOWN
        for k in [k for k, v in _nagged.items() if v < cutoff]:
            _nagged.pop(k, None)
    return True


class BudgetExhausted(Exception):
    pass


class ChatBlocked(Exception):
    pass


async def _charge_budget(conn, chat_id: int, title: str, cost: int) -> None:
    """
    Списать cost (в грибах) с суточного бюджета рулетки.
    Бросает ChatBlocked, если чат НЕ одобрен через /шимм, или BudgetExhausted.

    Рулетка теперь полностью отвязана от рефералки: работает ТОЛЬКО в чатах из
    rb_roulette_chats (одобренных главным админом). Нет чата в списке — не крутим,
    чтобы не платить за грибы в чужих чатах, о которых владелец бота не знает.
    """
    approved = await conn.fetchrow(
        "SELECT active FROM rb_roulette_chats WHERE chat_id=$1", chat_id)
    if not approved or not approved["active"]:
        raise ChatBlocked

    # общий суточный потолок-предохранитель на все одобренные чаты вместе
    await conn.execute(
        "INSERT INTO rb_roulette_budget (day) VALUES (CURRENT_DATE) ON CONFLICT DO NOTHING")
    spent = await conn.fetchval(
        "SELECT spent_mush FROM rb_roulette_budget WHERE day=CURRENT_DATE FOR UPDATE")
    if spent + cost > ROULETTE_DAILY_BUDGET:
        raise BudgetExhausted
    await conn.execute(
        "UPDATE rb_roulette_budget SET spent_mush = spent_mush + $1 WHERE day=CURRENT_DATE", cost)
    await conn.execute(
        "UPDATE rb_roulette_chats SET spins = spins + 1, last_spin_at = now(), "
        "title = COALESCE($2, title) WHERE chat_id = $1", chat_id, title or None)


def _match(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(t == c or t.startswith(c + " ") for c in SPIN_COMMANDS)


@router.message(F.chat.type.in_({"group", "supergroup"}), F.text.func(_match))
async def spin(msg: Message, bot: Bot):
    uid = msg.from_user.id
    log.info("СТАРТ spin: chat=%s user=%s msg=%s text=%r",
             msg.chat.id, uid, msg.message_id, msg.text)
    await db.upsert_user(uid, msg.from_user.username, msg.from_user.first_name)

    if await db.is_banned(uid):
        log.info("  -> забанен, выход")
        return await ui.reply(msg, "🚫 Ты заблокирован в системе.")

    user = await db.get_user(uid)
    cur = user["currency"]
    today = date.today()

    # оформление берём из админки (эмодзи + премиум), а не из config
    e_cur = await settings.emoji(cur)
    e_rou = await settings.emoji("roulette")
    label = await settings.label(cur)
    em = await settings.emoji_map()

    # ---------- ДЕШЁВЫЕ ПРОВЕРКИ ДО ОЧЕРЕДИ ----------
    # В очередь попадает ТОЛЬКО прокрутка, которая реально состоится. Кто уже
    # крутил / чат не одобрен — отвечаем сразу, место в очереди не занимаем,
    # иначе такой игрок тормозит тех, кто крутит впервые, и ждёт ради отказа.

    # чат одобрен для рулетки?
    approved = await db.pool().fetchval(
        "SELECT active FROM rb_roulette_chats WHERE chat_id=$1", msg.chat.id)
    if not approved:
        log.info("  -> чат %s НЕ одобрен (/шимм), выход", msg.chat.id)
        return  # рулетка тут не включена (/шимм) — молчим

    # уже крутил сегодня? (быстрый предварительный отсев; финальная гарантия —
    # UNIQUE(tg_id, spin_day) внутри транзакции, от гонки)
    if uid not in UNLIMITED_SPIN_IDS:
        prev = await db.pool().fetchval(
            "SELECT amount FROM rb_spins WHERE tg_id=$1 AND spin_day=$2", uid, today)
        if prev is not None:
            log.info("  -> уже крутил сегодня (%s), выход", prev)
            if not _should_nag(uid):
                return
            return await ui.reply(
                msg, f"{e_rou} Ты уже крутил сегодня — выпало <b>{prev:,}</b> {e_cur}.\n"
                     f"Прокрутка одна в сутки. Возвращайся завтра.".replace(",", " "))

    amount = roulette.roll(cur)

    # ---------- ОЧЕРЕДЬ НА ЧАТ ----------
    # Пока в чате крутится чья-то анимация, ставим ждущему реакцию ожидания.
    # Реакцию снимаем, как только очередь дошла (зашли в слот). Проверки уже
    # пройдены выше, так что сюда встаёт только реальная прокрутка.
    pos = spin_queue.position(msg.chat.id)
    log.info("  позиция в очереди чата %s = %s (>1 значит кто-то уже крутится)",
             msg.chat.id, pos)
    wait_set = False
    if pos > 1:
        wait_set = await reactions.set_wait(msg.chat.id, msg.message_id, bot)
        log.info("  реакция ожидания поставлена=%s на msg=%s", wait_set, msg.message_id)

    log.info("  жду слот очереди chat=%s user=%s", msg.chat.id, uid)
    async with spin_queue.QueueSlot(msg.chat.id):
        log.info("  ВОШЁЛ в слот chat=%s user=%s — кручу", msg.chat.id, uid)
        if wait_set:
            await reactions.clear(msg.chat.id, msg.message_id, bot)
            log.info("  реакция снята с msg=%s", msg.message_id)

        try:
            async with db.pool().acquire() as conn:
                async with conn.transaction():
                    cost = amount // COIN_RATE if cur == "coins" else amount
                    await _charge_budget(conn, msg.chat.id, msg.chat.title or "", cost)

                    if uid in UNLIMITED_SPIN_IDS:
                        # UNIQUE(tg_id, spin_day) не обходим — сносим сегодняшнюю строку
                        # и пишем заново. Индекс остаётся боевым для всех остальных.
                        await conn.execute(
                            "DELETE FROM rb_spins WHERE tg_id=$1 AND spin_day=$2", uid, today)

                    spin_id = await conn.fetchval(
                        "INSERT INTO rb_spins (tg_id, chat_id, currency, amount, spin_day) "
                        "VALUES ($1,$2,$3,$4,$5) RETURNING id",
                        uid, msg.chat.id, cur, amount, today)
                    total = await db.apply(conn, uid, cur, amount, "roulette",
                                           f"spin:{spin_id}", spin_id)
        except asyncpg.UniqueViolationError:
            # проскочил предварительную проверку (гонка) — крутил всё же сегодня
            if not _should_nag(uid):
                return
            last = await db.pool().fetchval(
                "SELECT amount FROM rb_spins WHERE tg_id=$1 AND spin_day=$2", uid, today)
            return await ui.reply(
                msg, f"{e_rou} Ты уже крутил сегодня — выпало <b>{last:,}</b> {e_cur}.\n"
                     f"Прокрутка одна в сутки. Возвращайся завтра.".replace(",", " "))
        except BudgetExhausted:
            # прокрутка НЕ засчитана — транзакция откатилась, завтра крутанёт
            return await ui.reply(msg, "🧯 Суточный лимит выплат в этом чате исчерпан.\n"
                                   "Прокрутка не потрачена — заходи завтра.")
        except ChatBlocked:
            return  # чат выключили между проверкой и слотом — молчим

        await _animate(msg, e_rou, e_cur, label, amount, total, em)

async def _animate(msg, e_rou, e_cur, label, amount, total, em):
    # анимация. Баланс уже начислен в транзакции выше — что бы ни случилось с
    # сообщениями (флуд-контроль), выигрыш не потеряется. Кадры необязательны,
    # важна финальная карточка.
    #
    # Шкала заполняется за один проход (кадры 0..BAR_LEN), затем сразу карточка —
    # без второго круга и без лишней паузы после заполнения. Карточка приходит
    # на том же интервале, что и кадры.
    card = roulette.result_card(msg.from_user.first_name, amount, e_cur, label, total, e_rou)
    m = None
    with contextlib.suppress(Exception):
        m = await r_reply(msg, roulette.frame(0, e_rou), em)
    if m is not None:
        for i in range(1, ANIM_FRAMES):
            await asyncio.sleep(ANIM_DELAY)
            with contextlib.suppress(Exception):
                await r_edit(m, roulette.frame(i, e_rou), em)
        # шкала полна — результат на том же интервале, без отдельной задержки
        await asyncio.sleep(ANIM_DELAY)
        with contextlib.suppress(Exception):
            await r_edit(m, card, em)
    else:
        # стартовый кадр не ушёл (флуд) — отдадим хотя бы результат, тоже без падения
        with contextlib.suppress(Exception):
            await r_reply(msg, card, em)


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
