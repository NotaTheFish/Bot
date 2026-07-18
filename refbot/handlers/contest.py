"""
Еженедельный конкурс: привязка чата, счётчик, /неделя, розыгрыш.

Счётчик сделан ВНЕШНИМ middleware, а не хендлером: обычный catch-all хендлер
перехватил бы апдейт и заблокировал всё, что зарегистрировано ниже (рулетку,
команды). Middleware считает и пропускает дальше.
"""
import asyncio
import contextlib
from datetime import timedelta
import logging
from html import escape
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware, Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
import roulette
from config import (ANIM_DELAY, ANIM_FRAMES, CONTEST_COUNT_PARTIAL_FIRST, CONTEST_MIN_MSGS,
                    CONTEST_MSGS_PER_TICKET, CONTEST_PRIZE, CONTEST_TEST_MINUTES, SUPER_ADMINS)
from config import CONTEST_TZ
from services import contest, settings, ui
from services.render import edit as r_edit
from services.ui import btn

router = Router()
log = logging.getLogger(__name__)

# Короткий ответ в чат — без инструкций по БД (их видит весь чат). Подробности в лог.
NO_TABLES = "⚙️ Конкурс ещё настраивается. Загляни попозже."


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _name(r) -> str:
    if r.get("username"):
        return f"@{r['username']}"
    return escape(r.get("first_name") or str(r["user_id"]))


async def _chat_admin(bot, chat_id: int, uid: int) -> bool:
    if uid in SUPER_ADMINS:
        return True
    try:
        m = await bot.get_chat_member(chat_id, uid)
        return m.status in ("creator", "administrator")
    except Exception:
        return False


# ==================== СЧЁТЧИК ====================
class CounterMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable[[Message, dict], Awaitable[Any]],
                       event: Message, data: dict) -> Any:
        try:
            u = event.from_user
            # is_contest_chat сам вернёт False, если таблиц нет: тогда ни запросов,
            # ни трейсбека в лог на каждое сообщение
            if (u and not u.is_bot
                    and event.chat.type in ("group", "supergroup")
                    and await contest.is_contest_chat(event.chat.id)):
                await contest.count_message(event.chat.id, u.id)
                await db.upsert_user(u.id, u.username, u.first_name)
        except Exception:
            log.exception("счётчик конкурса упал — пропускаю сообщение дальше")
        return await handler(event, data)


# ==================== ПРИВЯЗКА ====================
@router.message(Command("шимшайнуть", "cbind"), F.chat.type.in_({"group", "supergroup"}))
async def cbind(msg: Message):
    if not await _chat_admin(msg.bot, msg.chat.id, msg.from_user.id):
        return await ui.reply(msg, "🚫 Только админ чата.")
    await db.upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    if not await contest.bind(msg.chat.id, msg.chat.title or "", msg.from_user.id):
        log.error("НЕТ ТАБЛИЦ КОНКУРСА — накати setup.sql. /шимшайнуть не сработал в %s",
                  msg.chat.id)
        return await ui.reply(msg, NO_TABLES)
    await db.audit(msg.from_user.id, "contest_bind", {"chat_id": msg.chat.id})
    s, e = contest.period_bounds()
    sx = await settings.ctx()
    per = "неделя, пн 00:00 → вс 23:59"
    # когда пройдёт первый розыгрыш
    if CONTEST_TEST_MINUTES or CONTEST_COUNT_PARTIAL_FIRST:
        first = e
        note = ""
    else:
        # текущий период неполный -> первый розыгрыш по итогам СЛЕДУЮЩего
        _, first = contest.period_bounds(e + timedelta(seconds=1))
        note = ("\n\n⚠️ Чат привязан в середине недели — первый конкурс пройдёт "
                "по итогам <b>следующей полной недели</b>, чтобы счёт был честным.")
    await ui.reply(
        msg,
        f"{sx['e_paid']} <b>Конкурс активности подключён</b>\n\n"
        f"📊 Период: {per}\n"
        f"🎫 От <b>{CONTEST_MIN_MSGS}</b> сообщений — участие, "
        f"1 билет за каждые <b>{CONTEST_MSGS_PER_TICKET}</b>\n"
        f"{sx['e_roulette']} Приз: <b>{fmt(CONTEST_PRIZE['mushrooms'])}</b> "
        f"{sx['e_mushrooms']} или <b>{fmt(CONTEST_PRIZE['coins'])}</b> {sx['e_coins']}\n"
        f"🏁 Первый розыгрыш: {first.astimezone(contest.TZ):%d.%m %H:%M}\n\n"
        f"Статистика: /неделя\nОтключить: /отшимшайнуть{note}\n\n"
        f"<i>На рефералку это не влияет — она отдельно, через /шайнуть.</i>")


@router.message(Command("отшимшайнуть", "cunbind"), F.chat.type.in_({"group", "supergroup"}))
async def cunbind(msg: Message):
    if not await _chat_admin(msg.bot, msg.chat.id, msg.from_user.id):
        return await ui.reply(msg, "🚫 Только админ чата.")
    if not await contest.tables_ready():
        return await ui.reply(msg, NO_TABLES)
    row = await db.pool().fetchrow(
        "SELECT * FROM rb_contest_chats WHERE chat_id=$1", msg.chat.id)
    if not row or not row["active"]:
        return await ui.reply(msg, "Конкурс тут и не был подключён.")
    await contest.unbind(msg.chat.id, msg.from_user.id)
    await db.audit(msg.from_user.id, "contest_unbind", {"chat_id": msg.chat.id})
    await ui.reply(msg, "⚪️ Конкурс отключён. Счётчики сохранены — "
                        "неразыгранные периоды можно добить после /шимшайнуть.")


# ==================== /неделя ====================
@router.message(Command("неделя", "week"), F.chat.type.in_({"group", "supergroup"}))
async def week(msg: Message):
    if not await contest.tables_ready():
        return await ui.reply(msg, NO_TABLES)
    if not await contest.is_contest_chat(msg.chat.id):
        return
    s, e = contest.period_bounds()
    rows = await contest.standings(msg.chat.id, s)
    sx = await settings.ctx()
    if not rows:
        return await ui.reply(msg, "📊 За этот период сообщений пока нет.")

    lines = []
    for i, r in enumerate(rows[:25], 1):
        mark = (f"🎫 {r['tickets']}" if r["tickets"]
                else f"ещё {max(0, CONTEST_MIN_MSGS - r['msgs'])} до участия")
        lines.append(f"{i}. {_name(r)} — <b>{r['msgs']}</b> ({mark})")

    me = next((r for r in rows if r["user_id"] == msg.from_user.id), None)
    mine = ""
    if me:
        pos = rows.index(me) + 1
        mine = (f"\n\n{sx['e_profile']} Ты: <b>{me['msgs']}</b> сообщений, место {pos}, "
                f"билетов: <b>{me['tickets']}</b>")
        if not me["tickets"]:
            mine += (f"\n<i>Ещё {max(0, CONTEST_MIN_MSGS - me['msgs'])} сообщений — "
                     f"и ты в конкурсе</i>")

    players = [r for r in rows if r["tickets"]]
    total = sum(r["tickets"] for r in players)
    await ui.reply(
        msg,
        f"📊 <b>Активность за период</b>\n"
        f"<i>до {e.astimezone(contest.TZ):%d.%m %H:%M}</i>\n\n"
        + "\n".join(lines) +
        f"\n\n🎫 Участников: <b>{len(players)}</b>, билетов всего: <b>{total}</b>"
        f"{mine}\n\n"
        f"<i>От {CONTEST_MIN_MSGS} сообщений — участие. "
        f"1 билет за каждые {CONTEST_MSGS_PER_TICKET}. "
        f"Больше билетов — выше шанс, но не гарантия.</i>")


# ==================== АНОНС ====================
async def announce(bot: Bot, chat, draw):
    """Постим итог периода, снимаем СВОЙ прошлый закреп, закрепляем свежий."""
    sx = await settings.ctx()
    s = draw["period_start"].astimezone(contest.TZ)
    e = draw["period_end"].astimezone(contest.TZ)

    if draw["status"] == "empty":
        await ui.send(bot, chat["chat_id"],
                      f"🏁 <b>Период завершён</b> ({s:%d.%m} — {e:%d.%m})\n\n"
                      f"Никто не набрал {CONTEST_MIN_MSGS} сообщений — конкурс не состоялся.\n"
                      f"Новый период уже идёт: /неделя")
        return

    kb = InlineKeyboardBuilder()
    await btn(kb, "Запустить рулетку", f"draw:{draw['id']}", "roulette")
    m = await ui.send(
        bot, chat["chat_id"],
        f"{sx['e_roulette']} <b>НЕДЕЛЯ ЗАВЕРШЕНА</b>\n"
        f"<i>{s:%d.%m %H:%M} — {e:%d.%m %H:%M}</i>\n\n"
        f"🎫 Участников: <b>{draw['players']}</b>\n"
        f"🎟 Билетов разыграно: <b>{draw['tickets_total']}</b>\n\n"
        f"Победителя определит рулетка. Больше билетов — выше шанс, "
        f"но решает случай.\n\n"
        f"<i>Розыгрыш запускает администратор.</i>",
        reply_markup=kb.as_markup())

    await db.pool().execute(
        "UPDATE rb_week_draws SET announce_msg_id=$1 WHERE id=$2", m.message_id, draw["id"])

    # снимаем ТОЛЬКО свой прошлый закреп, чужие не трогаем
    if chat["pinned_msg_id"]:
        with contextlib.suppress(Exception):
            await bot.unpin_chat_message(chat["chat_id"], chat["pinned_msg_id"])
    try:
        await bot.pin_chat_message(chat["chat_id"], m.message_id, disable_notification=False)
        await db.pool().execute(
            "UPDATE rb_contest_chats SET pinned_msg_id=$1 WHERE chat_id=$2",
            m.message_id, chat["chat_id"])
    except Exception as ex:
        log.warning("не смог закрепить в %s: %s", chat["chat_id"], ex)


# ==================== РОЗЫГРЫШ ====================
@router.callback_query(F.data.startswith("draw:"))
async def do_draw(c: CallbackQuery):
    did = int(c.data.split(":")[1])
    draw = await db.pool().fetchrow("SELECT * FROM rb_week_draws WHERE id=$1", did)
    if not draw:
        return await c.answer("Розыгрыш не найден.", show_alert=True)
    if not await _chat_admin(c.bot, draw["chat_id"], c.from_user.id):
        return await c.answer("Кнопку жмёт админ чата.", show_alert=True)
    if draw["status"] != "pending":
        return await c.answer("Этот розыгрыш уже проведён.", show_alert=True)

    rows = await contest.standings(draw["chat_id"], draw["period_start"])
    pool = [(r["user_id"], r["tickets"]) for r in rows if r["tickets"] > 0]
    winner_id, wt = contest.pick_winner(pool)
    if not winner_id:
        await db.pool().execute("UPDATE rb_week_draws SET status='empty' WHERE id=$1", did)
        return await c.answer("Участников нет.", show_alert=True)

    wu = await db.get_user(winner_id)
    cur = wu["currency"] if wu else "mushrooms"
    amount = contest.prize(cur)

    # ---- фиксация результата: FOR UPDATE + идемпотентное начисление ----
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            d = await conn.fetchrow("SELECT * FROM rb_week_draws WHERE id=$1 FOR UPDATE", did)
            if d["status"] != "pending":
                return await c.answer("Уже проведён.", show_alert=True)
            bal = await db.apply(conn, winner_id, cur, amount, "contest",
                                 f"draw:{did}", did)
            await conn.execute(
                """
                UPDATE rb_week_draws SET status='drawn', winner_id=$1, winner_tickets=$2,
                       currency=$3, amount=$4, drawn_at=now(), drawn_by=$5
                WHERE id=$6
                """, winner_id, wt, cur, amount, c.from_user.id, did)
    await db.audit(c.from_user.id, "contest_draw",
                   {"draw": did, "winner": winner_id, "amount": amount})
    await c.answer("Крутим!")

    # ---- анимация как у !шайн ----
    sx = await settings.ctx()
    em = await settings.emoji_map()
    e_rou, e_cur = sx["e_roulette"], sx[f"e_{cur}"]
    with contextlib.suppress(Exception):
        await r_edit(c.message, roulette.frame(0, e_rou), em, reply_markup=None)
    for i in range(1, ANIM_FRAMES):
        await asyncio.sleep(ANIM_DELAY)
        with contextlib.suppress(Exception):
            await r_edit(c.message, roulette.frame(i, e_rou), em)
    await asyncio.sleep(ANIM_DELAY)

    name = (f"@{wu['username']}" if wu and wu["username"]
            else escape((wu["first_name"] if wu else None) or "Победитель"))
    total = sum(t for _, t in pool)
    chance = wt / total * 100 if total else 0

    kb = InlineKeyboardBuilder()
    url = (f"https://t.me/{wu['username']}" if wu and wu["username"]
           else f"tg://user?id={winner_id}")
    kb.button(text=f"👤 {name}", url=url)

    card = (
        f"{e_rou} <b>ПОБЕДИТЕЛЬ НЕДЕЛИ</b>\n"
        f"<blockquote>{e_cur}   ⟪ {e_cur} ⟫   {e_cur}</blockquote>\n"
        f'🏆 <a href="tg://user?id={winner_id}">{name}</a>\n'
        f"🎫 Билетов: <b>{wt}</b> из {total} (шанс был {chance:.1f}%)\n"
        f"🎁 Приз: <b>{fmt(amount)}</b> {e_cur} {sx[f'l_{cur}']}\n"
        f"💰 Баланс: <b>{fmt(bal or 0)}</b>\n\n"
        f"<i>Новый период уже идёт — /неделя</i>"
    )
    with contextlib.suppress(Exception):
        await r_edit(c.message, card, em, reply_markup=kb.as_markup())


# ==================== ПЛАНИРОВЩИК ====================
async def worker(bot: Bot):
    """
    Раз в минуту закрывает предыдущий период. Цикл вечный, пока чат активен.

    Ведём отсчёт ОТ ВРЕМЕНИ, а не от наличия сообщений. Раньше период искался
    через rb_week_msgs («есть строки, розыгрыша нет») — и в тихую неделю цикл
    просто останавливался, потому что периода как бы не существовало.
    Неделя заканчивается независимо от того, писал кто-нибудь или нет.

    Идемпотентность держится на UNIQUE (chat_id, period_start): сколько бы раз
    воркер ни проснулся за период, анонс будет ровно один.
    """
    while True:
        try:
            cur_start, _ = contest.period_bounds()
            prev_start, prev_end = contest.prev_period(cur_start)
            for chat in await contest.active_chats():
                # период, закончившийся ДО привязки чата, не наш
                if prev_end <= chat["created_at"]:
                    continue
                # период НАЧАЛСЯ до привязки => он неполный. По умолчанию пропускаем
                # (розыгрыш вышел бы по обрезанной неделе), если явно не разрешён.
                if prev_start < chat["created_at"] and not CONTEST_COUNT_PARTIAL_FIRST:
                    continue
                draw = await contest.create_draw(chat["chat_id"], prev_start)
                if not draw:
                    continue  # уже анонсирован
                await announce(bot, chat, draw)
        except Exception:
            log.exception("contest_worker упал, продолжаю")
        await asyncio.sleep(60)
