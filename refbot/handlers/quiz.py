"""
Викторина: мультиплеер до 10 игроков. Команда: !викторина <ставка> <валюта>.
Каждый ставит ставку, создатель запускает. 25 вопросов A/B/C/D, 30 сек на ответ.
Больше правильных — победа, ничья с проигравшим — делёж банка минус 3%.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, settings, ui
from services.amount_parse import parse_amount

router = Router()

GAME = "quiz"
QUESTIONS_PER_GAME = 25
_CUR_WORDS = {
    "грибы": "mushrooms", "гриб": "mushrooms", "грибов": "mushrooms",
    "коины": "coins", "коинов": "coins", "коин": "coins",
}


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


async def _guard_casino(uid: int) -> bool:
    from services import casino as casino_svc
    return await casino_svc.visible(uid)


async def _lobby_text(mid: int) -> str:
    m = await matches.get(mid)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    players = await matches.lobby_players(mid)
    names = ", ".join([await _name(p["tg_id"]) for p in players])
    return (f"🧠 <b>Викторина</b>\n\n"
            f"Ставка: <b>{_fmt(m['stake'])}</b> {e} · банк <b>{_fmt(m['stake']*len(players))}</b> {e}\n"
            f"Участников: <b>{len(players)}</b> (до 10)\n{names}\n\n"
            f"{QUESTIONS_PER_GAME} вопросов, 30 сек на каждый. Больше правильных — победа "
            f"(ничья — делёж).\n\n"
            f"Жми «Участвовать» — ставка замораживается сразу. Создатель жмёт «Старт».")


async def _lobby_kb(mid: int):
    from services.ui import btn
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Участвовать", f"quiz_join:{mid}")
    await btn(kb, "▶️ Старт", f"quiz_start:{mid}")
    await btn(kb, "❌ Отменить", f"quiz_cancel:{mid}")
    kb.adjust(1, 2)
    return kb.as_markup()


@router.message(F.text.lower().startswith(("!викторина", "/викторина")))
async def cmd_quiz(msg: Message):
    if not await _guard_casino(msg.from_user.id):
        return
    parts = (msg.text or "").split()
    bet = parse_amount(parts[1]) if len(parts) > 1 else None
    cur = None
    for w in parts[2:]:
        if w.lower() in _CUR_WORDS:
            cur = _CUR_WORDS[w.lower()]
            break
    if bet is None or bet <= 0 or cur is None:
        return await ui.reply(msg, "Формат: <code>!викторина ставка валюта</code>. "
                                   "Пример: <code>!викторина 1000 грибы</code>")
    uid = msg.from_user.id
    if await matches.has_active(uid):
        return await ui.reply(msg, "У тебя уже есть активная игра. Заверши или отмени её.")
    # хватает ли вопросов в базе
    total_q = await db.pool().fetchval("SELECT count(*) FROM rb_quiz WHERE active")
    if not total_q or total_q < QUESTIONS_PER_GAME:
        return await ui.reply(msg, "В базе пока мало вопросов для викторины. Загляни позже.")
    b = await db.balances(uid)
    if b.get(cur, 0) < bet:
        sx = await settings.ctx()
        return await ui.reply(msg, f"Не хватает: на балансе {_fmt(b[cur])} {sx['e_' + cur]}.")

    mid, err = await matches.create_lobby(GAME, uid, cur, bet, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    sent = await ui.send(msg.bot, msg.chat.id, await _lobby_text(mid),
                         reply_markup=await _lobby_kb(mid))
    if sent:
        await db.pool().execute(
            "UPDATE rb_matches SET board_chat_id=$1, board_msg_id=$2 WHERE id=$3",
            msg.chat.id, sent.message_id, mid)


async def _refresh_lobby(bot, mid: int):
    m = await matches.get(mid)
    if not m or not m.get("board_msg_id"):
        return
    from services import render
    em = await settings.emoji_map()
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, m["board_chat_id"], m["board_msg_id"],
                                await _lobby_text(mid), em, reply_markup=await _lobby_kb(mid))


@router.callback_query(F.data.startswith("quiz_join:"))
async def cb_quiz_join(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.join_lobby(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Ты в игре! Ставка заморожена.")
    await _refresh_lobby(c.bot, mid)


@router.callback_query(F.data.startswith("quiz_cancel:"))
async def cb_quiz_cancel(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.cancel_lobby(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Викторина отменена, ставки возвращены.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Викторина отменена создателем. Ставки возвращены.",
                                  reply_markup=None)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    for uid in m.get("refunded", []):
        if uid == c.from_user.id:
            continue
        with contextlib.suppress(Exception):
            await ui.send(c.bot, uid,
                f"❌ Викторина отменена создателем.\nСтавка <b>{_fmt(m['stake'])}</b> {e} возвращена.")


@router.callback_query(F.data.startswith("quiz_start:"))
async def cb_quiz_start(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] != "lobby":
        return await c.answer("Лобби уже закрыто.", show_alert=True)
    if c.from_user.id != m["p1"]:
        return await c.answer("Только создатель может начать.", show_alert=True)
    players = await matches.lobby_players(mid)
    if len(players) < 2:
        return await c.answer("Нужно минимум 2 игрока.", show_alert=True)
    await c.answer("Викторина начинается!")
    from handlers.quiz_game import start_quiz
    with contextlib.suppress(Exception):
        await start_quiz(c.bot, mid)


@router.callback_query(F.data.startswith("quiz_again:"))
async def cb_quiz_again(c: CallbackQuery):
    _, cur, stake_s = c.data.split(":")
    stake = int(stake_s)
    uid = c.from_user.id
    if not await _guard_casino(uid):
        return await c.answer("Казино закрыто.", show_alert=True)
    if await matches.has_active(uid):
        return await c.answer("У тебя уже есть активная игра.", show_alert=True)
    b = await db.balances(uid)
    if b.get(cur, 0) < stake:
        sx = await settings.ctx()
        return await c.answer(f"Не хватает: {_fmt(b.get(cur,0))} {sx['e_'+cur]}.", show_alert=True)
    mid, err = await matches.create_lobby(GAME, uid, cur, stake, c.message.chat.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Новая викторина создана!")
    sent = await ui.send(c.bot, c.message.chat.id, await _lobby_text(mid),
                         reply_markup=await _lobby_kb(mid))
    if sent:
        await db.pool().execute(
            "UPDATE rb_matches SET board_chat_id=$1, board_msg_id=$2 WHERE id=$3",
            c.message.chat.id, sent.message_id, mid)
    with contextlib.suppress(Exception):
        await c.message.edit_reply_markup(reply_markup=None)
