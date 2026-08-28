"""
Камень-ножницы-бумага: лобби (набор игроков), старт, раунды.
Мультиплеер до 5 человек на выбывание. Команда: !шайн кнб <ставка> <валюта>.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, rps, settings, ui
from services.amount_parse import parse_amount

router = Router()

GAME = "rps"
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
    return (f"✊✋✌️ <b>Камень-ножницы-бумага</b>\n\n"
            f"Ставка: <b>{_fmt(m['stake'])}</b> {e} · банк <b>{_fmt(m['stake']*len(players))}</b> {e}\n"
            f"Участников: <b>{len(players)}</b> (до 5)\n"
            f"{names}\n\n"
            f"Жми «Участвовать» — ставка замораживается сразу.\n"
            f"Создатель жмёт «Старт», когда все в сборе (до 15 мин).")


async def _lobby_kb(mid: int):
    from services.ui import btn
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Участвовать", f"rps_join:{mid}")
    await btn(kb, "▶️ Старт", f"rps_start:{mid}")
    await btn(kb, "❌ Отменить", f"rps_cancel:{mid}")
    kb.adjust(1, 2)
    return kb.as_markup()


# ---------------- запуск через !шайн кнб ----------------
async def handle_shine_rps(msg: Message, parts: list[str]):
    """Вызывается из cmd_shine, когда игра = кнб. parts — слова после '!шайн'."""
    if not await _guard_casino(msg.from_user.id):
        return
    bet = parse_amount(parts[1]) if len(parts) > 1 else None
    cur = None
    for w in parts[2:]:
        if w.lower() in _CUR_WORDS:
            cur = _CUR_WORDS[w.lower()]
            break
    if bet is None or bet <= 0 or cur is None:
        return await ui.reply(msg, "Формат: <code>!шайн кнб ставка валюта</code>. "
                                   "Пример: <code>!шайн кнб 5000 грибы</code>")
    uid = msg.from_user.id
    if await matches.has_active(uid):
        return await ui.reply(msg, "У тебя уже есть активная игра. Заверши или отмени её.")
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


# ---------------- участвовать / отмена ----------------
@router.callback_query(F.data.startswith("rps_join:"))
async def cb_rps_join(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.join_lobby(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Ты в игре! Ставка заморожена.")
    await _refresh_lobby(c.bot, mid)


@router.callback_query(F.data.startswith("rps_cancel:"))
async def cb_rps_cancel(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.cancel_lobby(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Игра отменена, ставки возвращены.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Игра отменена создателем. Ставки возвращены.",
                                  reply_markup=None)


# ---------------- старт (переход к раундам — Этап 3) ----------------
@router.callback_query(F.data.startswith("rps_start:"))
async def cb_rps_start(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] != "lobby":
        return await c.answer("Лобби уже закрыто.", show_alert=True)
    if c.from_user.id != m["p1"]:
        return await c.answer("Только создатель может начать игру.", show_alert=True)
    players = await matches.lobby_players(mid)
    if len(players) < 2:
        return await c.answer("Нужно минимум 2 игрока.", show_alert=True)
    await c.answer("Игра начинается!")
    from handlers.rps_game import start_round
    with contextlib.suppress(Exception):
        await start_round(c.bot, mid, first=True)


# ---------------- ТЕСТ Rich Messages ----------------
@router.message(F.text.lower() == "!шайнтест")
async def cmd_rich_test(msg: Message):
    import logging
    log = logging.getLogger("refbot")
    log.info("!шайнтест от %s в чате %s", msg.from_user.id, msg.chat.id)
    from services import richmsg
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    # fallback-клавиатура на случай отката
    fb = InlineKeyboardBuilder()
    fb.button(text="⚔️ Вступить #1", callback_data="rps_test:1")
    fb.button(text="⚔️ Вступить #2", callback_data="rps_test:2")
    fb.adjust(1)
    rows = [
        ("🎮 <b>Тестовая игра #1</b> — ставка 1000 🍄", [("⚔️ Вступить", "rps_test:1")]),
        ("🎮 <b>Тестовая игра #2</b> — ставка 5000 🪙", [("⚔️ Вступить", "rps_test:2")]),
    ]
    try:
        m, is_rich = await richmsg.send_rich(
            msg.bot, msg.chat.id, "✊✋✌️ <b>Открытые игры (тест)</b>", rows,
            fallback_kb=fb.as_markup())
        log.info("!шайнтест: отправлено, is_rich=%s", is_rich)
        await msg.reply(f"Отправил. Rich-режим: {'ДА ✅' if is_rich else 'НЕТ (откат на инлайн) ⚠️'}")
    except Exception as e:
        log.exception("!шайнтест упал")
        await msg.reply(f"Ошибка: {type(e).__name__}: {e}")


@router.callback_query(F.data.startswith("rps_test:"))
async def cb_rps_test(c: CallbackQuery):
    await c.answer(f"Кнопка работает! Ты нажал игру #{c.data.split(':')[1]}", show_alert=True)
