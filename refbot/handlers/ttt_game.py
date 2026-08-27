"""
Крестики-нолики: игровое поле, ходы, синхронизация.

Синхронизация:
  - ЛС-режим: у каждого игрока своё сообщение с полем (p1_msg_id, p2_msg_id).
    При ходе редактируем ОБА (у обоих обновляется доска и «чей ход»).
  - Чат-режим (Этап 5): одно общее сообщение (board_msg_id) в чате.
Допуск: ходить может только тот, чей сейчас turn. Чужой/не в свой ход — блок.
Тайм-аут на ход (Этап 6) отслеживает фоновый воркер по move_deadline.
"""
import contextlib
import json

from aiogram import F, Router
from aiogram.types import CallbackQuery

import db
from services import matches, ttt, settings, ui

router = Router()


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


async def _board_text(m: dict, state: dict, for_uid=None) -> str:
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    stake = _fmt(m["stake"])
    win = ttt.winner(state)
    p1, p2 = m["p1"], m["p2"]
    n1, n2 = await _name(p1), await _name(p2)
    s1 = ttt.SYM[ttt.symbol_of(state, p1)]
    s2 = ttt.SYM[ttt.symbol_of(state, p2)]
    head = (f"❌⭕️ <b>Крестики-нолики</b>\n"
            f"{s1} {n1}  vs  {s2} {n2}\n"
            f"Ставка: <b>{stake}</b> {e} · банк <b>{_fmt(m['stake']*2)}</b> {e}\n\n")
    if win is None:
        turn = m["turn"]
        who = "Твой ход" if (for_uid is not None and turn == for_uid) else f"Ходит {await _name(turn)}"
        sym = ttt.SYM[ttt.symbol_of(state, turn)]
        return head + f"{sym} <b>{who}</b>"
    if win == 0:
        return head + "🤝 <b>Ничья!</b> Ставки возвращены."
    wname = "Ты" if (for_uid is not None and win == for_uid) else await _name(win)
    return head + f"🏆 <b>{wname} победил(а)!</b>"


async def _kb(m: dict, state: dict, finished: bool):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    if not finished:
        return await ttt.board_kb(state, m["id"])
    kb = InlineKeyboardBuilder()
    kb.button(text="🔁 Ещё раз", callback_data=f"ttt_again:{m['id']}")
    kb.button(text="Меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


async def start_game(bot, mid: int):
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return
    state = ttt.new_game(m["p1"], m["p2"])
    await matches.save_state(mid, state, state["first"])
    m = await matches.get(mid)
    for uid, col in ((m["p1"], "p1_msg_id"), (m["p2"], "p2_msg_id")):
        text = await _board_text(m, state, for_uid=uid)
        with contextlib.suppress(Exception):
            msg = await ui.send(bot, uid, text, reply_markup=await _kb(m, state, False))
            if msg:
                await db.pool().execute(
                    f"UPDATE rb_matches SET {col}=$1 WHERE id=$2", msg.message_id, mid)


async def _sync(bot, m: dict, state: dict, finished: bool):
    from services import render, settings as st
    em = await st.emoji_map()
    for uid, col in ((m["p1"], "p1_msg_id"), (m["p2"], "p2_msg_id")):
        msg_id = m.get(col)
        if not msg_id:
            continue
        text = await _board_text(m, state, for_uid=uid)
        with contextlib.suppress(Exception):
            await render.edit_by_id(bot, uid, msg_id, text, em,
                                    reply_markup=await _kb(m, state, finished))


@router.callback_query(F.data.startswith("ttt_mv:"))
async def cb_move(c: CallbackQuery):
    _, mid_s, cell_s = c.data.split(":")
    mid, cell = int(mid_s), int(cell_s)
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return await c.answer("Игра завершена.", show_alert=True)
    uid = c.from_user.id
    if uid not in (m["p1"], m["p2"]):
        return await c.answer("Это не твоя игра.", show_alert=True)
    if m["turn"] != uid:
        return await c.answer("Сейчас не твой ход.", show_alert=True)

    state = m["state"] if isinstance(m["state"], dict) else json.loads(m["state"])
    okm, err = ttt.apply_move(state, uid, cell)
    if err:
        return await c.answer(err, show_alert=False)

    win = ttt.winner(state)
    if win is None:
        nxt = ttt.other(state, uid)
        await matches.save_state(mid, state, nxt)
        m = await matches.get(mid)
        await _sync(c.bot, m, state, finished=False)
        await _sync_chat(c.bot, m, state, finished=False)
        return await c.answer()

    fin_m, _ = await matches.finish(mid, None if win == 0 else win)
    await db.pool().execute("UPDATE rb_matches SET state=$1::jsonb WHERE id=$2",
                            json.dumps(state), mid)
    m = await matches.get(mid)
    await _sync(c.bot, m, state, finished=True)
    await _sync_chat(c.bot, m, state, finished=True)
    await c.answer("Игра окончена!" if win != 0 else "Ничья!")


async def start_game_chat(bot, mid: int, chat_id: int):
    """Запуск игры с ОДНИМ общим полем в чате (все видят, жмут по очереди)."""
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return
    state = ttt.new_game(m["p1"], m["p2"])
    await matches.save_state(mid, state, state["first"])
    m = await matches.get(mid)
    text = await _board_text(m, state, for_uid=None)
    with contextlib.suppress(Exception):
        msg = await ui.send(bot, chat_id, text, reply_markup=await _kb(m, state, False))
        if msg:
            await db.pool().execute(
                "UPDATE rb_matches SET board_chat_id=$1, board_msg_id=$2 WHERE id=$3",
                chat_id, msg.message_id, mid)


async def _sync_chat(bot, m: dict, state: dict, finished: bool):
    """Обновить общее поле в чате."""
    if not m.get("board_msg_id"):
        return
    from services import render, settings as st
    em = await st.emoji_map()
    text = await _board_text(m, state, for_uid=None)
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, m["board_chat_id"], m["board_msg_id"], text, em,
                                reply_markup=await _kb(m, state, finished))


async def timeout_worker(bot):
    """Фоновая проверка тайм-аутов матчей: поиск (10 мин) и ход (5 мин)."""
    import asyncio
    log = __import__("logging").getLogger("refbot")
    log.info("ttt timeout worker запущен")
    while True:
        try:
            # просроченные поиски — вернуть ставку, убрать сообщение-вызов
            for m in await matches.expire_searches():
                with contextlib.suppress(Exception):
                    await ui.send(bot, m["p1"],
                        "⏳ Соперник не нашёлся за 10 минут. Ставка возвращена.")
            # просроченные ходы — поражение зевнувшему
            for m in await matches.expire_moves():
                state = m["state"] if isinstance(m["state"], dict) else __import__("json").loads(m["state"])
                fresh = await matches.get(m["id"])
                # синк финального поля (чат + лички)
                with contextlib.suppress(Exception):
                    await _sync(bot, fresh, state, finished=True)
                    await _sync_chat(bot, fresh, state, finished=True)
                # уведомления
                sx = await settings.ctx()
                e = sx["e_" + m["currency"]]
                payout = _fmt(m["stake"] * 2 - int(m["stake"] * 0.03))
                with contextlib.suppress(Exception):
                    await ui.send(bot, m["loser"],
                        "⏳ Ты не сходил за 5 минут — засчитано поражение.")
                with contextlib.suppress(Exception):
                    await ui.send(bot, m["winner"],
                        f"⏳ Соперник просрочил ход — победа тебе! +{payout} {e}")
        except Exception as e:
            log.warning("ttt timeout worker: %s", e)
        await __import__("asyncio").sleep(20)
