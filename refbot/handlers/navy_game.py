"""
Морской бой: боевая фаза.
Общее поле в чате — выстрелы того, чей ход (корабли врага скрыты). Кнопки координат.
Эфемерка «Мой флот» у каждого обновляется при попадании по нему.
Попал -> ходишь ещё. Промах -> ход сопернику.
"""
import contextlib
import json

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, navy, settings, ui, render

router = Router()

# состояние прицела стреляющего: (mid, uid) -> col (выбранный столбец, ждём строку)
_aim: dict[tuple, int] = {}
# id общего поля в чате: mid -> (chat_id, message_id)
_board: dict[int, tuple] = {}
# id эфемерок «мой флот»: (mid, uid) -> ephemeral_message_id
_myfleet: dict[tuple, int] = {}


async def _emo() -> dict:
    return {
        "water": await settings.emoji("navy_water"),
        "ship":  await settings.emoji("navy_ship"),
        "hit":   await settings.emoji("navy_hit"),
        "miss":  await settings.emoji("navy_miss"),
        "sunk":  await settings.emoji("navy_sunk"),
    }


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


def _fields(st: dict) -> dict:
    return st.get("fields", {})


def _opponent(m: dict, uid: int) -> int:
    return m["p2"] if uid == m["p1"] else m["p1"]


async def start_battle(bot, mid: int):
    """Начать бой: показать общее поле в чате + эфемерки «мой флот» обоим."""
    m = await matches.get(mid)
    if not m:
        return
    # общее поле в чат
    text, kb = await _board_view(mid)
    emo = await _emo()
    em = await settings.emoji_map()
    sent = await ui.send(bot, m["origin_chat"], text, reply_markup=kb)
    if sent:
        _board[mid] = (m["origin_chat"], sent.message_id)
    # эфемерки «мой флот» обоим
    for uid in (m["p1"], m["p2"]):
        await _send_myfleet(bot, m["origin_chat"], mid, uid)
    # уведомим, чей ход
    st = await matches.navy_state(mid)
    turn = m["turn"]
    with contextlib.suppress(Exception):
        await ui.send(bot, turn, "⚓ Твой ход! Стреляй по общему полю в чате.")


async def _board_view(mid: int):
    """Общее поле: выстрелы ТЕКУЩЕГО игрока по врагу (корабли скрыты) + кнопки."""
    m = await matches.get(mid)
    st = await matches.navy_state(mid)
    emo = await _emo()
    turn = m["turn"]
    opp = _opponent(m, turn)
    fields = _fields(st)
    opp_field = fields.get(str(opp)) or navy.new_field()
    grid = navy.render_shots_field(opp_field, emo)
    tname = await _name(turn)
    text = (f"⚓ <b>Морской бой</b>\n"
            f"{await _name(m['p1'])} vs {await _name(m['p2'])}\n\n"
            f"Ходит: <b>{tname}</b>\n\n{grid}\n\n"
            f"{tname}, выбери клетку для выстрела:")
    kb = await _shot_kb(mid, turn)
    return text, kb


async def _shot_kb(mid: int, turn: int):
    """Кнопки координат для выстрела (буква -> цифра)."""
    from services.ui import btn
    kb = InlineKeyboardBuilder()
    aim = _aim.get((mid, turn))
    if aim is None:
        for i, letter in enumerate(navy.COLS):
            await btn(kb, letter, f"navy_fire_col:{mid}:{i}")
        kb.adjust(5, 5)
    else:
        for r in range(navy.SIZE):
            await btn(kb, str(r + 1), f"navy_fire_row:{mid}:{r}")
        await btn(kb, "↩️ Сменить столбец", f"navy_fire_reset:{mid}")
        kb.adjust(5, 5, 1)
    return kb.as_markup()


async def _sync_board(bot, mid: int):
    """Обновить общее поле в чате."""
    if mid not in _board:
        return
    chat_id, msg_id = _board[mid]
    text, kb = await _board_view(mid)
    em = await settings.emoji_map()
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, chat_id, msg_id, text, em, reply_markup=kb)


async def _send_myfleet(bot, chat_id: int, mid: int, uid: int):
    """Эфемерка «Мой флот» игроку uid."""
    st = await matches.navy_state(mid)
    field = _fields(st).get(str(uid)) or navy.new_field()
    emo = await _emo()
    grid = navy.render_my_field(field, emo)
    text = f"⚓ <b>Мой флот</b>\n(куда стрелял противник)\n\n{grid}"
    key = (mid, uid)
    eph_id = _myfleet.get(key)
    if eph_id:
        with contextlib.suppress(Exception):
            await ui.edit_ephemeral(bot, chat_id, uid, eph_id, text)
            return
    with contextlib.suppress(Exception):
        m = await ui.send_ephemeral(bot, chat_id, uid, text)
        if m and getattr(m, "ephemeral_message_id", None):
            _myfleet[key] = m.ephemeral_message_id


# ---------------- выбор столбца для выстрела ----------------
@router.callback_query(F.data.startswith("navy_fire_col:"))
async def cb_fire_col(c: CallbackQuery):
    _, mid_s, col_s = c.data.split(":")
    mid, col = int(mid_s), int(col_s)
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return await c.answer("Игра не активна.", show_alert=True)
    if c.from_user.id != m["turn"]:
        return await c.answer("Сейчас не твой ход.", show_alert=True)
    _aim[(mid, c.from_user.id)] = col
    await c.answer(f"Столбец {navy.col_letter(col)}")
    await _sync_board(c.bot, mid)


@router.callback_query(F.data.startswith("navy_fire_reset:"))
async def cb_fire_reset(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    _aim.pop((mid, c.from_user.id), None)
    await c.answer("Выбери столбец заново")
    await _sync_board(c.bot, mid)


# ---------------- выстрел (выбор строки) ----------------
@router.callback_query(F.data.startswith("navy_fire_row:"))
async def cb_fire_row(c: CallbackQuery):
    _, mid_s, row_s = c.data.split(":")
    mid, row = int(mid_s), int(row_s)
    uid = c.from_user.id
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return await c.answer("Игра не активна.", show_alert=True)
    if uid != m["turn"]:
        return await c.answer("Сейчас не твой ход.", show_alert=True)
    col = _aim.get((mid, uid))
    if col is None:
        return await c.answer("Сначала выбери столбец.", show_alert=True)
    cell = (row, col)

    st = await matches.navy_state(mid)
    opp = _opponent(m, uid)
    fields = _fields(st)
    opp_field = fields.get(str(opp))
    if not opp_field:
        return await c.answer("Ошибка поля соперника.", show_alert=True)
    # уже стреляли сюда?
    if navy._ck(cell) in opp_field.get("shots_at_me", {}):
        return await c.answer("Сюда уже стреляли, выбери другую клетку.", show_alert=True)

    result = navy.register_shot(opp_field, cell)
    fields[str(opp)] = opp_field
    st["fields"] = fields
    _aim.pop((mid, uid), None)

    # обновить эфемерку соперника (по нему попали/мимо)
    await _send_myfleet(c.bot, m["origin_chat"], mid, opp)

    if navy.all_sunk(opp_field):
        # победа!
        st_json = st
        await matches.navy_save_turn(mid, st_json, uid)
        await c.answer("💥 Потоплен весь флот! Победа!")
        from handlers.navy_finish import finish_navy
        with contextlib.suppress(Exception):
            await finish_navy(c.bot, mid, uid)
        return

    coord = f"{navy.col_letter(col)}{row + 1}"
    if result in ("hit", "sunk"):
        # попал — ходит ещё
        await matches.navy_save_turn(mid, st, uid)
        msg = "🔥 Потопил корабль! Ходи ещё." if result == "sunk" else "🔥 Попал! Ходи ещё."
        await c.answer(f"{coord}: {msg}")
    else:
        # промах — ход сопернику
        await matches.navy_save_turn(mid, st, opp)
        await c.answer(f"{coord}: 💥 Мимо. Ход соперника.")
        with contextlib.suppress(Exception):
            await ui.send(c.bot, opp, "⚓ Твой ход! Стреляй по общему полю в чате.")
    await _sync_board(c.bot, mid)


async def timeout_worker(bot):
    """Тайм-ауты морского боя: расстановка (15 мин) и ход (5 мин)."""
    import asyncio, logging
    log = logging.getLogger("refbot")
    log.info("navy timeout worker запущен")
    while True:
        try:
            # расстановка просрочена — возврат обоим
            for m in await matches.navy_expire_placement():
                sx = await settings.ctx()
                e = sx["e_" + m["currency"]]
                for uid in (m["p1"], m["p2"]):
                    if uid:
                        with contextlib.suppress(Exception):
                            await ui.send(bot, uid,
                                f"⏳ Морской бой отменён — не успели расставить корабли за 15 минут.\n"
                                f"Ставка <b>{m['stake']:,}</b> {e} возвращена.".replace(",", " "))
                with contextlib.suppress(Exception):
                    if mid_board := _board.get(m["id"]):
                        await bot.edit_message_text("⏳ Бой отменён (тайм-аут расстановки).",
                            chat_id=mid_board[0], message_id=mid_board[1])
            # ход просрочен — поражение зевнувшего
            for m in await matches.navy_expire_moves():
                sx = await settings.ctx()
                e = sx["e_" + m["currency"]]
                w = m.get("timeout_winner"); l = m.get("timeout_loser")
                if w:
                    from config import MATCH_FEE_PCT
                    payout = m["stake"] * 2 - int(m["stake"] * MATCH_FEE_PCT / 100)
                    with contextlib.suppress(Exception):
                        await ui.send(bot, w, f"🏆 Соперник не сделал ход за 5 минут — победа твоя! "
                                              f"+{payout:,} {e}".replace(",", " "))
                    with contextlib.suppress(Exception):
                        await ui.send(bot, l, "⏳ Ты не сделал ход за 5 минут — поражение. Ставка потеряна.")
                    with contextlib.suppress(Exception):
                        if b := _board.get(m["id"]):
                            wname = await _name(w)
                            await bot.edit_message_text(
                                f"⚓ Морской бой окончен. {wname} победил (соперник просрочил ход).",
                                chat_id=b[0], message_id=b[1])
        except Exception as ex:
            log.warning("navy timeout worker: %s", ex)
        await __import__("asyncio").sleep(20)
