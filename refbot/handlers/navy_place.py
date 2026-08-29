"""
Морской бой: расстановка кораблей в эфемерном поле.
Выбор корабля -> ввод клеток по координатам (буква -> цифра) -> проверка смежности.
Отмена шага назад, подтверждение когда весь флот на поле.
Эфемерка приватна (видит только игрок), поэтому координаты обычные.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, navy, settings, ui

router = Router()

# in-memory состояние расстановки: (mid, uid) -> {building: [cells], target_len: int, col: int|None}
# building — клетки текущего строящегося корабля; col — выбранная буква (ждём цифру)
_place: dict[tuple, dict] = {}
# id эфемерного сообщения-поля: (mid, uid) -> message (для правки)
_eph: dict[tuple, object] = {}


async def _emo() -> dict:
    """Эмодзи клеток из слотов (премиум, если настроен)."""
    return {
        "water": await settings.emoji("navy_water"),
        "ship":  await settings.emoji("navy_ship"),
        "hit":   await settings.emoji("navy_hit"),
        "miss":  await settings.emoji("navy_miss"),
        "sunk":  await settings.emoji("navy_sunk"),
    }


async def _field_of(mid: int, uid: int) -> dict:
    st = await matches.navy_state(mid)
    fields = st.get("fields", {})
    return fields.get(str(uid)) or navy.new_field()


async def start_placement(bot, mid: int):
    """Начать расстановку у обоих игроков."""
    m = await matches.get(mid)
    if not m:
        return
    for uid in (m["p1"], m["p2"]):
        if uid:
            await _send_placement(bot, m["origin_chat"], mid, uid)


async def _send_placement(bot, chat_id: int, mid: int, uid: int):
    """Отправить/обновить эфемерное поле расстановки игроку uid."""
    key = (mid, uid)
    _place.setdefault(key, {"building": [], "target_len": None, "col": None})
    field = await _field_of(mid, uid)
    text = await _placement_text(mid, uid, field)
    kb = await _placement_kb(mid, uid, field)
    emsg = _eph.get(key)
    if emsg:
        eph_id = getattr(emsg, "ephemeral_message_id", None)
        if eph_id:
            with contextlib.suppress(Exception):
                await ui.edit_ephemeral(bot, chat_id, uid, eph_id, text, reply_markup=kb)
                return
    with contextlib.suppress(Exception):
        m = await ui.send_ephemeral(bot, chat_id, uid, text, reply_markup=kb)
        if m:
            _eph[key] = m


async def _placement_text(mid: int, uid: int, field: dict) -> str:
    emo = await _emo()
    st = _place.get((mid, uid), {})
    building = st.get("building", [])
    grid = navy.render_placement_field(field, building, emo)
    remain = navy.remaining_ships_to_place(field)
    remain_str = ", ".join(f"{L}-палубных: {cnt}" for L, cnt in sorted(remain.items(), reverse=True))
    lines = ["⚓ <b>Расстановка кораблей</b>", "", grid, ""]
    if navy.fleet_complete(field):
        lines.append("✅ Весь флот на поле! Нажми «Подтвердить».")
    elif st.get("target_len"):
        placed = len(building)
        need = st["target_len"]
        if st.get("col") is not None:
            lines.append(f"Ставлю {need}-палубный ({placed}/{need}). "
                         f"Столбец {navy.col_letter(st['col'])} выбран — выбери строку.")
        else:
            lines.append(f"Ставлю {need}-палубный ({placed}/{need}). Выбери столбец (букву).")
    else:
        lines.append(f"Осталось: {remain_str}\nВыбери корабль для установки.")
    return "\n".join(lines)


async def _placement_kb(mid: int, uid: int, field: dict):
    """Клавиатура расстановки: выбор корабля / координаты / отмена / подтвердить."""
    from services.ui import btn
    st = _place.get((mid, uid), {})
    kb = InlineKeyboardBuilder()
    building = st.get("building", [])
    target = st.get("target_len")

    if navy.fleet_complete(field):
        await btn(kb, "✅ Подтвердить расстановку", f"navy_confirm:{mid}")
        kb.adjust(1)
        return kb.as_markup()

    if not target:
        # выбор корабля — рич-кнопки по остатку (тут обычные inline; рич в чате отдельно)
        remain = navy.remaining_ships_to_place(field)
        for L in sorted(remain.keys(), reverse=True):
            await btn(kb, f"🚢 {L}-палубный ({remain[L]})", f"navy_ship:{mid}:{L}")
        kb.adjust(1)
        return kb.as_markup()

    # идёт построение корабля
    if st.get("col") is None:
        # выбираем столбец (буква)
        for i, letter in enumerate(navy.COLS):
            await btn(kb, letter, f"navy_col:{mid}:{i}")
        # раскладка по 5 в ряд
        row = []
        # отмена/сброс
        await btn(kb, "↩️ Отмена шага", f"navy_undo:{mid}")
        kb.adjust(5, 5, 1)
    else:
        # выбираем строку (цифра)
        for r in range(navy.SIZE):
            await btn(kb, str(r + 1), f"navy_row:{mid}:{r}")
        await btn(kb, "↩️ Отмена шага", f"navy_undo:{mid}")
        kb.adjust(5, 5, 1)
    return kb.as_markup()


# ---------------- выбор корабля ----------------
@router.callback_query(F.data.startswith("navy_ship:"))
async def cb_pick_ship(c: CallbackQuery):
    _, mid_s, L_s = c.data.split(":")
    mid, L = int(mid_s), int(L_s)
    uid = c.from_user.id
    field = await _field_of(mid, uid)
    if not navy.can_start_ship_of_length(field, L):
        return await c.answer("Таких кораблей больше нет.", show_alert=True)
    st = _place.setdefault((mid, uid), {"building": [], "target_len": None, "col": None})
    if st.get("target_len") and not navy.ship_ready(st["building"], st["target_len"]):
        return await c.answer("Сначала дострой текущий корабль или отмени.", show_alert=True)
    st["target_len"] = L
    st["building"] = []
    st["col"] = None
    await c.answer(f"Ставим {L}-палубный")
    await _send_placement(c.bot, c.message.chat.id, mid, uid)


# ---------------- выбор столбца (буква) ----------------
@router.callback_query(F.data.startswith("navy_col:"))
async def cb_pick_col(c: CallbackQuery):
    _, mid_s, col_s = c.data.split(":")
    mid, col = int(mid_s), int(col_s)
    uid = c.from_user.id
    st = _place.get((mid, uid))
    if not st or not st.get("target_len"):
        return await c.answer("Сначала выбери корабль.", show_alert=True)
    st["col"] = col
    await c.answer(f"Столбец {navy.col_letter(col)}")
    await _send_placement(c.bot, c.message.chat.id, mid, uid)


# ---------------- выбор строки (цифра) -> ставим клетку ----------------
@router.callback_query(F.data.startswith("navy_row:"))
async def cb_pick_row(c: CallbackQuery):
    _, mid_s, row_s = c.data.split(":")
    mid, row = int(mid_s), int(row_s)
    uid = c.from_user.id
    st = _place.get((mid, uid))
    if not st or st.get("col") is None:
        return await c.answer("Сначала выбери столбец.", show_alert=True)
    field = await _field_of(mid, uid)
    cell = (row, st["col"])
    ok, err = navy.try_add_cell(field, st["building"], cell, st["target_len"])
    st["col"] = None   # сбрасываем выбор столбца для следующей клетки
    if not ok:
        await c.answer(f"⚠️ {err}", show_alert=True)
        await _send_placement(c.bot, c.message.chat.id, mid, uid)
        return
    # клетка добавлена
    if navy.ship_ready(st["building"], st["target_len"]):
        # корабль готов — фиксируем
        navy.commit_ship(field, st["building"])
        await matches.navy_save_field(mid, uid, field)
        st["building"] = []
        st["target_len"] = None
        await c.answer("Корабль поставлен!")
    else:
        await c.answer(f"Клетка {navy.col_letter(cell[1])}{cell[0]+1} поставлена")
    await _send_placement(c.bot, c.message.chat.id, mid, uid)


# ---------------- отмена шага ----------------
@router.callback_query(F.data.startswith("navy_undo:"))
async def cb_undo(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    uid = c.from_user.id
    st = _place.get((mid, uid))
    if not st:
        return await c.answer("Нечего отменять.", show_alert=True)
    if st.get("col") is not None:
        # отменяем выбор столбца
        st["col"] = None
        await c.answer("Столбец сброшен")
    elif st.get("building"):
        # убираем последнюю клетку строящегося
        st["building"].pop()
        await c.answer("Клетка убрана")
    elif st.get("target_len"):
        # отменяем выбор корабля
        st["target_len"] = None
        await c.answer("Выбор корабля отменён")
    else:
        return await c.answer("Нечего отменять.", show_alert=True)
    await _send_placement(c.bot, c.message.chat.id, mid, uid)


# ---------------- подтверждение расстановки ----------------
@router.callback_query(F.data.startswith("navy_confirm:"))
async def cb_confirm(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    uid = c.from_user.id
    field = await _field_of(mid, uid)
    if not navy.fleet_complete(field):
        return await c.answer("Ещё не весь флот на поле.", show_alert=True)
    await matches.navy_set_ready(mid, uid)
    _place.pop((mid, uid), None)
    await c.answer("Готово! Ждём соперника.")
    with contextlib.suppress(Exception):
        emsg = _eph.pop((mid, uid), None)
        eph_id = getattr(emsg, "ephemeral_message_id", None) if emsg else None
        if eph_id:
            await ui.edit_ephemeral(c.bot, c.message.chat.id, uid, eph_id,
                "✅ <b>Флот расставлен!</b>\nЖдём, пока соперник закончит расстановку.",
                reply_markup=None)
    # оба готовы? — начать бой
    st = await matches.navy_state(mid)
    if st.get("phase") == "battle":
        from handlers.navy_game import start_battle
        with contextlib.suppress(Exception):
            await start_battle(c.bot, mid)
