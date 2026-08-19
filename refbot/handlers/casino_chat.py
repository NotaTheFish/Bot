"""
Казино в общем чате через команду «!казино».

  !казино                     -> справка (игры, ставки)
  !казино кейсы 100к грибы     -> открыть кейс на 100к грибов
  !казино рулетка 50к грибы    -> крутить колесо
  !казино карточки 1м грибы    -> карточки (mines) — В v83
  («карты» = синоним «карточки»)

Ставка списывается сразу, выигрыш начисляется. Под результатом — inline-кнопка
«Крутить ещё» (та же игра, та же ставка). Жать может ТОЛЬКО автор команды; чужие
нажатия игнорируются молча (не отвечаем на callback — не грузим бота).

Доступно только при включённом casino.enabled (тумблер в админке).
"""
import contextlib
import logging
import time

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
import keyboards as kb
from config import (CASES, COIN_RATE, WHEEL_MIN_BET, WHEEL_MAX_BET, MINES_BETS,
                    MINES_PRESETS)
from services import casino, settings, ui
from services.amount_parse import parse_amount
from keyboards import btn

router = Router()
log = logging.getLogger("casino_chat")

_CUR_WORDS = {
    "грибы": "mushrooms", "гриб": "mushrooms", "грибов": "mushrooms",
    "коины": "coins", "коинов": "coins", "коин": "coins",
}
_GAME_WORDS = {
    "кейсы": "cases", "кейс": "cases",
    "рулетка": "wheel", "рулетку": "wheel", "колесо": "wheel",
    "карточки": "mines", "карты": "mines", "карточка": "mines",
}


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _rate(cur: str) -> int:
    return COIN_RATE if cur == "coins" else 1


async def _casino_on() -> bool:
    return await settings.get("casino.enabled", "0") == "1"


# ---------------- парсинг команды ----------------
def _parse(text: str):
    """«!казино кейсы 100к грибы» -> (game, bet, cur) или None (нужна справка)."""
    t = (text or "").strip()
    low = t.lower()
    # срезаем «!казино» / «/казино»
    for pref in ("!казино", "/казино", "!casino"):
        if low.startswith(pref):
            rest = t[len(pref):].strip()
            break
    else:
        return "help"
    if not rest:
        return "help"
    parts = rest.split()
    if len(parts) < 3:
        return "help"
    game = _GAME_WORDS.get(parts[0].lower())
    bet = parse_amount(parts[1])
    cur = None
    for w in parts[2:]:
        if w.lower() in _CUR_WORDS:
            cur = _CUR_WORDS[w.lower()]
            break
    if not game or bet is None or bet <= 0 or cur is None:
        return "help"
    return game, bet, cur


# ---------------- справка ----------------
async def _help_text() -> str:
    r = COIN_RATE
    # кейсы: цены
    case_lines = []
    for key, (title, price_mush, _) in CASES.items():
        case_lines.append(f"  • {title}: {fmt(price_mush)} 🍄 / {fmt(price_mush*r)} 🪙")
    mines_lines = []
    for b in MINES_BETS:
        mines_lines.append(f"{fmt(b)} 🍄 / {fmt(b*r)} 🪙")
    return (
        "🎰 <b>Казино в чате</b>\n\n"
        "Формат: <code>!казино игра ставка валюта</code>\n\n"
        "🎁 <b>Кейсы</b> — фиксированные цены:\n"
        + "\n".join(case_lines) + "\n"
        "  <code>!казино кейсы 100к грибы</code>\n\n"
        f"🎡 <b>Рулетка</b> — ставка от {fmt(WHEEL_MIN_BET)} до {fmt(WHEEL_MAX_BET)} 🍄 "
        f"({fmt(WHEEL_MIN_BET*r)}–{fmt(WHEEL_MAX_BET*r)} 🪙):\n"
        "  <code>!казино рулетка 50к грибы</code>\n\n"
        "🃏 <b>Карточки</b> — ставки: " + " / ".join(mines_lines) + "\n"
        "  <code>!казино карты 1м грибы</code>\n\n"
        "Валюта: <b>грибы</b> или <b>коины</b>."
    )


# ---------------- команда ----------------
@router.message(F.text.lower().startswith(("!казино", "/казино", "!casino")))
async def cmd_casino(msg: Message):
    if not await _casino_on():
        return await ui.reply(msg, "🎰 Казино сейчас закрыто.")
    parsed = _parse(msg.text)
    if parsed == "help":
        return await ui.reply(msg, await _help_text())
    game, bet, cur = parsed
    await _launch(msg, msg.from_user.id, game, bet, cur, again_of=None)


async def _launch(msg_or_c, uid: int, game: str, bet: int, cur: str, again_of):
    """Запустить игру. msg_or_c — Message (новая) или CallbackQuery («ещё»)."""
    if game == "cases":
        await _play_case_chat(msg_or_c, uid, bet, cur, again_of)
    elif game == "wheel":
        await _play_wheel_chat(msg_or_c, uid, bet, cur, again_of)
    elif game == "mines":
        # карточки в чате — в v83
        target = msg_or_c.message if hasattr(msg_or_c, "message") else msg_or_c
        with contextlib.suppress(Exception):
            await ui.reply(target, "🃏 Карточки в чате скоро появятся. Пока — в личке бота.")


def _again_kb(uid: int, game: str, bet: int, cur: str):
    k = InlineKeyboardBuilder()
    # в callback зашиваем автора, игру, ставку, валюту
    import base64
    payload = f"{uid}:{game}:{bet}:{cur}"
    k.button(text="🔁 Крутить ещё", callback_data=f"cch:{payload}")
    k.adjust(1)
    return k.as_markup()


# ---------------- кейсы в чате ----------------
async def _play_case_chat(msg_or_c, uid: int, bet: int, cur: str, again_of):
    # bet должен совпасть с ценой одного из кейсов (в валюте игрока)
    rate = _rate(cur)
    case_key = None
    for key, (title, price_mush, _) in CASES.items():
        if price_mush * rate == bet:
            case_key = key
            break
    target = msg_or_c.message if hasattr(msg_or_c, "message") else msg_or_c
    if case_key is None:
        prices = " / ".join(fmt(CASES[k][1] * rate) for k in CASES)
        with contextlib.suppress(Exception):
            await ui.reply(target, f"🎁 Ставка для кейсов: {prices}. Выбери одну из них.")
        return

    price = bet
    mult, _ = casino.roll_prize(case_key)
    won = int(price * mult)
    idem = f"cchcase:{uid}:{case_key}:{int(time.time()*1000)}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, uid, cur, -price, "cch_case_bet", idem + ":bet")
                new_bal = await db.apply(conn, uid, cur, won, "cch_case_win", idem + ":win")
    except Exception:
        with contextlib.suppress(Exception):
            b = await db.balances(uid)
            await ui.reply(target, f"Недостаточно средств. У тебя {fmt(b[cur])}.")
        return

    sx = await settings.ctx()
    e = sx["e_" + cur]
    title = CASES[case_key][0]
    text = (f"🎁 <b>{title}</b>\n"
            f"Ставка: {fmt(price)} {e}\n"
            f"Выпало: <b>×{mult:g}</b> → <b>{fmt(won)}</b> {e}\n"
            f"Баланс: {fmt(new_bal)} {e}")
    await _send_result(msg_or_c, target, text, uid, "cases", price, cur, again_of)


# ---------------- рулетка в чате ----------------
async def _play_wheel_chat(msg_or_c, uid: int, bet: int, cur: str, again_of):
    target = msg_or_c.message if hasattr(msg_or_c, "message") else msg_or_c
    rate = _rate(cur)
    bet_mush = bet // rate
    if not casino.wheel_bet_ok(bet_mush):
        with contextlib.suppress(Exception):
            await ui.reply(target,
                f"🎡 Ставка рулетки: {fmt(WHEEL_MIN_BET*rate)}–{fmt(WHEEL_MAX_BET*rate)} {'🪙' if cur=='coins' else '🍄'}.")
        return

    mult = casino.roll_wheel()
    won = int(bet * mult)
    idem = f"cchwheel:{uid}:{int(time.time()*1000)}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, uid, cur, -bet, "cch_wheel_bet", idem + ":bet")
                new_bal = await db.apply(conn, uid, cur, won, "cch_wheel_win", idem + ":win")
    except Exception:
        with contextlib.suppress(Exception):
            b = await db.balances(uid)
            await ui.reply(target, f"Недостаточно средств. У тебя {fmt(b[cur])}.")
        return

    sx = await settings.ctx()
    e = sx["e_" + cur]
    text = (f"🎡 <b>Рулетка</b>\n"
            f"Ставка: {fmt(bet)} {e}\n"
            f"Выпало: <b>×{mult:g}</b> → <b>{fmt(won)}</b> {e}\n"
            f"Баланс: {fmt(new_bal)} {e}")
    await _send_result(msg_or_c, target, text, uid, "wheel", bet, cur, again_of)


async def _send_result(msg_or_c, target, text, uid, game, bet, cur, again_of):
    """Отправить/обновить результат с кнопкой «Крутить ещё»."""
    markup = _again_kb(uid, game, bet, cur)
    if again_of is not None:
        # это повтор — редактируем существующее сообщение
        with contextlib.suppress(Exception):
            await ui.edit(target, text, reply_markup=markup)
    else:
        with contextlib.suppress(Exception):
            await ui.reply(target, text, reply_markup=markup)


# ---------------- «Крутить ещё» ----------------
@router.callback_query(F.data.startswith("cch:"))
async def cb_again(c: CallbackQuery):
    payload = c.data[4:]
    try:
        owner_s, game, bet_s, cur = payload.split(":")
        owner = int(owner_s)
        bet = int(bet_s)
    except ValueError:
        return  # молча
    # только автор команды; чужие — игнорируем МОЛЧА (не отвечаем на callback)
    if c.from_user.id != owner:
        return
    if not await _casino_on():
        return await c.answer("Казино закрыто.", show_alert=True)
    await c.answer()
    await _launch(c, owner, game, bet, cur, again_of=True)
