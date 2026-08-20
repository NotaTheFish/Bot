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
import asyncio
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


async def _anim_ephemeral() -> bool:
    """Тумблер: анимация казино в чате эфемерно (тестово). Выкл -> публично как раньше."""
    return await settings.get("chat.anim_ephemeral", "0") == "1"


def _is_group(msg) -> bool:
    return msg.chat.type in ("group", "supergroup")


async def _err(msg_or_target, uid: int, text: str):
    """Ошибка/уведомление игроку: в группе — эфемерно (только ему), в личке — обычно.
    Ошибки не палят баланс, поэтому если эфемерка не сработала — показываем публично
    (текст ошибки вроде «недостаточно средств» не критичен), но публичный дубль от
    _NotEphemeral удаляем и шлём обычным reply один раз."""
    target = msg_or_target
    chat = getattr(target, "chat", None)
    if chat and chat.type in ("group", "supergroup"):
        sent = await ui.send_ephemeral(target.bot, chat.id, uid, text)
        if isinstance(sent, ui._NotEphemeral):
            return  # публичное уже отправлено Telegram-ом, дублировать не нужно
        if sent is not None:
            return
    with contextlib.suppress(Exception):
        await ui.reply(target, text)


# ---------------- !баланс / !профиль ----------------
@router.message(F.text.lower().in_({"!баланс", "!balance", "!баланс!", "!профиль", "!profile", "!профиль!"}))
async def cmd_balance_profile(msg: Message):
    text_l = (msg.text or "").lower().strip()
    public = text_l.endswith("!")  # !баланс! = показать всем; !баланс = только мне
    is_profile = "профил" in text_l or "profile" in text_l
    uid = msg.from_user.id
    b = await db.balances(uid)
    sx = await settings.ctx()

    if is_profile:
        # полный профиль по шаблону
        row = await db.get_user(uid)
        hold_sum = await db.pool().fetch(
            "SELECT currency, COALESCE(SUM(amount),0) s FROM rb_withdrawals "
            "WHERE tg_id=$1 AND status='pending' GROUP BY currency", uid) if row else []
        holds = {r["currency"]: r["s"] for r in hold_sum}
        tpl = await settings.profile_template()
        data = {
            **sx, "id": uid,
            "bal_m": fmt(b["mushrooms"]), "bal_c": fmt(b["coins"]), "bal_s": fmt(b["shimcoins"]),
            "hold_m": fmt(holds.get("mushrooms", 0)), "hold_c": fmt(holds.get("coins", 0)),
            "paid": row["paid"] if row else 0, "hold": row["hold"] if row else 0,
            "lost": row["lost"] if row else 0, "chats": "",
            "e_cur": sx.get(f"e_{row['currency']}", "🍄") if row else "🍄",
            "l_cur": sx.get(f"l_{row['currency']}", "Грибы") if row else "Грибы",
        }
        try:
            text = tpl.format(**data)
        except Exception:
            text = (f"{sx['e_profile']} <b>Профиль</b>\nID: <code>{uid}</code>\n\n"
                    f"{sx['e_mushrooms']} {fmt(b['mushrooms'])} · "
                    f"{sx['e_coins']} {fmt(b['coins'])} · "
                    f"{sx['e_shimcoins']} {fmt(b['shimcoins'])}")
    else:
        text = (f"{sx['e_balance']} <b>Баланс</b>\n"
                f"{sx['e_mushrooms']} {fmt(b['mushrooms'])}\n"
                f"{sx['e_coins']} {fmt(b['coins'])}\n"
                f"{sx['e_shimcoins']} {fmt(b['shimcoins'])}")

    # Приватность: баланс чувствителен (сумма на виду) — при сбое эфемерки прячем.
    # Профиль менее критичен — при сбое показываем публично (лучше показать, чем спрятать).
    if _is_group(msg) and not public:
        sent = await ui.send_ephemeral(msg.bot, msg.chat.id, uid, text)
        if isinstance(sent, ui._NotEphemeral):
            if is_profile:
                # профиль: публичное сообщение уже отправлено Telegram-ом — оставляем как есть
                pass
            else:
                # баланс: удаляем публичный дубль, чтобы не палить сумму
                with contextlib.suppress(Exception):
                    await sent.msg.delete()
                with contextlib.suppress(Exception):
                    await ui.reply(msg, "⚠️ Не удалось показать баланс приватно. Смотри в личке бота.")
        elif sent is None:
            # эфемерка не отправилась совсем — показываем публично (иначе пусто)
            with contextlib.suppress(Exception):
                await ui.reply(msg, text)
    else:
        await ui.reply(msg, text)


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
        target = msg_or_c.message if hasattr(msg_or_c, "message") else msg_or_c
        await _mines_choose_field(target, uid, bet, cur)


# ================= КАРТОЧКИ (MINES) В ЧАТЕ =================
# Состояние игр по message_id (в чате несколько игроков одновременно — каждая
# игра под своим сообщением, привязана к автору). Не FSM, чтобы не путать игроков.
_chat_mines: dict[int, dict] = {}
_cm_redraw: dict[int, asyncio.Task] = {}


async def _mines_choose_field(target, uid: int, bet: int, cur: str):
    """Показать выбор поля (пресета) после !казино карты."""
    rate = _rate(cur)
    if bet not in [b * rate for b in MINES_BETS]:
        prices = " / ".join(fmt(b * rate) for b in MINES_BETS)
        with contextlib.suppress(Exception):
            await ui.reply(target, f"🃏 Ставка карточек: {prices}. Выбери одну.")
        return
    k = InlineKeyboardBuilder()
    for key, (total, mines, label) in MINES_PRESETS.items():
        k.button(text=label, callback_data=f"cmfield:{uid}:{bet}:{cur}:{key}")
    k.button(text="Отмена", callback_data=f"cmcancel:{uid}")
    k.adjust(1)
    e = "🪙" if cur == "coins" else "🍄"
    with contextlib.suppress(Exception):
        await ui.reply(target,
            f"🃏 <b>Карточки</b> · ставка {fmt(bet)} {e}\n\nВыбери поле:",
            reply_markup=k.as_markup())


@router.callback_query(F.data.startswith("cmcancel:"))
async def cb_cm_cancel(c: CallbackQuery):
    owner = int(c.data.split(":")[1])
    if c.from_user.id != owner:
        return  # молча
    await c.answer()
    with contextlib.suppress(Exception):
        await ui.edit(c.message, "🃏 Отменено.")


@router.callback_query(F.data.startswith("cmfield:"))
async def cb_cm_field(c: CallbackQuery):
    try:
        _, owner_s, bet_s, cur, key = c.data.split(":")
        owner, bet = int(owner_s), int(bet_s)
    except ValueError:
        return
    if c.from_user.id != owner:
        return  # чужой — молча
    if not await _casino_on():
        return await c.answer("Казино закрыто.", show_alert=True)
    total, mines, label = casino.mines_preset(key)
    # списываем ставку
    idem = f"cchmines:{owner}:{int(time.time()*1000)}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, owner, cur, -bet, "cch_mines_bet", idem + ":bet")
    except Exception:
        b = await db.balances(owner)
        return await c.answer(f"Не хватает: у тебя {fmt(b[cur])}.", show_alert=True)

    field = casino.mines_new_field(total, mines)
    mid = c.message.message_id
    _chat_mines[mid] = {
        "owner": owner, "field": field, "total": total, "mines": mines,
        "key": key, "bet": bet, "cur": cur, "opened": [], "idem": idem,
    }
    sx = await settings.ctx()
    await c.answer()
    with contextlib.suppress(Exception):
        await ui.edit(c.message,
            f"🃏 <b>Карточки</b> · {label}\n\n"
            f"Ставка: {fmt(bet)} {sx['e_' + cur]}\n"
            f"Открывай карты 👇",
            reply_markup=await _cm_grid(total, {}, 1.0, False))


async def _cm_grid(total, opened, mult, can_cashout):
    import math
    cols = {9: 3, 16: 4, 25: 5, 36: 6}.get(total, min(6, int(math.isqrt(total)) or 1))
    if total <= 8:
        cols = total if total <= 5 else (total + 1) // 2
    k = InlineKeyboardBuilder()
    for i in range(total):
        if i in opened:
            k.button(text=opened[i], callback_data="cmnoop")
        else:
            k.button(text="🎴", callback_data=f"cmopen:{i}")
    rows = [cols] * ((total + cols - 1) // cols)
    if can_cashout:
        k.button(text=f"💰 Забрать (×{mult:g})", callback_data="cmcash")
        rows.append(1)
    k.adjust(*rows)
    return k.as_markup()


@router.callback_query(F.data == "cmnoop")
async def cb_cm_noop(c: CallbackQuery):
    await c.answer()


@router.callback_query(F.data.startswith("cmopen:"))
async def cb_cm_open(c: CallbackQuery):
    mid = c.message.message_id
    g = _chat_mines.get(mid)
    if not g:
        return await c.answer("Игра не активна.", show_alert=True)
    if c.from_user.id != g["owner"]:
        return  # чужой игрок — молча
    idx = int(c.data.split(":")[1])
    if idx in g["opened"]:
        return await c.answer()
    field = g["field"]
    sx = await settings.ctx()

    if field[idx] == 0:
        # бомба — проигрыш
        _cm_cancel_redraw(mid)
        opened = {i: ("💣" if field[i] == 0 else "💎") for i in range(g["total"])}
        _chat_mines.pop(mid, None)
        with contextlib.suppress(Exception):
            await ui.edit(c.message,
                f"💥 <b>Бомба!</b>\n"
                f"Ставка {fmt(g['bet'])} {sx['e_' + g['cur']]} сгорела.",
                reply_markup=await _cm_grid(g["total"], opened, 1.0, False))
        # кнопка «ещё» отдельным сообщением
        with contextlib.suppress(Exception):
            await ui.reply(c.message, "Попробовать снова?",
                           reply_markup=_again_kb(g["owner"], "mines", g["bet"], g["cur"]))
        return await c.answer("Бомба!")

    # алмаз
    g["opened"].append(idx)
    picks = len(g["opened"])
    mult = casino.mines_multiplier(g["total"], g["mines"], picks)
    await c.answer(f"💎 ×{mult:g}")
    safe = g["total"] - g["mines"]
    if picks >= safe:
        _cm_cancel_redraw(mid)
        return await _cm_cashout(c, mid, forced=True)
    _cm_schedule_redraw(c, mid)


def _cm_cancel_redraw(mid: int):
    t = _cm_redraw.pop(mid, None)
    if t and not t.done():
        t.cancel()


def _cm_schedule_redraw(c, mid: int, delay: float = 0.4):
    _cm_cancel_redraw(mid)
    _cm_redraw[mid] = asyncio.create_task(_cm_redraw_do(c, mid, delay))


async def _cm_redraw_do(c, mid: int, delay: float):
    try:
        await asyncio.sleep(delay)
        g = _chat_mines.get(mid)
        if not g:
            return
        picks = len(g["opened"])
        safe = g["total"] - g["mines"]
        if picks >= safe or picks == 0:
            return
        mult = casino.mines_multiplier(g["total"], g["mines"], picks)
        win_now = int(g["bet"] * mult)
        sx = await settings.ctx()
        opened_map = {i: "💎" for i in g["opened"]}
        with contextlib.suppress(Exception):
            await ui.edit(c.message,
                f"🃏 <b>Карточки</b> · {casino.mines_preset(g['key'])[2]}\n\n"
                f"Открыто: {picks} · множитель <b>×{mult:g}</b>\n"
                f"Заберёшь: <b>{fmt(win_now)}</b> {sx['e_' + g['cur']]}",
                reply_markup=await _cm_grid(g["total"], opened_map, mult, True))
    except asyncio.CancelledError:
        pass
    finally:
        _cm_redraw.pop(mid, None)


@router.callback_query(F.data == "cmcash")
async def cb_cm_cash(c: CallbackQuery):
    mid = c.message.message_id
    g = _chat_mines.get(mid)
    if not g:
        return await c.answer("Игра не активна.", show_alert=True)
    if c.from_user.id != g["owner"]:
        return  # чужой — молча
    await _cm_cashout(c, mid, forced=False)


async def _cm_cashout(c, mid: int, forced: bool):
    g = _chat_mines.get(mid)
    if not g:
        return
    _cm_cancel_redraw(mid)
    picks = len(g["opened"])
    if picks == 0:
        return await c.answer("Открой хотя бы одну карту.", show_alert=True)
    mult = casino.mines_multiplier(g["total"], g["mines"], picks)
    won = int(g["bet"] * mult)
    _chat_mines.pop(mid, None)
    idem = g["idem"]
    with contextlib.suppress(Exception):
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                new_bal = await db.apply(conn, g["owner"], g["cur"], won,
                                         "cch_mines_win", idem + ":win")
    sx = await settings.ctx()
    e = sx["e_" + g["cur"]]
    opened_map = {i: "💎" for i in g["opened"]}
    with contextlib.suppress(Exception):
        await ui.edit(c.message,
            f"💰 <b>Забрал ×{mult:g}!</b>\n"
            f"Выигрыш: <b>{fmt(won)}</b> {e}",
            reply_markup=await _cm_grid(g["total"], opened_map, mult, False))
    with contextlib.suppress(Exception):
        await ui.reply(c.message, "Ещё разок?",
                       reply_markup=_again_kb(g["owner"], "mines", g["bet"], g["cur"]))
    await c.answer("Забрал!")


def _again_kb(uid: int, game: str, bet: int, cur: str):
    k = InlineKeyboardBuilder()
    # в callback зашиваем автора, игру, ставку, валюту
    import base64
    payload = f"{uid}:{game}:{bet}:{cur}"
    k.button(text="🔁 Крутить ещё", callback_data=f"cch:{payload}")
    k.adjust(1)
    return k.as_markup()


async def _animate_and_finish(msg_or_c, target, uid, frames, result, markup, again_of):
    """
    Проиграть кадры анимации, затем показать результат.

    Режим по тумблеру chat.anim_ephemeral (тестовый):
      ВЫКЛ (по умолчанию): всё публично — сообщение редактируется кадрами, потом результат.
      ВКЛ + группа: кадры анимации эфемерно (только игроку), финал — публично всем.
    Эфемерка новая и может глючить, поэтому при любой осечке — фолбэк на публичный режим.
    """
    chat = getattr(target, "chat", None)
    is_group = chat and chat.type in ("group", "supergroup")
    ephemeral = is_group and await _anim_ephemeral() and again_of is None

    if ephemeral:
        # кадры — эфемерно, редактируя одно эфемерное сообщение
        eph = None
        eph_id = None
        try:
            first = True
            for f in frames:
                if first:
                    eph = await ui.send_ephemeral(target.bot, chat.id, uid, f)
                    if eph is None or isinstance(eph, ui._NotEphemeral):
                        # эфемерка не сработала — удалим публичный дубль, если был,
                        # и уйдём в обычный публичный режим ниже
                        if isinstance(eph, ui._NotEphemeral):
                            with contextlib.suppress(Exception):
                                await eph.msg.delete()
                        raise RuntimeError("ephemeral failed")
                    eph_id = getattr(eph, "ephemeral_message_id", None)
                    first = False
                else:
                    if eph_id:
                        await ui.edit_ephemeral(target.bot, chat.id, uid, eph_id, f)
                await asyncio.sleep(0.6)
            # финал — публичное сообщение всем
            await ui.reply(target, result, reply_markup=markup)
            return
        except Exception:
            # эфемерка сломалась — просто покажем результат публично
            with contextlib.suppress(Exception):
                await ui.reply(target, result, reply_markup=markup)
            return

    # обычный публичный режим: редактируем одно сообщение кадрами
    anim_msg = target if again_of is not None else None
    for f in frames:
        with contextlib.suppress(Exception):
            if anim_msg is None:
                anim_msg = await ui.reply(target, f)
            else:
                await ui.edit(anim_msg, f)
        await asyncio.sleep(0.6)
    with contextlib.suppress(Exception):
        if anim_msg is not None:
            await ui.edit(anim_msg, result, reply_markup=markup)
        else:
            await ui.reply(target, result, reply_markup=markup)


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
            await _err(target, uid, f"Недостаточно средств. У тебя {fmt(b[cur])}.")
        return

    sx = await settings.ctx()
    e = sx["e_" + cur]
    title = CASES[case_key][0]

    # заголовок по редкости
    if mult >= 10:
        head = "🎰💥 <b>ДЖЕКПОТ!!!</b> 💥🎰"
    elif mult >= 5:
        head = "🔥💰 <b>ОГРОМНЫЙ ВЫИГРЫШ!</b>"
    elif mult >= 3:
        head = "💎 <b>КРУПНЫЙ ВЫИГРЫШ!</b>"
    elif mult >= 1:
        head = "🔥 <b>Неплохо!</b>"
    else:
        head = "🎁 <b>Открыто</b>"
    result = (f"{head}\n"
              f"«{title}» · ставка {fmt(price)} {e}\n"
              f"Выпало: <b>×{mult:g}</b> → <b>{fmt(won)}</b> {e}\n"
              f"Баланс: {fmt(new_bal)} {e}")
    frames = [f"🎁 <b>{title}</b>\n\n<blockquote>{f}</blockquote>"
              for f in ["📦 открываем…", "📦✨ открываем…", "📦💥 почти…", "🎁 готово!"]]
    markup = _again_kb(uid, "cases", price, cur)
    await _animate_and_finish(msg_or_c, target, uid, frames, result, markup, again_of)


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
            await _err(target, uid, f"Недостаточно средств. У тебя {fmt(b[cur])}.")
        return

    sx = await settings.ctx()
    e = sx["e_" + cur]

    if mult >= 50:
        head = "🎰💥 <b>ДЖЕКПОТ КОЛЕСА!!!</b>"
    elif mult >= 5:
        head = "🔥💰 <b>ОГРОМНЫЙ ВЫИГРЫШ!</b>"
    elif mult >= 2:
        head = "💎 <b>Хороший занос!</b>"
    elif mult >= 1:
        head = "🔥 <b>Неплохо!</b>"
    elif mult > 0:
        head = "🙂 <b>Половина назад</b>"
    else:
        head = "💨 <b>Мимо</b>"
    result = (f"{head}\n"
              f"Ставка: {fmt(bet)} {e}\n"
              f"Выпало: <b>×{mult:g}</b> → <b>{fmt(won)}</b> {e}\n"
              f"Баланс: {fmt(new_bal)} {e}")
    frames = [f"🎡 <b>Рулетка</b>\n\n<blockquote>{f}</blockquote>"
              for f in ["🎡 крутится…", "🎡💨 крутится…", "🎡💨 замедляется…", "🎯 стоп!"]]
    markup = _again_kb(uid, "wheel", bet, cur)
    await _animate_and_finish(msg_or_c, target, uid, frames, result, markup, again_of)


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
    # убираем кнопку со старого сообщения (чтобы не жали повторно)
    with contextlib.suppress(Exception):
        await c.message.edit_reply_markup(reply_markup=None)
    # запускаем НОВУЮ крутку как свежую: again_of=None -> эфемерная анимация (если
    # тумблер включён) + результат НОВЫМ публичным сообщением, а не правкой старого
    await _launch(c, owner, game, bet, cur, again_of=None)
