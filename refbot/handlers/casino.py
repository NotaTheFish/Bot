"""
Казино (в личке бота). Пока — кейсы. Рулетка платная и карточки — позже.

Доступ: rb_settings['casino.enabled'] = '0'|'1'. 0 — видит только админ (кнопки нет
у клиентов), 1 — все. Переключается в админке на лету.

Открытие кейса: выбрал кейс -> выбрал валюту -> оплата и приз в одной транзакции.
RTP ~85%, игрок всегда что-то получает. Каждое открытие логируется.
"""
import asyncio
import contextlib
import logging

import asyncpg
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from config import SUPER_ADMINS, COIN_RATE, WHEEL_MIN_BET, WHEEL_MAX_BET
from services import casino, settings, ui

router = Router()
log = logging.getLogger("casino")


class Wheel(StatesGroup):
    bet = State()


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


async def casino_enabled() -> bool:
    return await casino.enabled()


async def casino_visible(uid: int) -> bool:
    """Видит ли пользователь казино: включено для всех ИЛИ он админ."""
    return await casino.visible(uid)


# ---------------- меню казино ----------------
@router.callback_query(F.data == "casino")
async def cb_casino(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    note = "" if await casino_enabled() else "\n\n<i>🔴 Сейчас видно только админам.</i>"
    await ui.edit(c.message,
        "🎰 <b>Казино</b>\n\nВыбери игру:" + note,
        reply_markup=await kb.casino_menu())
    await c.answer()


@router.callback_query(F.data == "casino_cases")
async def cb_cases(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    b = await db.balances(c.from_user.id)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"📦 <b>Кейсы</b>\n\n"
        f"Твой баланс: {sx['e_mushrooms']} {b['mushrooms']:,} · "
        f"{sx['e_coins']} {b['coins']:,}\n\n"
        f"Открываешь кейс — всегда что-то выигрываешь. С небольшим шансом — джекпот ×10!"
        .replace(",", " "),
        reply_markup=await kb.casino_cases(casino.all_cases()))
    await c.answer()


@router.callback_query(F.data.startswith("case:"))
async def cb_case_pick(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    case_key = c.data.split(":")[1]
    title = casino.case_title(case_key)
    price_m = casino.case_price(case_key, "mushrooms")
    price_c = casino.case_price(case_key, "coins")
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"📦 <b>{title}</b>\n\n"
        f"Цена: {price_m:,} {sx['e_mushrooms']} или {price_c:,} {sx['e_coins']}\n\n"
        f"Чем платишь?".replace(",", " "),
        reply_markup=await kb.case_currency(case_key))
    await c.answer()


@router.callback_query(F.data.startswith("casebuy:"))
async def cb_case_open(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    _, case_key, cur = c.data.split(":")
    uid = c.from_user.id
    if await db.is_banned(uid):
        return await c.answer("Ты заблокирован в системе.", show_alert=True)

    cost = casino.case_price(case_key, cur)
    b = await db.balances(uid)
    if b[cur] < cost:
        sx = await settings.ctx()
        return await c.answer(
            f"Не хватает: нужно {cost:,} {sx['e_' + cur]}, у тебя {b[cur]:,}."
            .replace(",", " "), show_alert=True)

    # ролл приза
    mult, _p = casino.roll_prize(case_key)
    won = casino.prize_amount(case_key, cur, mult)

    # оплата + приз в одной транзакции. idem уникален на каждое открытие (иначе
    # «Открыть ещё» в том же сообщении не сработает — message_id не меняется).
    import time
    stamp = int(time.time() * 1000)
    idem = f"case:{uid}:{case_key}:{stamp}:{cur}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, uid, cur, -cost, "case_buy", idem + ":pay")
                new_bal = await db.apply(conn, uid, cur, won, "case_win", idem + ":win")
    except asyncpg.CheckViolationError:
        return await c.answer("Не хватает баланса.", show_alert=True)
    except asyncpg.UniqueViolationError:
        return await c.answer("Повтори ещё раз.", show_alert=True)

    await db.log_case_open(uid, case_key, cur, cost, won, mult)
    await db.audit(uid, "case_open",
                   {"case": case_key, "cur": cur, "cost": cost, "won": won, "mult": mult})

    await c.answer("Открываю…")
    await _animate_case(c, case_key, cur, mult, won, new_bal)


async def _animate_case(c, case_key, cur, mult, won, new_bal):
    """Простая анимация открытия + результат."""
    e_cur = await settings.emoji_html(cur)
    title = casino.case_title(case_key)

    # короткая анимация «прокрутки»
    frames = ["📦", "📦✨", "📦💥", "🎁"]
    for f in frames:
        with contextlib.suppress(Exception):
            await ui.edit(c.message, f"<b>{title}</b>\n\n{f} открываем…")
        await asyncio.sleep(0.5)

    # подпись по редкости
    if mult == 10.0:
        head = "🎰💥 <b>ДЖЕКПОТ!!!</b> 💥🎰"
    elif mult == 5.0:
        head = "🔥💰 <b>ОГРОМНЫЙ ВЫИГРЫШ!</b> 💰🔥"
    elif mult >= 3.0:
        head = "💎 <b>КРУПНЫЙ ВЫИГРЫШ!</b>"
    elif mult >= 1.0:
        head = "🔥 <b>Неплохо!</b>"
    else:
        head = "🎁 <b>Открыто</b>"

    profit = won - casino.case_price(case_key, cur)
    profit_line = (f"В плюс: +{profit:,}" if profit > 0
                   else f"В этот раз в минус: {profit:,}" if profit < 0
                   else "Вернул своё")
    await ui.edit(c.message,
        f"{head}\n\n"
        f"Из «{title}» выпало: <b>{won:,}</b> {e_cur} (×{mult})\n"
        f"{profit_line}\n\n"
        f"Баланс: <b>{new_bal:,}</b> {e_cur}".replace(",", " "),
        reply_markup=await kb.case_again(case_key))


# ---------------- рулетка (колесо) ----------------
@router.callback_query(F.data == "wheel")
async def cb_wheel(c: CallbackQuery, state: FSMContext):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    await state.set_state(Wheel.bet)
    b = await db.balances(c.from_user.id)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"🎡 <b>Рулетка</b>\n\n"
        f"Крути колесо — множитель ×0…×50 на ставку!\n"
        f"Сектора: ×0.5, ×1, ×2, ×3, ×5, ×10 и джекпот <b>×50</b>.\n\n"
        f"Баланс: {sx['e_mushrooms']} {b['mushrooms']:,} · {sx['e_coins']} {b['coins']:,}\n\n"
        f"Введи ставку от <b>{WHEEL_MIN_BET:,}</b> до <b>{WHEEL_MAX_BET:,}</b> грибов "
        f"(коины по курсу). Например <code>5000</code> или <code>5к</code>."
        .replace(",", " "),
        reply_markup=await kb.wheel_back())
    await c.answer()


@router.message(Wheel.bet)
async def wheel_bet_input(msg: Message, state: FSMContext):
    if not await casino_visible(msg.from_user.id):
        return await state.clear()
    from services.amount_parse import parse_amount
    bet = parse_amount(msg.text or "")
    if bet is None or bet <= 0:
        return await ui.answer(msg, "Нужно число. Например <code>5000</code> или <code>5к</code>.")
    if not casino.wheel_bet_ok(bet):
        return await ui.answer(msg,
            f"Ставка от {WHEEL_MIN_BET:,} до {WHEEL_MAX_BET:,} грибов.".replace(",", " "))
    await state.clear()
    # валюта игрока
    u = await db.get_user(msg.from_user.id)
    cur = u["currency"]
    bet_cur = bet * COIN_RATE if cur == "coins" else bet
    await _spin_wheel(msg, msg.from_user.id, bet_cur, cur, edit=False)


@router.callback_query(F.data.startswith("wheelspin:"))
async def cb_wheel_again(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    _, bet_s, cur = c.data.split(":")
    bet_cur = int(bet_s)
    await c.answer()
    await _spin_wheel(c.message, c.from_user.id, bet_cur, cur, edit=True)


async def _spin_wheel(target, uid: int, bet_cur: int, cur: str, edit: bool):
    """
    Крутить колесо. target — Message (для ответа/редактирования).
    bet_cur — ставка в валюте игрока. Списываем ставку, начисляем выигрыш.
    """
    if await db.is_banned(uid):
        return
    b = await db.balances(uid)
    if b[cur] < bet_cur:
        sx = await settings.ctx()
        txt = f"Не хватает: ставка {bet_cur:,} {sx['e_' + cur]}, у тебя {b[cur]:,}.".replace(",", " ")
        return await (ui.edit(target, txt, reply_markup=await kb.wheel_back()) if edit
                      else ui.answer(target, txt, reply_markup=await kb.wheel_back()))

    mult = casino.roll_wheel()
    won = int(bet_cur * mult)

    # ставка -> выигрыш в одной транзакции
    import time
    idem = f"wheel:{uid}:{int(time.time()*1000)}:{cur}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, uid, cur, -bet_cur, "wheel_bet", idem + ":bet")
                new_bal = await db.apply(conn, uid, cur, won, "wheel_win", idem + ":win")
    except asyncpg.CheckViolationError:
        return
    except asyncpg.UniqueViolationError:
        return

    await db.audit(uid, "wheel_spin", {"bet": bet_cur, "mult": mult, "won": won, "cur": cur})
    bet_mush = bet_cur // COIN_RATE if cur == "coins" else bet_cur
    await db.log_case_open(uid, "wheel", cur, bet_cur, won, mult)

    await _animate_wheel(target, uid, bet_cur, cur, mult, won, new_bal, edit)


# лента секторов для анимации (по кругу)
_WHEEL_STRIP = ["×0", "×0.5", "×1", "×2", "×3", "×5", "×10", "×50",
                "×0", "×0.5", "×1", "×2", "×3", "×5"]


def _wheel_frame(pos: int) -> str:
    """Окно из 5 секторов с бегунком на pos."""
    N = len(_WHEEL_STRIP)
    parts = []
    for i in range(pos - 2, pos + 3):
        s = _WHEEL_STRIP[i % N]
        parts.append(f"▸{s}◂" if i == pos else s)
    return "  ".join(parts)


async def _animate_wheel(target, uid, bet_cur, cur, mult, won, new_bal, edit):
    e_cur = await settings.emoji_html(cur)

    async def show(text, kb_markup=None):
        if edit:
            return await ui.edit(target, text, reply_markup=kb_markup)
        # первый показ — отвечаем, дальше редактируем это же сообщение
        return await ui.answer(target, text, reply_markup=kb_markup)

    # найдём позицию сектора-результата в ленте
    label = ("×0" if mult == 0 else f"×{mult:g}")
    try:
        stop = _WHEEL_STRIP.index(label)
    except ValueError:
        stop = 0

    # кадры с замедлением: быстрая прокрутка -> подводим к stop
    positions = [0, 3, 6, 9, 12, 2, 5, 8, 11, 1, 4, 7]  # бег
    # последние кадры аккуратно подводим к результату
    positions += [(stop - 2) % len(_WHEEL_STRIP), (stop - 1) % len(_WHEEL_STRIP), stop, stop]

    msg = None
    for i, pos in enumerate(positions):
        speed = "🎰" if i < 8 else ("⏳" if i < 12 else "🎯")
        frame = f"🎡 <b>Рулетка</b>\n\n{speed}  {_wheel_frame(pos)}"
        with contextlib.suppress(Exception):
            m = await show(frame)
            if m:
                msg = m
        # после первого показа переключаемся на edit того же сообщения
        if not edit and msg:
            edit = True
            target = msg
        await asyncio.sleep(0.35 if i < 8 else 0.5)

    # результат
    bet_mush = bet_cur // COIN_RATE if cur == "coins" else bet_cur
    if mult == 50.0:
        head = ("🎡💥 <b>МЕГА-ДЖЕКПОТ КОЛЕСА!!!</b> 💥🎡\n"
                "Колесо замерло на <b>×50</b> — такое видят единицы!\n"
                "Шанс — 0.1%. Один на тысячу оборотов. Ты поймал его 🔥")
    elif mult == 0.0:
        head = "💨 <b>Мимо!</b> Колесо встало на ×0 — ставка сгорела. Крутанём ещё?"
    elif mult >= 5.0:
        head = f"🔥 <b>Крупно!</b> ×{mult:g}!"
    elif mult >= 1.0:
        head = f"✅ Выпало ×{mult:g}"
    else:
        head = f"🙂 Выпало ×{mult:g} — вернулась часть ставки"

    profit = won - bet_cur
    profit_line = (f"В плюс: <b>+{profit:,}</b>" if profit > 0
                   else f"В минус: <b>{profit:,}</b>" if profit < 0
                   else "Вернул ставку")
    result = (
        f"🎡 <b>Рулетка</b>\n\n{head}\n\n"
        f"Ставка: {bet_cur:,} {e_cur} → множитель ×{mult:g}\n"
        f"Выигрыш: <b>{won:,}</b> {e_cur}\n"
        f"{profit_line}\n\n"
        f"Баланс: <b>{new_bal:,}</b> {e_cur}"
    ).replace(",", " ")
    with contextlib.suppress(Exception):
        await ui.edit(target, result, reply_markup=await kb.wheel_again(bet_mush, cur))


# ---------------- админ: переключатель доступа ----------------
@router.callback_query(F.data == "casino_admin")
async def cb_casino_admin(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        "🔴 Только админ — клиенты не видят кнопку «Казино».\n"
        "🟢 Открыто всем — кнопка появляется у всех.",
        reply_markup=await kb.casino_admin_toggle(await casino_enabled()))
    await c.answer()


@router.callback_query(F.data == "casino_toggle")
async def cb_casino_toggle(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    new = "0" if await casino_enabled() else "1"
    await settings.set("casino.enabled", new, c.from_user.id)
    await db.audit(c.from_user.id, "casino_toggle", {"enabled": new})
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        + ("🟢 Теперь открыто всем." if new == "1" else "🔴 Теперь только админ."),
        reply_markup=await kb.casino_admin_toggle(new == "1"))
    await c.answer("Готово")
