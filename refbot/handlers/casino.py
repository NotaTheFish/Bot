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
from keyboards import btn
from aiogram.utils.keyboard import InlineKeyboardBuilder
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
        "🎰 <b>Казино</b>\n\nВыбирай:" + note,
        reply_markup=await kb.casino_menu())
    await c.answer()


@router.callback_query(F.data == "casino_games")
async def cb_casino_games(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    note = "" if await casino_enabled() else "\n\n<i>🔴 Сейчас видно только админам.</i>"
    await ui.edit(c.message,
        "🎮 <b>Игры</b>\n\nВыбери категорию:" + note,
        reply_markup=await kb.casino_games())
    await c.answer()


@router.callback_query(F.data == "games_solo")
async def cb_games_solo(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    await ui.edit(c.message,
        "🎲 <b>Одиночные игры</b>\n\nИграй против казино:",
        reply_markup=await kb.games_solo())
    await c.answer()


@router.callback_query(F.data == "games_pvp")
async def cb_games_pvp(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    await ui.edit(c.message,
        "⚔️ <b>PvP-игры</b>\n\nИгры на двоих со ставкой. Вызывай соперника "
        "или ищи онлайн:",
        reply_markup=await kb.games_pvp())
    await c.answer()


@router.callback_query(F.data == "casino_stats")
async def cb_casino_stats(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    st = await db.casino_stats(c.from_user.id)
    sx = await settings.ctx()
    _game_name = {"wheel": "🎡 Рулетка", "mines": "🃏 Карточки",
                  "small": "📦 Кейс S", "medium": "📦 Кейс M", "big": "📦 Кейс L"}
    _cur_name = {"mushrooms": "🍄 Грибы", "coins": "🪙 Коины"}

    lines = ["📊 <b>Статистика казино</b>\n"]
    if not st["per"]:
        lines.append("Пока пусто. Сыграй — и здесь появится статистика.")
    else:
        for cur in ("mushrooms", "coins"):
            d = st["per"].get(cur)
            if not d or d["games"] == 0:
                continue
            luck = round(d["wins"] / d["games"] * 100)
            e = sx.get("e_" + cur, "")
            fmt_n = lambda n: f"{n:,}".replace(",", " ")
            lines.append(
                f"\n<b>{_cur_name[cur]}</b>\n"
                f"Игр сыграно: <b>{d['games']}</b>\n"
                f"Удачных игр: <b>{luck}%</b>\n"
                f"Всего выиграл: <b>{fmt_n(d['won_total'])}</b> {e}\n"
                f"Всего проиграл: <b>{fmt_n(d['bet_total'])}</b> {e}")
        # последние операции
        if st["last"]:
            lines.append("\n<b>Последние игры:</b>")
            for r in st["last"]:
                gname = _game_name.get(r["case_key"], r["case_key"])
                e = sx.get("e_" + r["currency"], "")
                profit = r["won"] - r["cost"]
                sign = "🟢 +" if profit > 0 else ("🔴 " if profit < 0 else "⚪️ ")
                lines.append(f"{gname}: ставка {r['cost']:,} {e} · {sign}{abs(profit):,} {e}"
                             .replace(",", " "))
    kb2 = InlineKeyboardBuilder()
    await btn(kb2, "Назад", "casino", "back")
    kb2.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb2.as_markup())
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
            await ui.edit(c.message, f"<b>{title}</b>\n\n<blockquote>{f} открываем…</blockquote>")
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
    u = await db.get_user(c.from_user.id)
    style = u["wheel_anim"] if u and "wheel_anim" in u else "runner"
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"🎡 <b>Рулетка</b>\n\n"
        f"Крути колесо — множитель ×0…×50 на ставку!\n"
        f"Сектора: ×0.5, ×1, ×2, ×3, ×5, ×10 и джекпот <b>×50</b>.\n\n"
        f"Баланс: {sx['e_mushrooms']} {b['mushrooms']:,} · {sx['e_coins']} {b['coins']:,}\n\n"
        f"Введи ставку от <b>{WHEEL_MIN_BET:,}</b> до <b>{WHEEL_MAX_BET:,}</b> грибов "
        f"(коины по курсу). Например <code>5000</code> или <code>5к</code>."
        .replace(",", " "),
        reply_markup=await kb.wheel_menu(style))
    await c.answer()


@router.callback_query(F.data == "wheel_anim_toggle")
async def cb_wheel_anim(c: CallbackQuery, state: FSMContext):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    u = await db.get_user(c.from_user.id)
    cur_style = u["wheel_anim"] if u and "wheel_anim" in u else "runner"
    new_style = "drum" if cur_style == "runner" else "runner"
    await db.set_wheel_anim(c.from_user.id, new_style)
    name = "барабан (как в ежедневной)" if new_style == "drum" else "бегунок с иксами"
    await c.answer(f"Анимация: {name}")
    # перерисуем меню
    await cb_wheel(c, state)


@router.message(Wheel.bet)
async def wheel_bet_input(msg: Message, state: FSMContext):
    if not await casino_visible(msg.from_user.id):
        return await state.clear()
    from services.amount_parse import parse_amount
    bet = parse_amount(msg.text or "")
    if bet is None or bet <= 0:
        return await ui.answer_smart(msg, msg.from_user.id, "Нужно число. Например <code>5000</code> или <code>5к</code>.")
    if not casino.wheel_bet_ok(bet):
        return await ui.answer_smart(msg, msg.from_user.id,
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
                      else ui.answer_smart(target, uid, txt, reply_markup=await kb.wheel_back()))

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

    u = await db.get_user(uid)
    style = u["wheel_anim"] if u and "wheel_anim" in u else "runner"
    await _animate_wheel(target, uid, bet_cur, cur, mult, won, new_bal, edit, style)


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


async def _animate_wheel(target, uid, bet_cur, cur, mult, won, new_bal, edit, style="runner"):
    e_cur = await settings.emoji_html(cur)

    async def show(text, kb_markup=None):
        if edit:
            return await ui.edit(target, text, reply_markup=kb_markup)
        # первый показ — эфемерно, если игрок в группе с эфемерным меню; дальше edit
        return await ui.answer_smart(target, uid, text, reply_markup=kb_markup)

    if style == "drum":
        # барабан как в ежедневной рулетке: окно эмодзи + растущая полоса
        import roulette
        msg = None
        n_frames = 8
        for i in range(n_frames):
            frame = roulette.frame(i, "🎡")
            with contextlib.suppress(Exception):
                m = await show(frame)
                if m:
                    msg = m
            if not edit and msg:
                edit = True
                target = msg
            await asyncio.sleep(0.35 if i < 5 else 0.5)
    else:
        # бегунок по ленте секторов с иксами
        label = ("×0" if mult == 0 else f"×{mult:g}")
        try:
            stop = _WHEEL_STRIP.index(label)
        except ValueError:
            stop = 0
        # короче: ~6 кадров как в ежедневной (было 16 ≈ 6.5с, стало ≈ 2.7с).
        # быстрый бег + аккуратный подвод к результату на последних кадрах.
        N = len(_WHEEL_STRIP)
        positions = [2, 7, 11, (stop - 1) % N, stop, stop]
        msg = None
        for i, pos in enumerate(positions):
            speed = "🎰" if i < 3 else ("⏳" if i < 5 else "🎯")
            frame = f"🎡 <b>Рулетка</b>\n\n<blockquote>{speed}  {_wheel_frame(pos)}</blockquote>"
            with contextlib.suppress(Exception):
                m = await show(frame)
                if m:
                    msg = m
            if not edit and msg:
                edit = True
                target = msg
            await asyncio.sleep(0.45)

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


# ---------------- карточки (mines) ----------------
@router.callback_query(F.data == "mines")
async def cb_mines(c: CallbackQuery, state: FSMContext):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    await state.clear()
    from config import MINES_BETS
    b = await db.balances(c.from_user.id)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"🃏 <b>Карточки</b>\n\n"
        f"Открывай карты: 💎 — множитель растёт, 💣 — ставка сгорела.\n"
        f"Можешь забрать выигрыш в любой момент или рискнуть дальше.\n\n"
        f"Баланс: {sx['e_mushrooms']} {b['mushrooms']:,} · {sx['e_coins']} {b['coins']:,}\n\n"
        f"Выбери ставку:".replace(",", " "),
        reply_markup=await kb.mines_bet(MINES_BETS))
    await c.answer()


@router.callback_query(F.data.startswith("mbet:"))
async def cb_mines_bet(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    from config import MINES_PRESETS
    bet = int(c.data.split(":")[1])
    await ui.edit(c.message,
        f"🃏 <b>Карточки</b>\n\nСтавка: <b>{bet:,}</b>\n\nВыбери поле:".replace(",", " "),
        reply_markup=await kb.mines_field(MINES_PRESETS, bet))
    await c.answer()


@router.callback_query(F.data.startswith("mfield:"))
async def cb_mines_field(c: CallbackQuery, state: FSMContext):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    _, bet_s, key = c.data.split(":")
    bet = int(bet_s)
    await _mines_start(c, state, bet, key)


async def _mines_start(c, state, bet: int, key: str):
    """Начать раунд: списать ставку, создать поле, показать сетку."""
    uid = c.from_user.id
    if await db.is_banned(uid):
        return await c.answer("Ты заблокирован.", show_alert=True)
    total, mines, label = casino.mines_preset(key)
    u = await db.get_user(uid)
    cur = u["currency"]
    bet_cur = bet * COIN_RATE if cur == "coins" else bet

    b = await db.balances(uid)
    if b[cur] < bet_cur:
        sx = await settings.ctx()
        return await c.answer(
            f"Не хватает: ставка {bet_cur:,}, у тебя {b[cur]:,}.".replace(",", " "),
            show_alert=True)

    # списываем ставку сразу
    import time
    idem = f"mines:{uid}:{int(time.time()*1000)}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, uid, cur, -bet_cur, "mines_bet", idem + ":bet")
    except asyncpg.CheckViolationError:
        return await c.answer("Не хватает баланса.", show_alert=True)
    except asyncpg.UniqueViolationError:
        return await c.answer("Повтори ещё раз.", show_alert=True)

    field = casino.mines_new_field(total, mines)
    # состояние игры в FSM
    await state.update_data(mines={
        "field": field, "total": total, "mines": mines, "key": key,
        "bet": bet, "bet_cur": bet_cur, "cur": cur, "opened": [], "idem": idem,
    })
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"🃏 <b>Карточки</b> · {label}\n\n"
        f"Ставка: {bet_cur:,} {sx['e_' + cur]}\n"
        f"Открой карту 👇".replace(",", " "),
        reply_markup=await kb.mines_grid(total, {}, 1.0, False))
    await c.answer()


@router.callback_query(F.data == "mnoop")
async def cb_mines_noop(c: CallbackQuery):
    await c.answer()


@router.callback_query(F.data.startswith("mopen:"))
async def cb_mines_open(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    g = data.get("mines")
    if not g:
        return await c.answer("Игра не активна. Начни заново.", show_alert=True)
    idx = int(c.data.split(":")[1])
    if idx in g["opened"]:
        return await c.answer()
    uid = c.from_user.id
    field = g["field"]
    sx = await settings.ctx()

    if field[idx] == 0:
        # ТЕСТ-режим: для MINES_NOLOSE_IDS бомба не проигрывает — всплывашка и мимо.
        from config import MINES_NOLOSE_IDS
        if uid in MINES_NOLOSE_IDS:
            return await c.answer("💣 Бомба! (тест: не считается)", show_alert=False)
        # БОМБА — проигрыш, показываем всё поле
        _mines_cancel_redraw(uid)  # отменить отложенную перерисовку
        opened = {i: ("💣" if field[i] == 0 else "💎") for i in range(g["total"])}
        await db.audit(uid, "mines_lose", {"bet": g["bet_cur"], "cur": g["cur"], "key": g["key"]})
        await db.log_case_open(uid, "mines", g["cur"], g["bet_cur"], 0, 0.0)
        await state.update_data(mines=None)
        await ui.edit(c.message,
            f"💥 <b>Бомба!</b>\n\n"
            f"Ставка {g['bet_cur']:,} {sx['e_' + g['cur']]} сгорела.\n"
            f"Попробуешь ещё?".replace(",", " "),
            reply_markup=await kb.mines_after())
        # сохраним последнюю конфигурацию для «играть ещё»
        await state.update_data(last_mines={"bet": g["bet"], "key": g["key"]})
        return await c.answer("Бомба!")

    # АЛМАЗ — открываем, множитель растёт.
    # Логику делаем МГНОВЕННО (запись + ответ игроку), а перерисовку сетки —
    # с дебаунсом: быстрые тапы схлопываются в одну правку, чтобы не упереться
    # во флуд-лимит Telegram на editMessageText (тогда карты «не открывались»).
    g["opened"].append(idx)
    picks = len(g["opened"])
    mult = casino.mines_multiplier(g["total"], g["mines"], picks)
    await state.update_data(mines=g)
    await c.answer(f"💎 ×{mult:g}")  # мгновенный отклик, не лимитируется

    safe = g["total"] - g["mines"]
    if picks >= safe:
        # открыл все алмазы — авто-выигрыш (отменим отложенную отрисовку)
        _mines_cancel_redraw(uid)
        return await _mines_cashout(c, state, forced=True)

    # запланировать перерисовку (схлопывает частые тапы)
    _mines_schedule_redraw(c, state, uid)


# ---- дебаунс-отрисовка поля карточек ----
_mines_redraw_tasks: dict[int, asyncio.Task] = {}


def _mines_cancel_redraw(uid: int):
    t = _mines_redraw_tasks.pop(uid, None)
    if t and not t.done():
        t.cancel()


def _mines_schedule_redraw(c, state, uid: int, delay: float = 0.4):
    """Отложить перерисовку поля на delay сек, отменив прошлую отложенную."""
    _mines_cancel_redraw(uid)
    _mines_redraw_tasks[uid] = asyncio.create_task(_mines_redraw(c, state, uid, delay))


async def _mines_redraw(c, state, uid: int, delay: float):
    try:
        await asyncio.sleep(delay)
        data = await state.get_data()
        g = data.get("mines")
        if not g:
            return
        picks = len(g["opened"])
        safe = g["total"] - g["mines"]
        if picks >= safe or picks == 0:
            return
        mult = casino.mines_multiplier(g["total"], g["mines"], picks)
        win_now = int(g["bet_cur"] * mult)
        sx = await settings.ctx()
        opened_map = {i: "💎" for i in g["opened"]}
        with contextlib.suppress(Exception):
            await ui.edit(c.message,
                f"🃏 <b>Карточки</b> · {casino.mines_preset(g['key'])[2]}\n\n"
                f"Открыто: {picks} · множитель <b>×{mult:g}</b>\n"
                f"Заберёшь сейчас: <b>{win_now:,}</b> {sx['e_' + g['cur']]}".replace(",", " "),
                reply_markup=await kb.mines_grid(g["total"], opened_map, mult, True))
    except asyncio.CancelledError:
        pass
    finally:
        _mines_redraw_tasks.pop(uid, None)


@router.callback_query(F.data == "mcashout")
async def cb_mines_cashout(c: CallbackQuery, state: FSMContext):
    await _mines_cashout(c, state, forced=False)


async def _mines_cashout(c, state, forced: bool):
    data = await state.get_data()
    g = data.get("mines")
    if not g:
        return await c.answer("Игра не активна.", show_alert=True)
    uid = c.from_user.id
    _mines_cancel_redraw(uid)  # отменить отложенную перерисовку поля
    picks = len(g["opened"])
    if picks == 0:
        return await c.answer("Открой хотя бы одну карту.", show_alert=True)
    mult = casino.mines_multiplier(g["total"], g["mines"], picks)
    won = int(g["bet_cur"] * mult)
    cur = g["cur"]
    sx = await settings.ctx()

    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                new_bal = await db.apply(conn, uid, cur, won, "mines_win", g["idem"] + ":win")
    except asyncpg.UniqueViolationError:
        return await c.answer("Уже забрано.", show_alert=True)

    await db.audit(uid, "mines_win", {"bet": g["bet_cur"], "mult": mult, "won": won, "cur": cur})
    await db.log_case_open(uid, "mines", cur, g["bet_cur"], won, mult)
    await state.update_data(mines=None, last_mines={"bet": g["bet"], "key": g["key"]})

    profit = won - g["bet_cur"]
    opened_map = {i: "💎" for i in g["opened"]}
    head = "🏆 <b>Все алмазы собраны!</b>" if forced else "💰 <b>Забрал!</b>"
    await ui.edit(c.message,
        f"{head}\n\n"
        f"Открыто {picks} · множитель ×{mult:g}\n"
        f"Выигрыш: <b>{won:,}</b> {sx['e_' + cur]} (в плюс +{profit:,})\n"
        f"Баланс: <b>{new_bal:,}</b> {sx['e_' + cur]}".replace(",", " "),
        reply_markup=await kb.mines_after())
    await c.answer("Забрал!")


@router.callback_query(F.data == "magain")
async def cb_mines_again(c: CallbackQuery, state: FSMContext):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    data = await state.get_data()
    last = data.get("last_mines")
    if not last:
        return await cb_mines(c, state)
    await _mines_start(c, state, last["bet"], last["key"])


# ---------------- админ: переключатель доступа ----------------
@router.callback_query(F.data == "casino_admin")
async def cb_casino_admin(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    anim = await settings.get("chat.anim_ephemeral", "0") == "1"
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        "🔴 Только админ — клиенты не видят кнопку «Казино».\n"
        "🟢 Открыто всем — кнопка появляется у всех.\n\n"
        "<i>Анимация в чате приватно</i> — тестовая фича: анимация игры видна "
        "только игроку (эфемерно), результат — всем. Если глючит — выключи.",
        reply_markup=await kb.casino_admin_toggle(await casino_enabled(), anim))
    await c.answer()


@router.callback_query(F.data == "casino_anim_toggle")
async def cb_casino_anim_toggle(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    new = "0" if await settings.get("chat.anim_ephemeral", "0") == "1" else "1"
    await settings.set("chat.anim_ephemeral", new, c.from_user.id)
    await c.answer("Готово")
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        + ("🟢 Приватная анимация включена (тест)." if new == "1"
           else "🔴 Приватная анимация выключена — всё публично."),
        reply_markup=await kb.casino_admin_toggle(await casino_enabled(), new == "1"))


@router.callback_query(F.data == "casino_toggle")
async def cb_casino_toggle(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    new = "0" if await casino_enabled() else "1"
    await settings.set("casino.enabled", new, c.from_user.id)
    await db.audit(c.from_user.id, "casino_toggle", {"enabled": new})
    anim = await settings.get("chat.anim_ephemeral", "0") == "1"
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        + ("🟢 Теперь открыто всем." if new == "1" else "🔴 Теперь только админ."),
        reply_markup=await kb.casino_admin_toggle(new == "1", anim))
    await c.answer("Готово")
