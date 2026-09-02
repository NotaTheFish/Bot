"""
Морской бой: команда, лобби на 2 игроков, вход в расстановку.
Команда: !шайн мб <ставка> <валюта>.
  reply на сообщение игрока -> прямой вызов его.
  без reply -> открытая онлайн-игра (появится в браузере «Найти игру»).
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, settings, ui
from services.amount_parse import parse_amount

router = Router()

GAME = "navy"


class NavyNew(StatesGroup):
    amount = State()    # ввод ставки
    invite = State()    # ввод ника соперника (direct)
# сообщения поиска: mid -> (chat_id, message_id) — чтобы погасить кнопки при связывании
_search_msgs: dict[int, tuple] = {}
_CUR_WORDS = {
    "грибы": "mushrooms", "гриб": "mushrooms", "грибов": "mushrooms",
    "коины": "coins", "коинов": "coins", "коин": "coins",
}


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    from services import profile as _prof
    return await _prof.display_name(uid)


async def _guard_casino(uid: int) -> bool:
    from services import casino as casino_svc
    return await casino_svc.visible(uid)


async def _kill_search_msg(bot, mid: int):
    """Погасить сообщение поиска (убрать кнопки «Вступить»/«Отменить»), когда
    соперник уже присоединился — чтобы нельзя было отменить активную игру."""
    loc = _search_msgs.pop(mid, None)
    if loc:
        with contextlib.suppress(Exception):
            await bot.edit_message_text("⚓ Соперник найден! Бой начался.",
                                        chat_id=loc[0], message_id=loc[1])


async def handle_shine_navy(msg: Message, parts: list[str]):
    """!шайн мб <ставка> <валюта> [reply -> прямой вызов]."""
    if not await _guard_casino(msg.from_user.id):
        return
    bet = parse_amount(parts[1]) if len(parts) > 1 else None
    cur = None
    for w in parts[2:]:
        if w.lower() in _CUR_WORDS:
            cur = _CUR_WORDS[w.lower()]
            break
    if bet is None or bet <= 0 or cur is None:
        return await ui.reply(msg, "Формат: <code>!шайн мб ставка валюта</code>. "
                                   "Пример: <code>!шайн мб 5000 грибы</code>")
    uid = msg.from_user.id
    if await matches.has_active(uid):
        return await ui.reply(msg, "У тебя уже есть активная игра. Заверши или отмени её.")
    b = await db.balances(uid)
    if b.get(cur, 0) < bet:
        sx = await settings.ctx()
        return await ui.reply(msg, f"Не хватает: на балансе {_fmt(b[cur])} {sx['e_' + cur]}.")

    # reply -> прямой вызов конкретного игрока
    opponent = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        opp = msg.reply_to_message.from_user
        if opp.is_bot:
            return await ui.reply(msg, "Нельзя вызвать бота.")
        if opp.id == uid:
            return await ui.reply(msg, "С самим собой нельзя 🙂")
        opponent = opp.id

    mode = "direct" if opponent else "online"
    # ОНЛАЙН: сначала ищем уже открытую игру с той же ставкой/валютой — свяжем сразу
    if not opponent:
        existing = await matches.navy_find_open(cur, bet, uid)
        if existing:
            m2, err2 = await matches.navy_join(existing, uid)
            if not err2:
                await _kill_search_msg(msg.bot, existing)
                sx = await settings.ctx()
                e = sx["e_" + cur]
                await ui.send(msg.bot, msg.chat.id,
                    f"⚓ <b>Соперник найден!</b> Бой начинается — расставляйте корабли (15 мин).")
                from handlers.navy_place import start_placement
                with contextlib.suppress(Exception):
                    await start_placement(msg.bot, existing)
                from handlers.lobby_browser import refresh_all
                with contextlib.suppress(Exception):
                    await refresh_all(msg.bot, GAME)
                return
            # если присоединиться не вышло (баланс/гонка) — падаем в обычное создание

    mid, err = await matches.create_navy(uid, cur, bet, mode, opponent, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")

    sx = await settings.ctx()
    e = sx["e_" + cur]
    challenger = await _name(uid)
    if opponent:
        # прямой вызов — приглашение в чат
        oppname = await _name(opponent)
        k = InlineKeyboardBuilder()
        from services.ui import btn
        await btn(k, "⚓ Принять бой", f"navy_accept:{mid}")
        await btn(k, "❌ Отклонить", f"navy_decline:{mid}")
        k.adjust(2)
        await ui.send(msg.bot, msg.chat.id,
            f"⚓ <b>Морской бой!</b>\n\n{challenger} вызывает {oppname}.\n"
            f"Ставка: <b>{_fmt(bet)}</b> {e}\n\n{oppname}, принимаешь бой?",
            reply_markup=k.as_markup())
    else:
        # онлайн — открытый вызов: кнопки «Вступить» + «Отменить», плюс в браузере
        k = InlineKeyboardBuilder()
        from services.ui import btn
        await btn(k, "⚓ Вступить в бой", f"navy_join:{mid}")
        await btn(k, "❌ Отменить поиск", f"navy_cancel:{mid}")
        k.adjust(1, 1)
        sent = await ui.send(msg.bot, msg.chat.id,
            f"⚓ <b>Морской бой — ищу соперника…</b>\n\n"
            f"{challenger}, ставка <b>{_fmt(bet)}</b> {e}.\n"
            f"Средства заморожены. Поиск до 10 минут.\n\n"
            f"Любой может нажать «Вступить в бой».",
            reply_markup=k.as_markup())
        if sent:
            _search_msgs[mid] = (msg.chat.id, sent.message_id)
        from handlers.lobby_browser import refresh_all
        with contextlib.suppress(Exception):
            await refresh_all(msg.bot, GAME)


@router.callback_query(F.data.startswith("navy_accept:"))
async def cb_accept(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.accept_invite(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    # морской бой: после принятия — фаза расстановки (не сразу бой)
    from datetime import datetime, timedelta, timezone
    dl = datetime.now(timezone.utc) + timedelta(minutes=matches.NAVY_PLACE_MINUTES)
    await db.pool().execute(
        "UPDATE rb_matches SET status='placement', move_deadline=$1 WHERE id=$2", dl, mid)
    await c.answer("Бой начинается! Расставляй корабли.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("⚓ Бой принят! Оба игрока расставляют корабли (15 мин).",
                                  reply_markup=None)
    from handlers.navy_place import start_placement
    with contextlib.suppress(Exception):
        await start_placement(c.bot, mid)


@router.callback_query(F.data.startswith("navy_decline:"))
async def cb_decline(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] not in ("invited", "searching"):
        return await c.answer("Уже неактуально.", show_alert=True)
    await matches.decline_invite(mid, c.from_user.id)
    await c.answer("Вызов отклонён.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Вызов на морской бой отклонён. Ставка возвращена.",
                                  reply_markup=None)


@router.callback_query(F.data.startswith("navy_cancel:"))
async def cb_cancel(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.cancel(mid, c.from_user.id)
    if err:
        # игра уже началась (соперник присоединился) — отменять нельзя
        await c.answer(f"⚠️ {err}", show_alert=True)
        with contextlib.suppress(Exception):
            await c.message.edit_reply_markup(reply_markup=None)
        return
    await c.answer("Поиск отменён, ставка возвращена.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Поиск морского боя отменён. Ставка возвращена.",
                                  reply_markup=None)
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


@router.callback_query(F.data.startswith("navy_join:"))
async def cb_navy_join(c: CallbackQuery):
    """Вступить в открытый морской бой (из сообщения поиска или из браузера)."""
    mid = int(c.data.split(":")[1])
    m, err = await matches.navy_join(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Бой начинается! Расставляй корабли.")
    await _kill_search_msg(c.bot, mid)
    # убрать кнопки у сообщения поиска / браузера
    with contextlib.suppress(Exception):
        await c.message.edit_text("⚓ Соперник найден! Расставляйте корабли (15 мин).",
                                  reply_markup=None)
    from handlers.navy_place import start_placement
    with contextlib.suppress(Exception):
        await start_placement(c.bot, mid)
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


# ==================== СОЗДАНИЕ В ЛИЧКЕ (navy_new_dm) ====================
async def _b(kb, text, cb, slot=None):
    from services.ui import btn
    await btn(kb, text, cb, slot)


@router.callback_query(F.data == "navy_new_dm")
async def cb_navy_new_dm(c: CallbackQuery):
    """Экран морского боя в личке: создать (валюта→ставка→онлайн/вызов) или найти."""
    if not await _guard_casino(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    if await matches.has_active(c.from_user.id):
        return await c.answer("У тебя уже есть активная игра.", show_alert=True)
    b = await db.balances(c.from_user.id)
    k = InlineKeyboardBuilder()
    await _b(k, "🔍 Найти игру", "lb_open:navy")
    await _b(k, f"🍄 Грибы ({_fmt(b['mushrooms'])})", "navycur:mushrooms")
    await _b(k, f"🪙 Коины ({_fmt(b['coins'])})", "navycur:coins")
    await _b(k, "Назад", "games_pvp", "back")
    k.adjust(1)
    await ui.edit(c.message,
        "⚓ <b>Морской бой</b>\n\n"
        "Игра на двоих со ставкой. Потопи весь флот противника, чтобы забрать банк "
        "(казино берёт 3%).\n\n"
        "Создай игру (выбери валюту) или найди открытую.",
        reply_markup=k.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("navycur:"))
async def cb_navy_cur(c: CallbackQuery, state: FSMContext):
    cur = c.data.split(":")[1]
    await state.update_data(cur=cur)
    await state.set_state(NavyNew.amount)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"⚓ <b>Морской бой</b>\n\nВалюта: {sx['e_' + cur]}\n\nВведи ставку числом:",
        reply_markup=None)
    await c.answer()


@router.message(NavyNew.amount)
async def msg_navy_amount(msg: Message, state: FSMContext):
    amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.reply(msg, "Нужно положительное число. Введи ставку:")
    data = await state.get_data()
    cur = data["cur"]
    b = await db.balances(msg.from_user.id)
    if b.get(cur, 0) < amount:
        sx = await settings.ctx()
        return await ui.reply(msg, f"Не хватает: {_fmt(b[cur])} {sx['e_' + cur]}. Введи другую:")
    await state.update_data(amount=amount)
    await state.set_state(None)
    k = InlineKeyboardBuilder()
    await _b(k, "🌐 Онлайн (открытая игра)", "navymode:online")
    await _b(k, "👤 Позвать по нику", "navymode:direct")
    await _b(k, "Отмена", "games_pvp", "back")
    k.adjust(1)
    sx = await settings.ctx()
    await ui.reply(msg,
        f"⚓ <b>Морской бой</b>\n\nСтавка: <b>{_fmt(amount)}</b> {sx['e_' + cur]}\n\nКак играем?",
        reply_markup=k.as_markup())


@router.callback_query(F.data == "navymode:online")
async def cb_navy_mode_online(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cur, amount = data.get("cur"), data.get("amount")
    await state.set_state(None)
    if not cur or not amount:
        return await c.answer("Данные потеряны, начни заново.", show_alert=True)
    # сначала ищем открытую игру с той же ставкой — свяжем
    existing = await matches.navy_find_open(cur, amount, c.from_user.id)
    if existing:
        m2, err2 = await matches.navy_join(existing, c.from_user.id)
        if not err2:
            await _kill_search_msg(c.bot, existing)
            with contextlib.suppress(Exception):
                await c.message.edit_text("⚓ Соперник найден! Расставляйте корабли (15 мин).",
                                          reply_markup=None)
            from handlers.navy_place import start_placement
            with contextlib.suppress(Exception):
                await start_placement(c.bot, existing)
            return await c.answer("Соперник найден!")
    # иначе создаём открытую игру
    mid, err = await matches.create_navy(c.from_user.id, cur, amount, "online", None, c.message.chat.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    sx = await settings.ctx()
    e = sx["e_" + cur]
    k = InlineKeyboardBuilder()
    await _b(k, "❌ Отменить поиск", f"navy_cancel:{mid}")
    with contextlib.suppress(Exception):
        await c.message.edit_text(
            f"⚓ <b>Морской бой — ищу соперника…</b>\n\nСтавка <b>{_fmt(amount)}</b> {e}.\n"
            f"Средства заморожены. Другие увидят игру в «Найти игру».",
            reply_markup=k.as_markup())
    await c.answer("Игра создана!")
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


@router.callback_query(F.data == "navymode:direct")
async def cb_navy_mode_direct(c: CallbackQuery, state: FSMContext):
    await state.set_state(NavyNew.invite)
    await ui.edit(c.message,
        "👤 <b>Позвать по нику</b>\n\nНапиши @ник или ID соперника:",
        reply_markup=None)
    await c.answer()


@router.message(NavyNew.invite)
async def msg_navy_invite(msg: Message, state: FSMContext):
    data = await state.get_data()
    cur, amount = data.get("cur"), data.get("amount")
    await state.set_state(None)
    if not cur or not amount:
        return await ui.reply(msg, "Данные потеряны, начни заново.")
    raw = (msg.text or "").strip().lstrip("@")
    opp = await db.resolve_username(raw.lower())
    if not opp and raw.isdigit():
        opp = int(raw)
    if not opp or opp == msg.from_user.id:
        return await ui.reply(msg, "Не нашёл игрока. Он должен был писать боту. "
                                   "Попробуй снова или создай онлайн-игру.")
    mid, err = await matches.create_navy(msg.from_user.id, cur, amount, "direct", opp, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    sx = await settings.ctx()
    e = sx["e_" + cur]
    challenger = await _name(msg.from_user.id)
    oppname = await _name(opp)
    k = InlineKeyboardBuilder()
    await _b(k, "⚓ Принять бой", f"navy_accept:{mid}")
    await _b(k, "❌ Отклонить", f"navy_decline:{mid}")
    k.adjust(2)
    # приглашение шлём сопернику в ЛС
    sent = await ui.send(msg.bot, opp,
        f"⚓ <b>Морской бой!</b>\n\n{challenger} вызывает тебя.\n"
        f"Ставка: <b>{_fmt(amount)}</b> {e}\n\nПринимаешь?",
        reply_markup=k.as_markup())
    if sent:
        await ui.reply(msg, f"Вызов отправлен {oppname}. Ждём ответа.")
    else:
        await ui.reply(msg, f"Не смог написать {oppname} — возможно, он не запускал бота. "
                            f"Ставка вернётся, если он не ответит за 10 минут.")
