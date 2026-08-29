"""
Камень-ножницы-бумага: лобби (набор игроков), старт, раунды.
Мультиплеер до 5 человек на выбывание. Команда: !шайн кнб <ставка> <валюта>.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, rps, settings, ui
from services.amount_parse import parse_amount

router = Router()

GAME = "rps"


class RPSNew(StatesGroup):
    amount = State()      # ввод ставки
    invite = State()      # ввод ников для direct-режима
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
    return (f"🤓😎🥸 <b>Камень-ножницы-бумага</b>\n\n"
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
    # обновить список открытых игр у всех, кто смотрит браузер
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


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
    # уведомить остальных участников (кроме создателя, он и так видит)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    for uid in m.get("refunded", []):
        if uid == c.from_user.id:
            continue
        with contextlib.suppress(Exception):
            await ui.send(c.bot, uid,
                f"❌ Игра камень-ножницы-бумага отменена создателем.\n"
                f"Ставка <b>{_fmt(m['stake'])}</b> {e} возвращена.")
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


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
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


# ---------------- ТЕСТ Rich Messages ----------------
@router.message(F.text.lower() == "!ричтест")
async def cmd_rich_test(msg: Message):
    import logging
    log = logging.getLogger("refbot")
    log.info("!ричтест от %s в чате %s", msg.from_user.id, msg.chat.id)
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
            msg.bot, msg.chat.id, "🤓😎🥸 <b>Открытые игры (тест)</b>", rows,
            fallback_kb=fb.as_markup())
        log.info("!ричтест: отправлено, is_rich=%s", is_rich)
        await msg.reply(f"Отправил. Rich-режим: {'ДА ✅' if is_rich else 'НЕТ (откат на инлайн) ⚠️'}")
    except Exception as e:
        log.exception("!ричтест упал")
        await msg.reply(f"Ошибка: {type(e).__name__}: {e}")


@router.callback_query(F.data.startswith("rps_test:"))
async def cb_rps_test(c: CallbackQuery):
    await c.answer(f"Кнопка работает! Ты нажал игру #{c.data.split(':')[1]}", show_alert=True)


@router.callback_query(F.data == "rps_new_dm")
async def cb_rps_new_dm(c: CallbackQuery):
    """Экран КНБ в личке: создать игру или найти открытую."""
    if not await _guard_casino(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    if await matches.has_active(c.from_user.id):
        return await c.answer("У тебя уже есть активная игра. Заверши или отмени её.",
                              show_alert=True)
    b = await db.balances(c.from_user.id)
    k = InlineKeyboardBuilder()
    await _b(k, "🔍 Найти игру", "lb_open:rps")
    await _b(k, f"🍄 Грибы ({_fmt(b['mushrooms'])})", "rpscur:mushrooms")
    await _b(k, f"🪙 Коины ({_fmt(b['coins'])})", "rpscur:coins")
    await _b(k, "Назад", "casino_games", "back")
    k.adjust(1)
    await ui.edit(c.message,
        "🤓😎🥸 <b>Камень-ножницы-бумага</b>\n\n"
        "Мультиплеер до 5 игроков на выбывание. Победитель забирает банк "
        "(казино берёт 3% с проигравших ставок).\n\n"
        "Создай игру (выбери валюту) или найди открытую.",
        reply_markup=k.as_markup())
    await c.answer()


async def _b(kb, text, cb, slot=None):
    from services.ui import btn
    await btn(kb, text, cb, slot)


# ---------------- FSM создания КНБ в ЛС ----------------
@router.callback_query(F.data.startswith("rpscur:"))
async def cb_rps_cur(c: CallbackQuery, state: FSMContext):
    cur = c.data.split(":")[1]
    await state.update_data(cur=cur)
    await state.set_state(RPSNew.amount)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"🤓😎🥸 <b>Камень-ножницы-бумага</b>\n\n"
        f"Валюта: {sx['e_' + cur]}\n\nВведи ставку числом (например 1000):",
        reply_markup=None)
    await c.answer()


@router.message(RPSNew.amount)
async def msg_rps_amount(msg: Message, state: FSMContext):
    amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.reply(msg, "Нужно положительное число. Введи ставку:")
    data = await state.get_data()
    cur = data["cur"]
    b = await db.balances(msg.from_user.id)
    if b.get(cur, 0) < amount:
        sx = await settings.ctx()
        return await ui.reply(msg, f"Не хватает: на балансе {_fmt(b[cur])} {sx['e_' + cur]}. "
                                   f"Введи другую ставку:")
    await state.update_data(amount=amount)
    await state.set_state(None)
    # выбор режима: онлайн-пул или позвать по никам
    k = InlineKeyboardBuilder()
    await _b(k, "🌐 Онлайн (открытая игра)", "rpsmode:online")
    await _b(k, "👥 Позвать по нику", "rpsmode:direct")
    await _b(k, "Отмена", "casino_games", "back")
    k.adjust(1)
    sx = await settings.ctx()
    await ui.reply(msg,
        f"🤓😎🥸 <b>Камень-ножницы-бумага</b>\n\n"
        f"Ставка: <b>{_fmt(amount)}</b> {sx['e_' + cur]}\n\n"
        f"Как играем?\n"
        f"• <b>Онлайн</b> — создашь открытую игру, другие найдут и вступят\n"
        f"• <b>Позвать по нику</b> — пригласишь конкретных игроков",
        reply_markup=k.as_markup())


# ---------------- режим: онлайн (открытое лобби) ----------------
@router.callback_query(F.data == "rpsmode:online")
async def cb_rps_online(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cur, amount = data.get("cur"), data.get("amount")
    await state.set_state(None)
    if not cur or not amount:
        return await c.answer("Данные потеряны, начни заново.", show_alert=True)
    # создаём открытое лобби (в личке создателя); появится в браузере «Найти игру»
    mid, err = await matches.create_lobby(GAME, c.from_user.id, cur, amount, c.message.chat.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    sent = await ui.send(c.bot, c.message.chat.id, await _lobby_text(mid),
                         reply_markup=await _lobby_kb(mid))
    if sent:
        await db.pool().execute(
            "UPDATE rb_matches SET board_chat_id=$1, board_msg_id=$2 WHERE id=$3",
            c.message.chat.id, sent.message_id, mid)
    with contextlib.suppress(Exception):
        await c.message.edit_text("✅ Открытая игра создана! Жди игроков или позови друзей.",
                                  reply_markup=None)
    await c.answer("Игра создана!")
    from handlers.lobby_browser import refresh_all
    import contextlib as _cl
    with _cl.suppress(Exception):
        await refresh_all(c.bot, GAME)


# ---------------- режим: direct (позвать по никам) ----------------
@router.callback_query(F.data == "rpsmode:direct")
async def cb_rps_direct(c: CallbackQuery, state: FSMContext):
    await state.set_state(RPSNew.invite)
    await ui.edit(c.message,
        "👥 <b>Позвать по нику</b>\n\n"
        "Напиши ники через пробел (до 4 игроков):\n"
        "<code>@user1 @user2 @user3</code>\n\n"
        "Каждому придёт приглашение. Твоя ставка заморозится сразу.",
        reply_markup=None)
    await c.answer()


@router.message(RPSNew.invite)
async def msg_rps_invite(msg: Message, state: FSMContext):
    data = await state.get_data()
    cur, amount = data.get("cur"), data.get("amount")
    await state.set_state(None)
    if not cur or not amount:
        return await ui.reply(msg, "Данные потеряны, начни заново.")
    # разобрать ники
    nicks = [w.strip().lstrip("@").lower() for w in (msg.text or "").split() if w.strip()]
    nicks = list(dict.fromkeys(nicks))[:4]   # уникальные, до 4
    if not nicks:
        return await ui.reply(msg, "Не вижу ников. Напиши например: <code>@user1 @user2</code>")
    # резолвим в id
    invited = []
    unknown = []
    for n in nicks:
        uid = await db.resolve_username(n)
        if uid and uid != msg.from_user.id:
            invited.append(uid)
        else:
            unknown.append(n)
    invited = list(dict.fromkeys(invited))
    if not invited:
        return await ui.reply(msg,
            "Никого не нашёл по этим никам. Игроки должны были писать боту. "
            "Попробуй снова или создай онлайн-игру.")
    # создаём лобби и шлём приглашения
    mid, err = await matches.create_lobby(GAME, msg.from_user.id, cur, amount, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    from handlers.rps_direct import send_invites
    await send_invites(msg.bot, mid, msg.from_user.id, invited, unknown)


# ---------------- «Играть ещё» — новое лобби той же ставкой ----------------
@router.callback_query(F.data.startswith("rps_again:"))
async def cb_rps_again(c: CallbackQuery):
    _, cur, stake_s = c.data.split(":")
    stake = int(stake_s)
    uid = c.from_user.id
    if not await _guard_casino(uid):
        return await c.answer("Казино закрыто.", show_alert=True)
    if await matches.has_active(uid):
        return await c.answer("У тебя уже есть активная игра. Заверши или отмени её.",
                              show_alert=True)
    b = await db.balances(uid)
    if b.get(cur, 0) < stake:
        sx = await settings.ctx()
        return await c.answer(f"Не хватает: {_fmt(b.get(cur,0))} {sx['e_'+cur]}.", show_alert=True)
    # создаём новое лобби в том же месте (чат, где была игра, или личка)
    mid, err = await matches.create_lobby(GAME, uid, cur, stake, c.message.chat.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Новая игра создана!")
    sent = await ui.send(c.bot, c.message.chat.id, await _lobby_text(mid),
                         reply_markup=await _lobby_kb(mid))
    if sent:
        await db.pool().execute(
            "UPDATE rb_matches SET board_chat_id=$1, board_msg_id=$2 WHERE id=$3",
            c.message.chat.id, sent.message_id, mid)
    # убрать кнопку «играть ещё» со старого финала
    with contextlib.suppress(Exception):
        await c.message.edit_reply_markup(reply_markup=None)
