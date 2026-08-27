"""
Крестики-нолики: создание игры, приглашения, принятие/отклонение.
Игровое поле и ходы — в этом же файле (Этап 4).
"""
import contextlib

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
import keyboards as kb
from keyboards import btn
from services import matches, ttt, settings, ui
from services.amount_parse import parse_amount

router = Router()

GAME = "ttt"


class TTTNew(StatesGroup):
    amount = State()      # ввод ставки
    opponent = State()    # ввод username/id оппонента (для direct)


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _guard_casino(uid: int) -> bool:
    from services import casino as casino_svc
    return await casino_svc.visible(uid)


async def _name_of(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


# ---------------- создание ----------------
@router.callback_query(F.data == "ttt_new")
async def cb_ttt_new(c: CallbackQuery, state: FSMContext):
    if not await _guard_casino(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    # уже в игре?
    if await matches.has_active(c.from_user.id):
        return await c.answer("У тебя уже есть активная игра. Заверши или отмени её.",
                              show_alert=True)
    await state.set_state(None)
    b = await db.balances(c.from_user.id)
    sx = await settings.ctx()
    k = InlineKeyboardBuilder()
    await btn(k, f"🍄 Грибы ({_fmt(b['mushrooms'])})", "tttcur:mushrooms")
    await btn(k, f"🪙 Коины ({_fmt(b['coins'])})", "tttcur:coins")
    await btn(k, "Назад", "casino_games", "back")
    k.adjust(1)
    await ui.edit(c.message,
        "❌⭕️ <b>Крестики-нолики</b>\n\n"
        "Игра на двоих со ставкой. Победитель забирает банк "
        "(с проигравшей ставки казино берёт 3%). Ничья — ставки возвращаются.\n\n"
        "Чем играем?",
        reply_markup=k.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("tttcur:"))
async def cb_ttt_currency(c: CallbackQuery, state: FSMContext):
    if await matches.has_active(c.from_user.id):
        return await c.answer("У тебя уже есть активная игра.", show_alert=True)
    cur = c.data.split(":")[1]
    await state.update_data(cur=cur)
    await state.set_state(TTTNew.amount)
    sx = await settings.ctx()
    b = await db.balances(c.from_user.id)
    e = sx["e_" + cur]
    await ui.edit(c.message,
        f"✍️ <b>Ставка</b> в {e}\n\n"
        f"Баланс: <b>{_fmt(b[cur])}</b> {e}\n\n"
        f"Оба игрока ставят одинаково. Напиши число (<code>5000</code>, <code>5к</code>).",
        reply_markup=await kb.back_menu())
    await c.answer()


@router.message(TTTNew.amount, ~F.text.startswith("/"), F.text != "☰ Меню")
async def msg_ttt_amount(msg: Message, state: FSMContext):
    amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.answer_smart(msg, msg.from_user.id,
                                     "Не понял сумму. Например <code>5000</code> или <code>5к</code>.")
    data = await state.get_data()
    cur = data.get("cur")
    b = await db.balances(msg.from_user.id)
    if amount > b[cur]:
        sx = await settings.ctx()
        return await ui.answer_smart(msg, msg.from_user.id,
            f"Не хватает: на балансе {_fmt(b[cur])} {sx['e_' + cur]}.")
    await state.update_data(amount=amount)
    await state.set_state(None)
    # выбор режима
    k = InlineKeyboardBuilder()
    await btn(k, "🔍 Найти соперника", "tttmode:online")
    await btn(k, "👤 Позвать по нику/id", "tttmode:direct")
    await btn(k, "Отмена", "casino_games", "back")
    k.adjust(1)
    sx = await settings.ctx()
    await ui.answer_smart(msg, msg.from_user.id,
        f"Ставка: <b>{_fmt(amount)}</b> {sx['e_' + cur]}\n\nКак ищем соперника?",
        reply_markup=k.as_markup())


@router.callback_query(F.data.startswith("tttmode:"))
async def cb_ttt_mode(c: CallbackQuery, state: FSMContext):
    mode = c.data.split(":")[1]
    data = await state.get_data()
    cur, amount = data.get("cur"), data.get("amount")
    if not cur or not amount:
        return await c.answer("Начни заново.", show_alert=True)

    if mode == "online":
        mid, err = await matches.create_match(
            GAME, c.from_user.id, cur, amount, "online",
            origin_chat=c.message.chat.id)
        if err:
            return await c.answer(f"⚠️ {err}", show_alert=True)
        await state.set_state(None)
        k = InlineKeyboardBuilder()
        await btn(k, "❌ Отменить поиск", f"ttt_cancel:{mid}")
        k.adjust(1)
        sx = await settings.ctx()
        await ui.edit(c.message,
            f"🔍 <b>Ищу соперника…</b>\n\n"
            f"Ставка: <b>{_fmt(amount)}</b> {sx['e_' + cur]}\n"
            f"Средства заморожены. Поиск до 10 минут.\n"
            f"Как только кто-то присоединится — начнём.",
            reply_markup=k.as_markup())
        await c.answer("Ищем соперника")
        return

    # direct — просим ник/id
    await state.set_state(TTTNew.opponent)
    await ui.edit(c.message,
        "👤 <b>Кого зовём?</b>\n\n"
        "Пришли <b>@username</b> или <b>id</b> игрока.",
        reply_markup=await kb.back_menu())
    await c.answer()


@router.message(TTTNew.opponent, ~F.text.startswith("/"), F.text != "☰ Меню")
async def msg_ttt_opponent(msg: Message, state: FSMContext):
    data = await state.get_data()
    cur, amount = data.get("cur"), data.get("amount")
    if not cur or not amount:
        await state.set_state(None)
        return await ui.answer_smart(msg, msg.from_user.id, "Начни заново через меню.")
    # разбор username/id
    raw = (msg.text or "").strip()
    opp_id = None
    if raw.lstrip("-").isdigit():
        opp_id = int(raw)
    else:
        uname = raw.lstrip("@").lower()
        opp_id = await db.resolve_username(uname)
    if not opp_id:
        return await ui.answer_smart(msg, msg.from_user.id,
            "Не нашёл такого игрока. Пришли @username или id "
            "(человек должен был писать в чат/боту).")
    if opp_id == msg.from_user.id:
        return await ui.answer_smart(msg, msg.from_user.id, "С самим собой нельзя 🙂")

    # проверим баланс оппонента заранее (дружелюбное сообщение)
    ob = await db.balances(opp_id)
    if ob.get(cur, 0) < amount:
        return await ui.answer_smart(msg, msg.from_user.id,
            "У оппонента недостаточно средств для этой ставки.")

    mid, err = await matches.create_match(
        GAME, msg.from_user.id, cur, amount, "direct", opponent=opp_id,
        origin_chat=msg.chat.id)
    if err:
        return await ui.answer_smart(msg, msg.from_user.id, f"⚠️ {err}")
    await state.set_state(None)
    sx = await settings.ctx()

    # приглашение оппоненту в ЛС
    inv = InlineKeyboardBuilder()
    await btn(inv, "✅ Принять", f"ttt_accept:{mid}")
    await btn(inv, "❌ Отклонить", f"ttt_decline:{mid}")
    inv.adjust(2)
    challenger = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.first_name
    sent_dm = False
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, opp_id,
            f"⚔️ <b>Вызов на крестики-нолики!</b>\n\n"
            f"{challenger} зовёт тебя сыграть.\n"
            f"Ставка: <b>{_fmt(amount)}</b> {sx['e_' + cur]} "
            f"(с проигравшей ставки казино берёт 3%).\n\n"
            f"Принимаешь?",
            reply_markup=inv.as_markup())
        sent_dm = True

    k = InlineKeyboardBuilder()
    await btn(k, "❌ Отменить вызов", f"ttt_cancel:{mid}")
    k.adjust(1)
    note = ("Приглашение отправлено ему в ЛС." if sent_dm
            else "⚠️ Не смог написать ему в ЛС — возможно, он не запускал бота. "
                 "Он всё равно может принять, если откроет бота.")
    await ui.answer_smart(msg, msg.from_user.id,
        f"📨 <b>Вызов отправлен</b>\n\n"
        f"Ставка: <b>{_fmt(amount)}</b> {sx['e_' + cur]}. Средства заморожены.\n"
        f"{note}",
        reply_markup=k.as_markup())


# ---------------- отмена / принятие / отклонение ----------------
@router.callback_query(F.data.startswith("ttt_cancel:"))
async def cb_ttt_cancel(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.cancel(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await ui.edit(c.message, "❌ Игра отменена, ставка возвращена.",
                  reply_markup=await kb.back_menu())
    await c.answer("Отменено")


@router.callback_query(F.data.startswith("ttt_decline:"))
async def cb_ttt_decline(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.decline_invite(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await ui.edit(c.message, "Ты отклонил вызов.", reply_markup=None)
    await c.answer("Отклонено")
    # уведомим создателя
    with contextlib.suppress(Exception):
        await ui.send(c.bot, m["p1"], "❌ Соперник отклонил твой вызов. Ставка возвращена.")


@router.callback_query(F.data.startswith("ttt_accept:"))
async def cb_ttt_accept(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.accept_invite(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Принято! Начинаем.")
    # запуск игры — Этап 4 (пока заглушка-уведомление)
    from handlers.ttt_game import start_game
    with contextlib.suppress(Exception):
        await start_game(c.bot, mid)


# ==================== ИГРА В ОБЩЕМ ЧАТЕ: !шайн кн ====================
_CUR_WORDS = {
    "грибы": "mushrooms", "гриб": "mushrooms", "грибов": "mushrooms",
    "коины": "coins", "коинов": "coins", "коин": "coins",
}
# первое слово после «!шайн» — код игры
_GAME_WORDS = {"кн": "ttt", "крестики": "ttt", "крестики-нолики": "ttt"}


@router.message(F.text.lower().startswith(("!шайн", "/шайн", "!shine")))
async def cmd_shine(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not await _guard_casino(msg.from_user.id):
        return
    t = (msg.text or "").strip()
    low = t.lower()
    for pref in ("!шайн", "/шайн", "!shine"):
        if low.startswith(pref):
            rest = t[len(pref):].strip()
            break
    else:
        return
    parts = rest.split()
    # реагируем ТОЛЬКО на игровые подкоманды (!шайн кн ...). Голый !шайн и прочее —
    # это рулетка, её обрабатывает roulette_cmd; молча выходим.
    from config import SHINE_GAME_WORDS
    if not parts or parts[0].lower() not in SHINE_GAME_WORDS:
        return
    game = _GAME_WORDS.get(parts[0].lower())
    if game != "ttt":
        return await ui.reply(msg, "Пока доступны только крестики-нолики: <code>!шайн кн ставка валюта</code>")
    # ставка и валюта
    bet = parse_amount(parts[1]) if len(parts) > 1 else None
    cur = None
    for w in parts[2:]:
        if w.lower() in _CUR_WORDS:
            cur = _CUR_WORDS[w.lower()]
            break
    if bet is None or bet <= 0 or cur is None:
        return await ui.reply(msg, "Формат: <code>!шайн кн ставка валюта</code>. "
                                   "Пример: <code>!шайн кн 5000 грибы</code>")

    uid = msg.from_user.id
    if await matches.has_active(uid):
        return await ui.reply(msg, "У тебя уже есть активная игра. Заверши или отмени её.")
    b = await db.balances(uid)
    if b[cur] < bet:
        sx = await settings.ctx()
        return await ui.reply(msg, f"Не хватает: на балансе {_fmt(b[cur])} {sx['e_' + cur]}.")

    sx = await settings.ctx()
    # реплай -> вызов конкретного игрока
    if msg.reply_to_message and msg.reply_to_message.from_user \
            and not msg.reply_to_message.from_user.is_bot:
        opp = msg.reply_to_message.from_user
        if opp.id == uid:
            return await ui.reply(msg, "С самим собой нельзя 🙂")
        ob = await db.balances(opp.id)
        if ob.get(cur, 0) < bet:
            return await ui.reply(msg, "У оппонента недостаточно средств для этой ставки.")
        mid, err = await matches.create_match(GAME, uid, cur, bet, "direct",
                                              opponent=opp.id, origin_chat=msg.chat.id)
        if err:
            return await ui.reply(msg, f"⚠️ {err}")
        inv = InlineKeyboardBuilder()
        await btn(inv, "✅ Принять", f"ttt_accept_chat:{mid}")
        await btn(inv, "❌ Отклонить", f"ttt_decline:{mid}")
        inv.adjust(2)
        challenger = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.first_name
        oppname = f"@{opp.username}" if opp.username else opp.first_name
        sent = await ui.reply(msg,
            f"⚔️ <b>{challenger}</b> вызывает <b>{oppname}</b> на крестики-нолики!\n"
            f"Ставка: <b>{_fmt(bet)}</b> {sx['e_' + cur]}.\n\n"
            f"{oppname}, принимаешь?",
            reply_markup=inv.as_markup())
        # запомним владельца приглашения (только opp может принять) — через match.p2
        return

    # без реплая -> открытый вызов (онлайн), любой примет кнопкой
    mid, err = await matches.create_match(GAME, uid, cur, bet, "online", origin_chat=msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    k = InlineKeyboardBuilder()
    await btn(k, "⚔️ Принять вызов", f"ttt_join_chat:{mid}")
    await btn(k, "❌ Отменить", f"ttt_cancel:{mid}")
    k.adjust(1)
    challenger = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.first_name
    await ui.reply(msg,
        f"⚔️ <b>Открытый вызов!</b>\n\n"
        f"{challenger} играет в крестики-нолики.\n"
        f"Ставка: <b>{_fmt(bet)}</b> {sx['e_' + cur]}.\n\n"
        f"Кто примет — жми кнопку. Поиск 10 минут.",
        reply_markup=k.as_markup())


@router.callback_query(F.data.startswith("ttt_join_chat:"))
async def cb_ttt_join_chat(c: CallbackQuery):
    """Открытый вызов в чате: любой (кроме создателя) может принять."""
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] != "searching":
        return await c.answer("Вызов уже неактуален.", show_alert=True)
    if c.from_user.id == m["p1"]:
        return await c.answer("Это твой собственный вызов.", show_alert=True)
    if await matches.has_active(c.from_user.id):
        return await c.answer("У тебя уже есть активная игра.", show_alert=True)
    # присоединяемся к этому конкретному матчу
    joined, err = await _join_specific(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Ты в игре!")
    with contextlib.suppress(Exception):
        await c.message.edit_text("⚔️ Соперник найден! Игра началась — поле в личке у обоих.",
                                  reply_markup=None)
    from handlers.ttt_game import start_game_chat
    with contextlib.suppress(Exception):
        await start_game_chat(c.bot, mid, c.message.chat.id)


@router.callback_query(F.data.startswith("ttt_accept_chat:"))
async def cb_ttt_accept_chat(c: CallbackQuery):
    """Принять именной вызов в чате (только приглашённый)."""
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] != "invited":
        return await c.answer("Вызов уже неактуален.", show_alert=True)
    if c.from_user.id != m["p2"]:
        return await c.answer("Этот вызов не тебе.", show_alert=True)
    mm, err = await matches.accept_invite(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Принято!")
    with contextlib.suppress(Exception):
        await c.message.edit_text("⚔️ Вызов принят! Игра началась — поле в личке у обоих.",
                                  reply_markup=None)
    from handlers.ttt_game import start_game_chat
    with contextlib.suppress(Exception):
        await start_game_chat(c.bot, mid, c.message.chat.id)


async def _join_specific(mid: int, p2: int):
    """Присоединить p2 к конкретному searching-матчу (для открытого вызова в чате)."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m or m["status"] != "searching":
                return None, "Вызов уже неактуален."
            bal = await conn.fetchval(
                "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2 FOR UPDATE",
                p2, m["currency"]) or 0
            if bal < m["stake"]:
                return None, "Недостаточно средств для ставки."
            await db.apply(conn, p2, m["currency"], -m["stake"], "match_stake",
                           f"mstake:{mid}:p2", mid)
            await conn.execute(
                "UPDATE rb_matches SET p2=$1, status='active', search_deadline=NULL WHERE id=$2",
                p2, mid)
    return True, ""


# ==================== РЕВАНШ / ЕЩЁ РАЗ ====================
@router.callback_query(F.data.startswith("ttt_again:"))
async def cb_ttt_again(c: CallbackQuery):
    """Предложить сопернику из прошлой игры сыграть ещё раз (та же ставка/валюта)."""
    old_mid = int(c.data.split(":")[1])
    old = await matches.get(old_mid)
    if not old or old["status"] != "finished":
        return await c.answer("Игра недоступна для реванша.", show_alert=True)
    uid = c.from_user.id
    if uid not in (old["p1"], old["p2"]):
        return await c.answer("Это не твоя игра.", show_alert=True)
    if await matches.has_active(uid):
        return await c.answer("У тебя уже есть активная игра.", show_alert=True)
    opp = old["p2"] if uid == old["p1"] else old["p1"]
    cur, stake = old["currency"], old["stake"]

    b = await db.balances(uid)
    if b.get(cur, 0) < stake:
        return await c.answer("У тебя не хватает на ту же ставку.", show_alert=True)
    ob = await db.balances(opp)
    if ob.get(cur, 0) < stake:
        return await c.answer("У соперника не хватает на ту же ставку.", show_alert=True)

    mid, err = await matches.create_match(GAME, uid, cur, stake, "direct",
                                          opponent=opp, origin_chat=c.message.chat.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Вызов на реванш отправлен!")
    sx = await settings.ctx()
    challenger = f"@{c.from_user.username}" if c.from_user.username else c.from_user.first_name

    # реванш идёт туда же, где была игра: в чате -> приглашение в чат (accept_chat),
    # в ЛС -> приглашение в ЛС (accept)
    in_chat = bool(old.get("board_chat_id")) and c.message.chat.type in ("group", "supergroup")
    if in_chat:
        inv = InlineKeyboardBuilder()
        await btn(inv, "✅ Принять", f"ttt_accept_chat:{mid}")
        await btn(inv, "❌ Отклонить", f"ttt_decline:{mid}")
        inv.adjust(2)
        oppname = await _name_of(opp)
        with contextlib.suppress(Exception):
            await c.message.edit_reply_markup(reply_markup=None)
        await ui.send(c.bot, c.message.chat.id,
            f"🔁 <b>Реванш!</b> {challenger} снова вызывает {oppname}.\n"
            f"Ставка: <b>{_fmt(stake)}</b> {sx['e_' + cur]}.\n\n{oppname}, принимаешь?",
            reply_markup=inv.as_markup())
        return

    inv = InlineKeyboardBuilder()
    await btn(inv, "✅ Принять", f"ttt_accept:{mid}")
    await btn(inv, "❌ Отклонить", f"ttt_decline:{mid}")
    inv.adjust(2)
    with contextlib.suppress(Exception):
        await ui.send(c.bot, opp,
            f"🔁 <b>Реванш!</b>\n\n{challenger} зовёт отыграться.\n"
            f"Ставка: <b>{_fmt(stake)}</b> {sx['e_' + cur]}.\n\nПринимаешь?",
            reply_markup=inv.as_markup())
    with contextlib.suppress(Exception):
        await c.message.edit_reply_markup(reply_markup=None)
    with contextlib.suppress(Exception):
        await ui.send(c.bot, uid, "📨 Вызов на реванш отправлен. Ждём ответа соперника.")
