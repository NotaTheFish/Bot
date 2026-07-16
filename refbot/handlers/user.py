import re

from aiogram import F, Router
from aiogram.filters import CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from config import CURRENCY_EMOJI, CURRENCY_NAME, MIN_WITHDRAW, HOLD_HOURS
from services import referrals, withdrawals
from services.notify import push_admin_card, drop_admin_card

router = Router()


class WD(StatesGroup):
    amount = State()


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def guard(event) -> bool:
    """Единая точка проверки бана. Забаненный не может вообще ничего."""
    uid = event.from_user.id
    await db.upsert_user(uid, event.from_user.username, event.from_user.first_name)
    if await db.is_banned(uid):
        u = await db.get_user(uid)
        text = f"🚫 Ты заблокирован.\nПричина: {u['ban_reason'] or 'не указана'}"
        if isinstance(event, CallbackQuery):
            await event.answer(text, show_alert=True)
        else:
            await event.answer(text)
        return False
    return True


# ---------------- /start ----------------
@router.message(CommandStart(deep_link=True))
async def start_deeplink(msg: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    if not await guard(msg):
        return
    code = (command.args or "").strip()
    link = await referrals.resolve_ref_code(code)
    if not link:
        return await start_plain(msg, state)

    url, err = await referrals.issue_invite(
        msg.bot, link["chat_id"], link["inviter_id"], msg.from_user.id)
    if err:
        return await msg.answer(f"⚠️ {err}")

    await db.pool().execute("UPDATE rb_ref_links SET clicks = clicks + 1 WHERE code = $1", code)
    chat = await db.pool().fetchrow("SELECT title FROM rb_chats WHERE chat_id=$1", link["chat_id"])
    await msg.answer(
        f"🎉 Тебя пригласили в <b>{chat['title']}</b>\n\n"
        f"👇 Твоя <b>персональная одноразовая</b> ссылка (действует 30 минут):\n{url}\n\n"
        f"⚠️ Ссылка работает один раз и только для тебя. Никому не передавай.\n"
        f"После входа не выходи из чата — иначе пригласивший останется без награды.\n\n"
        f"Хочешь тоже зарабатывать? Жми /start после входа.",
        disable_web_page_preview=True)


@router.message(CommandStart())
async def start_plain(msg: Message, state: FSMContext):
    await state.clear()
    if not await guard(msg):
        return
    u = await db.get_user(msg.from_user.id)
    is_adm = bool(await db.admin_chats(msg.from_user.id))
    await msg.answer(
        f"👋 Привет, {msg.from_user.first_name}!\n\n"
        f"Приглашай людей в чат по своей ссылке и получай валюту.\n"
        f"За каждого реферала: <b>5 000</b> 🍄 или <b>100 000</b> 🪙\n"
        f"Награда зачисляется через <b>{HOLD_HOURS // 24} дня</b> после входа — "
        f"если реферал остался в чате.\n\n"
        f"Текущая валюта: {CURRENCY_EMOJI[u['currency']]} <b>{CURRENCY_NAME[u['currency']]}</b>",
        reply_markup=kb.main_menu(u["currency"], is_adm))


@router.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await guard(c):
        return
    u = await db.get_user(c.from_user.id)
    is_adm = bool(await db.admin_chats(c.from_user.id))
    await c.message.edit_text("🏠 <b>Главное меню</b>",
                              reply_markup=kb.main_menu(u["currency"], is_adm))
    await c.answer()


# ---------------- профиль ----------------
@router.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    if not await guard(c):
        return
    u = await db.get_user(c.from_user.id)
    b = await db.balances(c.from_user.id)
    row = await db.pool().fetchrow(
        """
        SELECT
          count(*) FILTER (WHERE status='paid') AS paid,
          count(*) FILTER (WHERE status='hold') AS hold,
          count(*) FILTER (WHERE status='void') AS lost
        FROM rb_referrals WHERE inviter_id = $1
        """, c.from_user.id)
    hold_sum = await db.pool().fetch(
        "SELECT currency, sum(amount) s FROM rb_referrals "
        "WHERE inviter_id=$1 AND status='hold' GROUP BY currency", c.from_user.id)
    holds = {r["currency"]: r["s"] for r in hold_sum}

    await c.message.edit_text(
        f"👤 <b>Профиль</b>\n"
        f"ID: <code>{c.from_user.id}</code>\n\n"
        f"💰 <b>Баланс</b>\n"
        f"🍄 Грибы: <b>{fmt(b['mushrooms'])}</b>\n"
        f"🪙 Коины: <b>{fmt(b['coins'])}</b>\n\n"
        f"⏳ <b>На удержании</b>\n"
        f"🍄 {fmt(holds.get('mushrooms', 0))}   🪙 {fmt(holds.get('coins', 0))}\n\n"
        f"👥 <b>Рефералы</b>\n"
        f"✅ Зачислено: {row['paid']}\n"
        f"⏳ Ждут: {row['hold']}\n"
        f"❌ Потеряно: {row['lost']}\n\n"
        f"⚙️ Валюта: {CURRENCY_EMOJI[u['currency']]} {CURRENCY_NAME[u['currency']]}",
        reply_markup=kb.back_menu())
    await c.answer()


# ---------------- переключение валюты ----------------
@router.callback_query(F.data == "toggle_cur")
async def cb_toggle(c: CallbackQuery):
    if not await guard(c):
        return
    u = await db.get_user(c.from_user.id)
    new = "coins" if u["currency"] == "mushrooms" else "mushrooms"
    await db.pool().execute("UPDATE rb_users SET currency=$1 WHERE tg_id=$2", new, c.from_user.id)
    is_adm = bool(await db.admin_chats(c.from_user.id))
    await c.message.edit_reply_markup(reply_markup=kb.main_menu(new, is_adm))
    await c.answer(
        f"Теперь получаешь: {CURRENCY_NAME[new]}\n"
        f"Старый баланс никуда не делся. Уже висящие холды остаются в прежней валюте.",
        show_alert=True)


# ---------------- реф-ссылка ----------------
@router.callback_query(F.data == "mylink")
async def cb_link(c: CallbackQuery):
    if not await guard(c):
        return
    chats = await db.pool().fetch("SELECT * FROM rb_chats WHERE active ORDER BY chat_id")
    if not chats:
        await c.answer("Пока нет подключённых чатов.", show_alert=True)
        return
    me = await c.bot.get_me()
    parts = []
    for ch in chats:
        code = await referrals.get_or_create_ref_code(ch["chat_id"], c.from_user.id)
        clicks = await db.pool().fetchval("SELECT clicks FROM rb_ref_links WHERE code=$1", code)
        parts.append(f"📢 <b>{ch['title']}</b>\n"
                     f"<code>https://t.me/{me.username}?start={code}</code>\n"
                     f"👁 переходов: {clicks}")
    await c.message.edit_text(
        "🔗 <b>Твои реферальные ссылки</b>\n\n" + "\n\n".join(parts) +
        "\n\n<i>Кидай ссылку друзьям. Бот выдаст каждому персональный одноразовый "
        "инвайт. Награда упадёт на удержание сразу, а на баланс — через 3 дня.</i>",
        reply_markup=kb.back_menu(), disable_web_page_preview=True)
    await c.answer()


# ---------------- мои рефералы ----------------
@router.callback_query(F.data == "myrefs")
async def cb_refs(c: CallbackQuery):
    if not await guard(c):
        return
    rows = await referrals.my_refs(c.from_user.id)
    if not rows:
        await c.message.edit_text("👥 Рефералов пока нет.", reply_markup=kb.back_menu())
        return await c.answer()
    lines = []
    for r in rows[:25]:
        name = f"@{r['username']}" if r["username"] else (r["first_name"] or r["invitee_id"])
        if r["status"] == "paid":
            lines.append(f"✅ {name} — {fmt(r['amount'])} {CURRENCY_EMOJI[r['currency']]}")
        else:
            left = r["hold_until"] - referrals.now()
            h = max(0, int(left.total_seconds() // 3600))
            lines.append(f"⏳ {name} — {fmt(r['amount'])} {CURRENCY_EMOJI[r['currency']]} "
                         f"(через {h} ч)")
    await c.message.edit_text("👥 <b>Мои рефералы</b>\n\n" + "\n".join(lines),
                              reply_markup=kb.back_menu())
    await c.answer()


# ---------------- вывод ----------------
@router.callback_query(F.data == "wd_menu")
async def cb_wd_menu(c: CallbackQuery):
    if not await guard(c):
        return
    act = await withdrawals.active(c.from_user.id)
    b = await db.balances(c.from_user.id)
    head = (f"💸 <b>Вывод</b>\n\n"
            f"Минимум: <b>100 000</b> 🍄 или <b>2 000 000</b> 🪙\n"
            f"Баланс: 🍄 {fmt(b['mushrooms'])} | 🪙 {fmt(b['coins'])}\n\n")
    if act:
        head += (f"📌 Активная заявка: <b>{fmt(act['amount'])}</b> "
                 f"{CURRENCY_EMOJI[act['currency']]}\n"
                 f"Статус: ожидает админа. Пока он не подтвердил — можешь менять или отменить.")
    else:
        head += "Активных заявок нет."
    await c.message.edit_text(head, reply_markup=kb.wd_menu(bool(act)))
    await c.answer()


@router.callback_query(F.data == "wd_amount")
async def cb_wd_amount(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    await state.set_state(WD.amount)
    await c.message.edit_text("✍️ Отправь сумму вывода числом.\nНапример: <code>150000</code>",
                              reply_markup=kb.back_menu())
    await c.answer()


@router.message(WD.amount)
async def wd_amount_input(msg: Message, state: FSMContext):
    if not await guard(msg):
        return
    raw = re.sub(r"[^\d]", "", msg.text or "")
    if not raw:
        return await msg.answer("Нужно число. Попробуй ещё раз.")
    amount = int(raw)
    u = await db.get_user(msg.from_user.id)

    act = await withdrawals.active(msg.from_user.id)
    if act:
        row, err = await withdrawals.change_amount(msg.from_user.id, amount)
        if err:
            return await msg.answer(f"⚠️ {err}")
        # старое сообщение админу удаляем, шлём свежее
        await drop_admin_card(msg.bot, act)
        await push_admin_card(msg.bot, row)
        await state.clear()
        return await msg.answer(f"✏️ Сумма изменена на <b>{fmt(amount)}</b> "
                                f"{CURRENCY_EMOJI[row['currency']]}. Админу ушло новое уведомление.",
                                reply_markup=kb.back_menu())

    chat = await db.pool().fetchrow("SELECT chat_id FROM rb_chats WHERE active LIMIT 1")
    if not chat:
        return await msg.answer("Нет активного чата для вывода.")
    wid, err = await withdrawals.create(msg.from_user.id, chat["chat_id"], u["currency"], amount)
    if err:
        return await msg.answer(f"⚠️ {err}")
    row = await db.pool().fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1", wid)
    await push_admin_card(msg.bot, dict(row))
    await state.clear()
    await msg.answer(
        f"✅ Заявка создана: <b>{fmt(amount)}</b> {CURRENCY_EMOJI[u['currency']]}\n"
        f"Админ получил уведомление. Пока он не подтвердил — сумму можно менять или отменить.\n"
        f"Списание произойдёт только в момент подтверждения.",
        reply_markup=kb.back_menu())


@router.callback_query(F.data == "wd_cancel")
async def cb_wd_cancel(c: CallbackQuery):
    if not await guard(c):
        return
    row = await withdrawals.cancel(c.from_user.id)
    if not row:
        return await c.answer("Активной заявки нет.", show_alert=True)
    await drop_admin_card(c.bot, row)
    await c.message.edit_text("❌ Заявка отменена. Уведомление у админа удалено.",
                              reply_markup=kb.back_menu())
    await c.answer()
