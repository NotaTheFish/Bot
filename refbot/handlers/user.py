import contextlib
import re

from aiogram import F, Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
import keyboards as kb
from keyboards import btn
from config import MIN_WITHDRAW, WITHDRAW_STEP, HOLD_HOURS, PAYOUT_CHAT_ID, SUPER_ADMINS
from services import settings, ui
from services.render import edit as r_edit
from services import referrals, withdrawals
from services import casino as casino_svc
from services.notify import push_admin_card, drop_admin_card

router = Router()


class WD(StatesGroup):
    currency = State()   # выбор валюты для добавления позиции
    amount = State()     # ввод суммы позиции


from services.amount_parse import shk_fmt, shk_parse
from services import wd_basket


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _is_admin(uid: int) -> bool:
    """
    Кто видит кнопку «Админка»: главный (SUPER_ADMINS / rb_admins) И второстепенный
    (PAYOUT_ADMINS) — последний заходит смотреть и принимать выводы.
    Что именно внутри доступно, решают гейты в admin.py (can_view / can_manage).
    """
    from config import PAYOUT_ADMINS
    return (uid in SUPER_ADMINS or uid in PAYOUT_ADMINS
            or bool(await db.admin_chats(uid)))


async def _payout_chat() -> int | None:
    """
    Касса общая на все чаты, поэтому заявка на вывод — не про конкретный чат,
    а про то, КТО её обрабатывает. Берём PAYOUT_CHAT_ID, иначе самый ранний
    активный (детерминированно, а не как повезёт).
    Если появятся чаты с РАЗНЫМИ владельцами — общая касса станет проблемой:
    владелец одного чата начнёт платить за рефералов другого. Тогда сюда надо
    возвращаться и делить балансы по чатам.
    """
    if PAYOUT_CHAT_ID:
        ok = await db.pool().fetchval(
            "SELECT 1 FROM rb_chats WHERE chat_id=$1 AND active", PAYOUT_CHAT_ID)
        if ok:
            return PAYOUT_CHAT_ID
    # сначала активный чат
    active = await db.pool().fetchval(
        "SELECT chat_id FROM rb_chats WHERE active ORDER BY created_at, chat_id LIMIT 1")
    if active:
        return active
    # активных нет — касса общая, chat_id нужен лишь чтобы понять, кто обработает.
    # Берём ЛЮБОЙ чат (даже неактивный), лишь бы заявку было к чему привязать.
    return await db.pool().fetchval(
        "SELECT chat_id FROM rb_chats ORDER BY created_at, chat_id LIMIT 1")


async def _has_live_offers(uid: int) -> bool:
    """Есть ли у игрока хотя бы одна действующая акция (для кнопки в меню)."""
    offers = await db.offers_for_user(uid)
    for o in offers:
        if await db.offer_is_live(o):
            return True
    return False


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
        return await ui.answer(msg, f"⚠️ {err}")

    await db.pool().execute("UPDATE rb_ref_links SET clicks = clicks + 1 WHERE code = $1", code)
    chat = await db.pool().fetchrow("SELECT title FROM rb_chats WHERE chat_id=$1", link["chat_id"])
    await ui.answer(msg, 
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
    is_adm = await _is_admin(msg.from_user.id)
    sx = await settings.ctx()
    # reply-клавиатура «☰ Меню» ставится отдельным тихим сообщением (reply и inline
    # нельзя в одном). Ставим один раз при старте — дальше висит всегда.
    with contextlib.suppress(Exception):
        await msg.answer("Панель управления снизу 👇", reply_markup=kb.menu_reply())  # noqa: ui
    await ui.answer(msg, 
        f"👋 Привет, {msg.from_user.first_name}!\n\n"
        f"Приглашай людей в чат по своей ссылке и получай валюту.\n"
        f"За каждого реферала: <b>5 000</b> 🍄 или <b>100 000</b> 🪙\n"
        f"Награда зачисляется через <b>{HOLD_HOURS // 24} дня</b> после входа — "
        f"если реферал остался в чате.\n\n"
        f"Текущая валюта: {sx['e_' + u['currency']]} <b>{sx['l_' + u['currency']]}</b>",
        reply_markup=await kb.main_menu(u["currency"], is_adm,
                                        show_casino=await casino_svc.visible(msg.from_user.id),
                                        show_offers=await _has_live_offers(msg.from_user.id)))


@router.message(F.text == "☰ Меню")
async def reply_menu_btn(msg: Message, state: FSMContext):
    """Нажатие reply-кнопки «☰ Меню» — открыть инлайн главное меню."""
    await state.clear()
    if not await guard(msg):
        return
    u = await db.get_user(msg.from_user.id)
    is_adm = await _is_admin(msg.from_user.id)
    await ui.answer(msg, "🏠 <b>Главное меню</b>",
                    reply_markup=await kb.main_menu(u["currency"], is_adm,
                                        show_casino=await casino_svc.visible(msg.from_user.id),
                                        show_offers=await _has_live_offers(msg.from_user.id)))


@router.message(Command("меню", "menu"))
async def show_reply_kb(msg: Message, state: FSMContext):
    """Развернуть reply-клавиатуру и показать меню."""
    await state.clear()
    if not await guard(msg):
        return
    u = await db.get_user(msg.from_user.id)
    is_adm = await _is_admin(msg.from_user.id)
    with contextlib.suppress(Exception):
        await msg.answer("Панель управления снизу 👇", reply_markup=kb.menu_reply())  # noqa: ui
    await ui.answer(msg, "🏠 <b>Главное меню</b>",
                    reply_markup=await kb.main_menu(u["currency"], is_adm,
                                        show_casino=await casino_svc.visible(msg.from_user.id),
                                        show_offers=await _has_live_offers(msg.from_user.id)))


@router.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await guard(c):
        return
    u = await db.get_user(c.from_user.id)
    is_adm = await _is_admin(c.from_user.id)
    await ui.edit(c.message, "🏠 <b>Главное меню</b>",
                              reply_markup=await kb.main_menu(u["currency"], is_adm,
                                        show_casino=await casino_svc.visible(c.from_user.id),
                                        show_offers=await _has_live_offers(c.from_user.id)))
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

    per_chat = await db.pool().fetch(
        """
        SELECT ch.title,
               count(*) FILTER (WHERE r.status='paid') paid,
               count(*) FILTER (WHERE r.status='hold') hold
        FROM rb_referrals r JOIN rb_chats ch ON ch.chat_id = r.chat_id
        WHERE r.inviter_id = $1 AND r.status IN ('paid','hold')
        GROUP BY ch.title ORDER BY paid DESC
        """, c.from_user.id)

    sctx = await settings.ctx()
    chats_block = ""
    if len(per_chat) > 1:
        chats_block = f"\n{sctx['e_chat']} <b>По чатам</b>\n" + "\n".join(
            f"  {r['title']}: {sctx['e_paid']} {r['paid']} | {sctx['e_hold']} {r['hold']}"
            for r in per_chat) + "\n"

    tpl = await settings.profile_template()
    data = {
        **sctx,
        "id": c.from_user.id,
        "bal_m": fmt(b["mushrooms"]), "bal_c": fmt(b["coins"]),
        "bal_s": shk_fmt(b["shimcoins"]),
        "hold_m": fmt(holds.get("mushrooms", 0)), "hold_c": fmt(holds.get("coins", 0)),
        "paid": row["paid"], "hold": row["hold"], "lost": row["lost"],
        "chats": chats_block,
        "e_cur": sctx[f"e_{u['currency']}"], "l_cur": sctx[f"l_{u['currency']}"],
    }
    try:
        text = tpl.format(**data)
    except Exception:
        # админ сломал шаблон -> не роняем профиль юзеру, откатываемся на дефолт
        text = settings.DEFAULT_PROFILE.format(**data)

    from services import tokens as _tok
    tl = await _tok.owned_lines(b)
    if tl:
        text += "\n\n" + "\n".join(tl)

    await r_edit(c.message, text, await settings.emoji_map(), reply_markup=await kb.back_menu())
    await c.answer()


# ---------------- переключение валюты ----------------
@router.callback_query(F.data == "toggle_cur")
async def cb_toggle(c: CallbackQuery):
    if not await guard(c):
        return
    u = await db.get_user(c.from_user.id)
    new = "coins" if u["currency"] == "mushrooms" else "mushrooms"
    await db.pool().execute("UPDATE rb_users SET currency=$1 WHERE tg_id=$2", new, c.from_user.id)
    is_adm = await _is_admin(c.from_user.id)
    await c.message.edit_reply_markup(reply_markup=await kb.main_menu(new, is_adm,
        show_casino=await casino_svc.visible(c.from_user.id),
                                        show_offers=await _has_live_offers(c.from_user.id)))
    await c.answer(
        f"Теперь получаешь: {await settings.label(new)}\n"
        f"Старый баланс никуда не делся. Уже висящие холды остаются в прежней валюте.",
        show_alert=True)


# ---------------- реф-ссылка ----------------
@router.callback_query(F.data == "mylink")
async def cb_link(c: CallbackQuery):
    if not await guard(c):
        return
    chats = await db.pool().fetch("SELECT * FROM rb_chats WHERE active ORDER BY created_at")
    if not chats:
        return await c.answer("Пока нет подключённых чатов.", show_alert=True)
    sx = await settings.ctx()
    await ui.edit(
        c.message,
        f"{sx['e_link']} <b>Выбери чат</b>\n\n"
        f"У каждого чата <b>своя</b> ссылка и свой зачёт. Один и тот же друг приносит "
        f"награду в каждом чате отдельно — пригласил в первый, потом во второй, "
        f"получил дважды.\n\n"
        f"<i>Ссылку можно получить только на чат, в котором ты сам состоишь.</i>",
        reply_markup=await kb.chat_picker(chats, "lnk"))
    await c.answer()


@router.callback_query(F.data.startswith("lnk:"))
async def cb_link_pick(c: CallbackQuery):
    if not await guard(c):
        return
    await _show_link(c, int(c.data.split(":")[1]))


IN_CHAT = frozenset({"creator", "administrator", "member", "restricted"})


async def _join_url(bot, chat_id: int) -> str | None:
    """Ссылка на вступление для того, кто ещё не в чате.

    НЕ реферальная и НЕ пишется в rb_invites: человек пришёл сам, награду за него
    никто не получит. on_join не найдёт такую ссылку по имени и просто пройдёт мимо.
    """
    try:
        ch = await bot.get_chat(chat_id)
        if ch.username:
            return f"https://t.me/{ch.username}"
        if ch.invite_link:
            return ch.invite_link
        link = await bot.create_chat_invite_link(chat_id=chat_id, name="self-join")
        return link.invite_link
    except Exception:
        return None


async def _show_link(c: CallbackQuery, chat_id: int):
    ch = await db.pool().fetchrow("SELECT * FROM rb_chats WHERE chat_id=$1 AND active", chat_id)
    if not ch:
        return await c.answer("Чат недоступен.", show_alert=True)
    sx = await settings.ctx()
    multi = await db.pool().fetchval("SELECT count(*) FROM rb_chats WHERE active") > 1

    # --- состоит ли человек в этом чате ---
    try:
        m = await c.bot.get_chat_member(chat_id, c.from_user.id)
        inside = m.status in IN_CHAT
    except Exception:
        inside = False

    if not inside:
        url = await _join_url(c.bot, chat_id)
        tail = (f"\n\n{url}" if url else
                "\n\n<i>Ссылку на вход попроси у админов чата.</i>")
        await ui.edit(
            c.message,
            f"{sx['e_chat']} <b>{ch['title']}</b>\n\n"
            f"⚠️ Ты пока не состоишь в этом чате, поэтому реферальной ссылки нет.\n"
            f"Сначала вступи — потом возвращайся и забирай ссылку.{tail}",
            reply_markup=await kb.link_card(multi), disable_web_page_preview=True)
        return await c.answer("Сначала вступи в чат", show_alert=True)

    me = await c.bot.get_me()
    code = await referrals.get_or_create_ref_code(chat_id, c.from_user.id)
    st = await db.pool().fetchrow(
        """
        SELECT (SELECT clicks FROM rb_ref_links WHERE code=$1) clicks,
               count(*) FILTER (WHERE status='paid') paid,
               count(*) FILTER (WHERE status='hold') hold
        FROM rb_referrals WHERE inviter_id=$2 AND chat_id=$3
        """, code, c.from_user.id, chat_id)
    await ui.edit(
        c.message,
        f"{sx['e_link']} <b>{ch['title']}</b>\n\n"
        f"<code>https://t.me/{me.username}?start={code}</code>\n\n"
        f"👁 Переходов: {st['clicks']}\n"
        f"{sx['e_paid']} Зачислено: {st['paid']}   {sx['e_hold']} Ждут: {st['hold']}\n\n"
        f"<i>Кидай друзьям. Бот выдаст каждому персональный одноразовый инвайт. "
        f"Награда упадёт на удержание сразу, на баланс — через 3 дня.</i>",
        reply_markup=await kb.link_card(multi), disable_web_page_preview=True)
    await c.answer()


# ---------------- мои рефералы ----------------
@router.callback_query(F.data == "myrefs")
async def cb_refs(c: CallbackQuery):
    if not await guard(c):
        return
    rows = await referrals.my_refs(c.from_user.id)
    sx = await settings.ctx()
    if not rows:
        await ui.edit(c.message, f"{sx['e_refs']} Рефералов пока нет.",
                      reply_markup=await kb.back_menu())
        return await c.answer()
    lines = []
    for r in rows[:25]:
        name = f"@{r['username']}" if r["username"] else (r["first_name"] or r["invitee_id"])
        if r["status"] == "paid":
            lines.append(f"{sx['e_paid']} {name} — {fmt(r['amount'])} {sx['e_' + r['currency']]}")
        else:
            left = r["hold_until"] - referrals.now()
            h = max(0, int(left.total_seconds() // 3600))
            lines.append(f"{sx['e_hold']} {name} — {fmt(r['amount'])} "
                         f"{sx['e_' + r['currency']]} (через {h} ч)")
    await ui.edit(c.message, f"{sx['e_refs']} <b>Мои рефералы</b>\n\n" + "\n".join(lines),
                              reply_markup=await kb.back_menu())
    await c.answer()


# ---------------- вывод ----------------
# ==================== КОРЗИНА ВЫВОДА ====================
# Игрок собирает позиции (валюта+сумма) в FSM (debt не заморожен до отправки),
# может добавлять/убирать, потом «Отправить» -> wd_basket.create_basket (заморозка всех).

_TOK_E = {"revive": "❤️‍🔥", "max": "🔱", "partials": "🧩"}


def _cur_e(sx, cur):
    return sx.get("e_" + cur, _TOK_E.get(cur, "🎫"))


def _fmt_cur(cur, amt):
    return shk_fmt(amt) if cur == "shimcoins" else fmt(amt)


async def _basket_text(uid, sx, cart: list):
    b = await db.balances(uid)
    lines = [f"{sx['e_withdraw']} <b>Вывод — корзина</b>\n"]
    if cart:
        lines.append("В корзине:")
        for i, (cur, amt) in enumerate(cart, 1):
            lines.append(f"  {i}. {_cur_e(sx, cur)} {_fmt_cur(cur, amt)}")
        lines.append("")
    else:
        lines.append("Корзина пуста. Добавь валюту для вывода.\n")
    lines.append(f"Баланс: {sx['e_mushrooms']} {fmt(b['mushrooms'])} · "
                 f"{sx['e_coins']} {fmt(b['coins'])}")
    from services import tokens as _tok
    tl = await _tok.owned_lines(b)
    if tl:
        lines.append(" · ".join(tl))
    return "\n".join(lines)


async def _basket_kb(cart: list):
    k = InlineKeyboardBuilder()
    await btn(k, "➕ Добавить валюту", "wd_add")
    if cart:
        await btn(k, "🗑 Убрать позицию", "wd_del")
        await btn(k, "✅ Отправить заявку", "wd_send")
    await btn(k, "Меню", "menu", "back")
    k.adjust(1)
    return k.as_markup()


@router.callback_query(F.data == "wd_menu")
async def cb_wd_menu(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    sx = await settings.ctx()
    # если есть активная (отправленная) заявка — показываем её, новую собрать нельзя
    w, items = await wd_basket.active_basket(c.from_user.id)
    if w:
        lines = [f"{sx['e_withdraw']} <b>Активная заявка #{w['id']}</b>\n"]
        for it in items:
            st = {"pending": "⏳", "confirmed": "✅", "rejected": "❌",
                  "cancelled": "🚫"}.get(it["status"], "•")
            lines.append(f"  {st} {_cur_e(sx, it['currency'])} {_fmt_cur(it['currency'], it['amount'])}")
        lines.append("\nЖдёт обработки админом. Можно отменить целиком (вернём незакрытые позиции).")
        kk = InlineKeyboardBuilder()
        await btn(kk, "❌ Отменить заявку", "wd_cancel")
        await btn(kk, "Меню", "menu", "back")
        kk.adjust(1)
        return await _finish_menu(c, "\n".join(lines), kk.as_markup())
    # иначе — собираем корзину (из FSM)
    data = await state.get_data()
    cart = data.get("cart") or []
    await state.update_data(cart=cart)
    await _finish_menu(c, await _basket_text(c.from_user.id, sx, cart), await _basket_kb(cart))


async def _finish_menu(c, text, markup):
    await ui.edit(c.message, text, reply_markup=markup)
    await c.answer()


@router.callback_query(F.data == "wd_add")
async def cb_wd_add(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    if await wd_basket.has_active(c.from_user.id):
        return await c.answer("У тебя уже есть отправленная заявка.", show_alert=True)
    b = await db.balances(c.from_user.id)
    await ui.edit(c.message, "Выбери валюту для вывода:", reply_markup=await kb.wd_currency(b))
    await c.answer()


@router.callback_query(F.data.startswith("wdcur:"))
async def cb_wd_currency_pick(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    cur = c.data.split(":")[1]
    if cur not in MIN_WITHDRAW:
        return await c.answer("Эту валюту вывести нельзя.", show_alert=True)
    data = await state.get_data()
    cart = data.get("cart") or []
    # нельзя добавить валюту, которая уже в корзине (одна позиция на валюту)
    if any(x[0] == cur for x in cart):
        return await c.answer("Эта валюта уже в корзине. Убери её, чтобы изменить.", show_alert=True)
    await state.update_data(wd_currency=cur)
    await state.set_state(WD.amount)
    sx = await settings.ctx()
    b = await db.balances(c.from_user.id)
    e = _cur_e(sx, cur)
    await ui.edit(c.message,
        f"✍️ <b>Сумма</b> в {e}\n\n"
        f"Баланс: <b>{_fmt_cur(cur, b[cur])}</b> {e}\n"
        f"Минимум: <b>{fmt(MIN_WITHDRAW[cur])}</b> · кратно <b>{fmt(WITHDRAW_STEP[cur])}</b>\n\n"
        f"Пришли число (<code>100к</code>, <code>1м</code>).",
        reply_markup=await kb.back_menu())
    await c.answer()


@router.message(WD.amount, ~F.text.startswith("/"), F.text != "☰ Меню")
async def wd_amount_input(msg: Message, state: FSMContext):
    if not await guard(msg):
        return
    from services.amount_parse import parse_amount
    amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.answer(msg, "Не понял сумму. Например: <code>100к</code>, <code>1м</code>.")
    data = await state.get_data()
    cur = data.get("wd_currency")
    if not cur:
        await state.set_state(None)
        return await ui.answer(msg, "Начни заново через меню вывода.")
    # валидация через сервис (минимум, кратность)
    err = wd_basket.validate_item(cur, amount)
    if err:
        return await ui.answer(msg, f"⚠️ {err}")
    # проверка баланса с учётом уже добавленного в корзину этой валюты
    cart = data.get("cart") or []
    b = await db.balances(msg.from_user.id)
    already = sum(a for cc, a in cart if cc == cur)
    if amount + already > b[cur]:
        e_ = _cur_e(await settings.ctx(), cur)
        return await ui.answer(msg,
            f"На балансе {_fmt_cur(cur, b[cur])} {e_}, а в корзине уже {_fmt_cur(cur, already)}. "
            f"Не хватает.")
    cart.append([cur, amount])
    await state.update_data(cart=cart, wd_currency=None)
    await state.set_state(None)
    sx = await settings.ctx()
    await ui.answer(msg, await _basket_text(msg.from_user.id, sx, cart),
                    reply_markup=await _basket_kb(cart))


@router.callback_query(F.data == "wd_del")
async def cb_wd_del(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    data = await state.get_data()
    cart = data.get("cart") or []
    if not cart:
        return await c.answer("Корзина пуста.", show_alert=True)
    sx = await settings.ctx()
    k = InlineKeyboardBuilder()
    for i, (cur, amt) in enumerate(cart):
        await btn(k, f"🗑 {_cur_e(sx, cur)} {_fmt_cur(cur, amt)}", f"wddel:{i}")
    await btn(k, "Назад", "wd_menu", "back")
    k.adjust(1)
    await ui.edit(c.message, "Что убрать из корзины?", reply_markup=k.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("wddel:"))
async def cb_wd_del_one(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    idx = int(c.data.split(":")[1])
    data = await state.get_data()
    cart = data.get("cart") or []
    if 0 <= idx < len(cart):
        cart.pop(idx)
        await state.update_data(cart=cart)
    sx = await settings.ctx()
    await ui.edit(c.message, await _basket_text(c.from_user.id, sx, cart),
                  reply_markup=await _basket_kb(cart))
    await c.answer("Убрано")


@router.callback_query(F.data == "wd_send")
async def cb_wd_send(c: CallbackQuery, state: FSMContext):
    if not await guard(c):
        return
    data = await state.get_data()
    cart = data.get("cart") or []
    if not cart:
        return await c.answer("Корзина пуста.", show_alert=True)
    chat_id = await _payout_chat()
    if not chat_id:
        return await c.answer("Некому обработать вывод — нет чата с ботом.", show_alert=True)
    items = [(cur, amt) for cur, amt in cart]
    wid, err = await wd_basket.create_basket(c.from_user.id, chat_id, items)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await state.update_data(cart=[])
    # уведомление админам (карточка с кнопками — в v106; пока текстовая заявка)
    from services import notify
    with contextlib.suppress(Exception):
        await notify.push_basket_card(c.bot, wid)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"{sx['e_paid']} <b>Заявка #{wid} отправлена!</b>\n\n"
        f"Средства заморожены. Админ обработает каждую позицию.\n"
        f"Можешь отменить заявку, пока позиции не обработаны.",
        reply_markup=await kb.back_menu())
    await c.answer("Отправлено!")


@router.callback_query(F.data == "wd_cancel")
async def cb_wd_cancel(c: CallbackQuery):
    if not await guard(c):
        return
    w = await wd_basket.cancel_basket(c.from_user.id)
    if not w:
        return await c.answer("Активной заявки нет.", show_alert=True)
    with contextlib.suppress(Exception):
        from services import notify
        await notify.drop_basket_card(c.bot, w["id"])
    await ui.edit(c.message,
        "❌ Заявка отменена. Незакрытые позиции вернулись на баланс.",
        reply_markup=await kb.back_menu())
    await c.answer("Отменено")
