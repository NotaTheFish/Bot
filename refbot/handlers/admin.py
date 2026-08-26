import asyncio
import contextlib
from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from config import PAYOUT_ADMINS, ROULETTE_DAILY_BUDGET, SUPER_ADMINS
from services import settings, withdrawals, ui
from services.notify import drop_admin_card

router = Router()


@router.callback_query(F.data == "a_noop")
async def cb_noop(c: CallbackQuery):
    await c.answer("Только просмотр — изменять может главный админ.", show_alert=True)


# ---------------- РОЛИ ----------------
# Главный (SUPER_ADMINS + владелец чата в rb_admins) — может ВСЁ.
# Второстепенный (PAYOUT_ADMINS) — заходит в админку, ВИДИТ всё, но из действий
#   умеет только принимать/отклонять выводы. Ни банов, ни настроек, ни чатов.
async def can_manage(uid: int) -> bool:
    """Право на любые изменяющие действия: баны, кастомизация, чаты, разбан."""
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


async def can_view(uid: int) -> bool:
    """Право войти в админку и смотреть. Второстепенный сюда тоже попадает."""
    return await can_manage(uid) or uid in PAYOUT_ADMINS


async def can_payout(uid: int) -> bool:
    """Право подтверждать/отклонять выводы."""
    return await can_manage(uid) or uid in PAYOUT_ADMINS


class Find(StatesGroup):
    query = State()


class Adjust(StatesGroup):
    amount = State()


from services.amount_parse import shk_fmt


def _tok_line(b: dict) -> str:
    """Строка с ненулевыми токенами для карточки: '\n❤️‍🔥 Revive: 1 200 | ...' или ''."""
    from services import tokens
    parts = []
    _emoji = {"revive": "❤️‍🔥", "max": "🔱", "partials": "🧩"}
    for code, name in tokens.TOKENS.items():
        v = b.get(code, 0) or 0
        if v > 0:
            parts.append(f"{_emoji.get(code,'🎫')} {name}: {v:,}".replace(",", " "))
    return ("\n" + " | ".join(parts)) if parts else ""


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


# ---------------- привязка чата ----------------
@router.message(Command("шайнуть", "bind"), F.chat.type.in_({"group", "supergroup"}))
async def bind(msg: Message):
    """
    Владелец чата пишет /шайнуть в самом чате. Права проверяем у Telegram, не на слово.

    Кириллица в командах работает только потому, что Group Privacy выключен —
    бот получает все сообщения, а не только распознанные Telegram команды.
    В меню BotFather такую команду не добавить. /bind оставлен алиасом.
    """
    m = await msg.bot.get_chat_member(msg.chat.id, msg.from_user.id)
    if m.status != "creator" and msg.from_user.id not in SUPER_ADMINS:
        return await ui.reply(msg, "🚫 Привязать чат может только его создатель.")
    me = await msg.bot.get_chat_member(msg.chat.id, (await msg.bot.get_me()).id)
    if not getattr(me, "can_invite_users", False):
        return await ui.reply(msg, "⚠️ Дай боту админку с правом «Пригласительные ссылки» — "
                               "без неё реферальная система не работает.")

    await db.upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    await db.pool().execute(
        """
        INSERT INTO rb_chats (chat_id, title, owner_id) VALUES ($1,$2,$3)
        ON CONFLICT (chat_id) DO UPDATE
        SET title=EXCLUDED.title, active=TRUE, owner_id=EXCLUDED.owner_id,
            deactivated_at=NULL, deactivated_by=NULL
        """, msg.chat.id, msg.chat.title, msg.from_user.id)
    await db.pool().execute(
        "INSERT INTO rb_admins (chat_id, tg_id, role) VALUES ($1,$2,'owner') "
        "ON CONFLICT (chat_id, tg_id) DO UPDATE SET role='owner'", msg.chat.id, msg.from_user.id)
    await db.audit(msg.from_user.id, "chat_bind", {"chat_id": msg.chat.id})
    await ui.reply(msg, "✅ Чат подключён. Уведомления о выводах будут приходить тебе в ЛС.\n"
                    "Добавь других админов: /addadmin @username")


@router.message(Command("отшайнуть", "unbind"), F.chat.type.in_({"group", "supergroup"}))
async def unbind(msg: Message):
    """Отключить чат прямо из чата. Прогресс сохраняется целиком."""
    row = await db.pool().fetchrow("SELECT * FROM rb_chats WHERE chat_id=$1", msg.chat.id)
    if not row:
        return await ui.reply(msg, "Этот чат и не был привязан.")
    if msg.from_user.id != row["owner_id"] and msg.from_user.id not in SUPER_ADMINS:
        return await ui.reply(msg, "🚫 Только владелец чата.")
    if not row["active"]:
        return await ui.reply(msg, "Чат уже отключён. Включить: «🛠 Админка → 📢 Чаты».")
    await db.pool().execute(
        "UPDATE rb_chats SET active=FALSE, deactivated_at=now(), deactivated_by=$1 "
        "WHERE chat_id=$2", msg.from_user.id, msg.chat.id)
    await db.audit(msg.from_user.id, "chat_off", {"chat_id": msg.chat.id, "via": "unbind"})
    st = await db.pool().fetchrow(
        "SELECT count(*) FILTER (WHERE status='hold') hold, "
        "count(*) FILTER (WHERE status='paid') paid FROM rb_referrals WHERE chat_id=$1",
        msg.chat.id)
    await ui.reply(msg, 
        f"⚪️ Чат отключён. Новых начислений и ссылок не будет.\n\n"
        f"<b>Прогресс сохранён полностью:</b>\n"
        f"✅ {st['paid']} выплаченных рефералов\n"
        f"⏳ {st['hold']} холдов заморожены\n"
        f"💰 Балансы юзеров не тронуты\n\n"
        f"Включить обратно: «🛠 Админка → 📢 Чаты» или снова /шайнуть.")


@router.message(Command("addadmin"), F.chat.type.in_({"group", "supergroup"}))
async def addadmin(msg: Message):
    row = await db.pool().fetchrow("SELECT owner_id FROM rb_chats WHERE chat_id=$1", msg.chat.id)
    if not row or (msg.from_user.id != row["owner_id"] and msg.from_user.id not in SUPER_ADMINS):
        return await ui.reply(msg, "🚫 Только владелец чата.")
    if not msg.reply_to_message:
        return await ui.reply(msg, "Ответь этой командой на сообщение нужного человека.")
    target = msg.reply_to_message.from_user
    await db.upsert_user(target.id, target.username, target.first_name)
    await db.pool().execute(
        "INSERT INTO rb_admins (chat_id, tg_id, added_by) VALUES ($1,$2,$3) "
        "ON CONFLICT DO NOTHING", msg.chat.id, target.id, msg.from_user.id)
    await ui.reply(msg, f"✅ {target.first_name} — админ бота в этом чате.")


# ---------------- баны ----------------
@router.message(Command("rban"))
async def rban(msg: Message, command: CommandObject):
    if not await can_manage(msg.from_user.id):
        return
    target = None
    if msg.reply_to_message:
        target = msg.reply_to_message.from_user.id
    elif command.args and command.args.split()[0].lstrip("-").isdigit():
        target = int(command.args.split()[0])
    if not target:
        return await ui.reply(msg, "Использование: ответом на сообщение — /rban причина\n"
                               "или /rban <user_id> причина")
    reason = " ".join((command.args or "").split()[1:]) if command.args else ""
    await db.pool().execute(
        "UPDATE rb_users SET banned=TRUE, ban_reason=$1, banned_by=$2, banned_at=now() "
        "WHERE tg_id=$3", reason or "не указана", msg.from_user.id, target)
    # гасим все холды и активную заявку забаненного. Замороженные под заявку средства
    # НЕ возвращаем (бан = наказание за фрод, заморозка сгорает намеренно).
    await db.pool().execute(
        "UPDATE rb_referrals SET status='void', voided_at=now() "
        "WHERE inviter_id=$1 AND status='hold'", target)
    wd = await db.pool().fetchrow(
        "UPDATE rb_withdrawals SET status='rejected', decided_at=now(), decided_by=$1, "
        "comment='ban' WHERE tg_id=$2 AND status='pending' RETURNING *", msg.from_user.id, target)
    if wd:
        await drop_admin_card(msg.bot, dict(wd))
    await db.audit(msg.from_user.id, "ban", {"target": target, "reason": reason})
    await ui.reply(msg, f"🚫 <code>{target}</code> заблокирован. Холды обнулены, заявка снята.")


@router.message(Command("runban"))
async def runban(msg: Message, command: CommandObject):
    if not await can_manage(msg.from_user.id):
        return
    target = msg.reply_to_message.from_user.id if msg.reply_to_message else \
        (int(command.args) if command.args and command.args.lstrip("-").isdigit() else None)
    if not target:
        return await ui.reply(msg, "Использование: /runban <user_id> или ответом.")
    await db.pool().execute(
        "UPDATE rb_users SET banned=FALSE, ban_reason=NULL WHERE tg_id=$1", target)
    await db.audit(msg.from_user.id, "unban", {"target": target})
    await ui.reply(msg, f"✅ <code>{target}</code> разблокирован.")


# ---------------- админ-меню ----------------
@router.callback_query(F.data == "admin")
async def cb_admin(c: CallbackQuery):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await ui.edit(c.message, "🛠 <b>Админка</b>", reply_markup=await kb.admin_menu(await can_manage(c.from_user.id)))
    await c.answer()


@router.callback_query(F.data == "a_top")
async def cb_top(c: CallbackQuery):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    rows = await db.pool().fetch(
        """
        SELECT b.tg_id, b.currency, b.amount, u.username, u.first_name, u.banned
        FROM rb_balances b JOIN rb_users u ON u.tg_id=b.tg_id
        WHERE b.amount > 0
        ORDER BY (CASE WHEN b.currency='coins' THEN b.amount/20 ELSE b.amount END) DESC
        LIMIT 25
        """)
    sx = await settings.ctx()
    lines = []
    for i, r in enumerate(rows, 1):
        name = f"@{r['username']}" if r["username"] else (r["first_name"] or str(r["tg_id"]))
        mark = "🚫" if r["banned"] else ""
        amt_s = shk_fmt(r["amount"]) if r["currency"] == "shimcoins" else fmt(r["amount"])
        lines.append(f"{i}. {mark}{name} — {amt_s} {sx['e_' + r['currency']]} "
                     f"<code>{r['tg_id']}</code>")
    await ui.edit(c.message, f"{sx['e_top']} <b>Топ-25 по балансу</b>\n\n" + ("\n".join(lines) or "пусто"),
                              reply_markup=await kb.admin_menu(await can_manage(c.from_user.id)))
    await c.answer()


@router.callback_query(F.data == "a_stats")
async def cb_stats(c: CallbackQuery):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    sx = await settings.ctx()
    s = await db.pool().fetchrow(
        """
        SELECT
          (SELECT count(*) FROM rb_users) users,
          (SELECT count(*) FROM rb_users WHERE banned) banned,
          (SELECT count(*) FROM rb_referrals WHERE status='paid') paid,
          (SELECT count(*) FROM rb_referrals WHERE status='hold') hold,
          (SELECT count(*) FROM rb_referrals WHERE flagged AND status='hold') flagged,
          (SELECT COALESCE(sum(amount),0) FROM rb_balances WHERE currency='mushrooms') m,
          (SELECT COALESCE(sum(amount),0) FROM rb_balances WHERE currency='coins') co,
          (SELECT COALESCE(sum(amount),0) FROM rb_withdrawals WHERE status='confirmed'
             AND currency='mushrooms') wm,
          (SELECT COALESCE(sum(amount),0) FROM rb_withdrawals WHERE status='confirmed'
             AND currency='coins') wc
        """)
    chats = await db.pool().fetch(
        "SELECT title, active, budget_spent_mush, daily_budget_mush FROM rb_chats "
        "ORDER BY active DESC, created_at")
    budget = "\n".join(
        f"  {'🟢' if ch['active'] else '⚪️'} {ch['title']}: "
        f"{fmt(ch['budget_spent_mush'])} / {fmt(ch['daily_budget_mush'])}"
        for ch in chats) or "  нет привязанных чатов"

    # рулетка: одобренные через /шимм чаты
    rb = await db.pool().fetchval(
        "SELECT spent_mush FROM rb_roulette_budget WHERE day=CURRENT_DATE") or 0
    rc = await db.pool().fetchrow(
        "SELECT count(*) FILTER (WHERE active) n, count(*) FILTER (WHERE NOT active) off "
        "FROM rb_roulette_chats")
    free = (f"\n  {sx['e_roulette']} <b>Рулетка</b> (одобренные чаты)\n"
            f"  {fmt(rb)} / {fmt(ROULETTE_DAILY_BUDGET)} за сегодня\n"
            f"  Активных чатов: {rc['n']} (выключено {rc['off']})")
    await ui.edit(c.message, 
        f"📊 <b>Сводка</b>\n\n"
        f"{sx['e_refs']} Юзеров: {s['users']} (забанено {s['banned']})\n"
        f"{sx['e_paid']} Рефералов зачислено: {s['paid']}\n"
        f"{sx['e_hold']} На удержании: {s['hold']}\n"
        f"🚩 На ручной проверке: {s['flagged']}\n\n"
        f"{sx['e_balance']} <b>Обязательства (на балансах)</b>\n"
        f"{sx['e_mushrooms']} {fmt(s['m'])} | {sx['e_coins']} {fmt(s['co'])}\n\n"
        f"{sx['e_withdraw']} <b>Выведено всего</b>\n"
        f"{sx['e_mushrooms']} {fmt(s['wm'])} | {sx['e_coins']} {fmt(s['wc'])}\n\n"
        f"🧯 <b>Суточный бюджет по чатам</b> {sx['e_mushrooms']}\n{budget}\n{free}",
        reply_markup=await kb.admin_menu(await can_manage(c.from_user.id)))
    await c.answer()


@router.callback_query(F.data == "a_flagged")
async def cb_flagged(c: CallbackQuery):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    rows = await db.pool().fetch(
        "SELECT r.*, u.username FROM rb_referrals r LEFT JOIN rb_users u ON u.tg_id=r.inviter_id "
        "WHERE r.flagged AND r.status='hold' ORDER BY r.joined_at DESC LIMIT 20")
    if not rows:
        await ui.edit(c.message, "🚩 Подозрительных начислений нет.", reply_markup=await kb.admin_menu(await can_manage(c.from_user.id)))
        return await c.answer()
    sx = await settings.ctx()
    lines = [f"• inviter <code>{r['inviter_id']}</code> (@{r['username']}) ← "
             f"<code>{r['invitee_id']}</code>, {fmt(r['amount'])} "
             f"{sx['e_' + r['currency']]} — {r['flag_reason']}\n"
             f"  /approve_{r['id']}  /deny_{r['id']}" for r in rows]
    await ui.edit(c.message, 
        "🚩 <b>Ручная проверка</b>\nЭти начисления автоматом НЕ пройдут.\n\n" + "\n".join(lines),
        reply_markup=await kb.admin_menu(await can_manage(c.from_user.id)))
    await c.answer()


@router.message(F.text.regexp(r"^/approve_(\d+)$").as_("m"))
async def approve_ref(msg: Message, m):
    if not await can_manage(msg.from_user.id):
        return
    rid = int(m.group(1))
    await db.pool().execute("UPDATE rb_referrals SET flagged=FALSE WHERE id=$1", rid)
    await db.audit(msg.from_user.id, "ref_approve", {"id": rid})
    await ui.reply(msg, f"✅ Реферал #{rid} разморожен, выплата пройдёт по окончании холда.")


@router.message(F.text.regexp(r"^/deny_(\d+)$").as_("m"))
async def deny_ref(msg: Message, m):
    if not await can_manage(msg.from_user.id):
        return
    rid = int(m.group(1))
    await db.pool().execute(
        "UPDATE rb_referrals SET status='void', voided_at=now() WHERE id=$1 AND status='hold'", rid)
    await db.audit(msg.from_user.id, "ref_deny", {"id": rid})
    await ui.reply(msg, f"❌ Реферал #{rid} отклонён.")


# ---------------- поиск юзера ----------------
@router.callback_query(F.data == "a_find")
async def cb_find(c: CallbackQuery, state: FSMContext):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.set_state(Find.query)
    await ui.edit(c.message, "🔍 Отправь @username или user_id", reply_markup=await kb.back_menu())
    await c.answer()


@router.message(Find.query)
async def find_input(msg: Message, state: FSMContext):
    await state.clear()
    q = (msg.text or "").strip().lstrip("@")
    row = await db.pool().fetchrow(
        "SELECT * FROM rb_users WHERE (username ILIKE $1) OR (tg_id::text = $1)", q)
    if not row:
        return await ui.answer(msg, "Не найден.", reply_markup=await kb.admin_menu(await can_manage(msg.from_user.id)))
    b = await db.balances(row["tg_id"])
    st = await db.pool().fetchrow(
        "SELECT count(*) FILTER (WHERE status='paid') paid, "
        "count(*) FILTER (WHERE status='hold') hold, "
        "count(*) FILTER (WHERE status='void') lost FROM rb_referrals WHERE inviter_id=$1",
        row["tg_id"])
    led = await db.pool().fetch(
        "SELECT reason, delta, currency, created_at FROM rb_ledger WHERE tg_id=$1 "
        "ORDER BY id DESC LIMIT 10", row["tg_id"])
    sx = await settings.ctx()
    def _delta_fmt(delta, cur):
        if cur == "shimcoins":
            sign = "+" if delta >= 0 else "−"
            return f"{sign}{shk_fmt(abs(delta))}"
        return f"{delta:+}"
    hist = "\n".join(f"  {r['created_at']:%d.%m %H:%M} {r['reason']} "
                     f"{_delta_fmt(r['delta'], r['currency'])} {sx['e_' + r['currency']]}" for r in led) or "  пусто"
    await ui.answer(msg, 
        f"{sx['e_profile']} <b>{row['first_name']}</b> @{row['username'] or '—'}\n"
        f"<code>{row['tg_id']}</code>{' 🚫 БАН' if row['banned'] else ''}\n\n"
        f"{sx['e_balance']} {sx['e_mushrooms']} {fmt(b['mushrooms'])} | "
        f"{sx['e_coins']} {fmt(b['coins'])} | "
        f"{sx['e_shimcoins']} {shk_fmt(b['shimcoins'])}\n"
        f"{_tok_line(b)}"
        f"{sx['e_refs']} {sx['e_paid']} {st['paid']} | {sx['e_hold']} {st['hold']} | "
        f"{sx['e_lost']} {st['lost']}\n"
        f"📅 В боте с {row['created_at']:%d.%m.%Y}\n\n"
        f"📜 <b>Последние операции</b>\n{hist}",
        reply_markup=await kb.find_card(row["tg_id"], row["banned"], await can_manage(msg.from_user.id)))


@router.callback_query(F.data == "a_pending")
async def cb_pending(c: CallbackQuery):
    from config import PAYOUT_ADMINS
    if (not await db.admin_chats(c.from_user.id) and c.from_user.id not in SUPER_ADMINS
            and c.from_user.id not in PAYOUT_ADMINS):
        return await c.answer("Нет доступа.", show_alert=True)
    rows = await db.pool().fetch(
        """
        SELECT w.*, u.username, u.first_name
        FROM rb_withdrawals w LEFT JOIN rb_users u ON u.tg_id = w.tg_id
        WHERE w.status='pending' ORDER BY w.created_at
        """)
    sx = await settings.ctx()
    if not rows:
        return await c.answer("Открытых заявок нет 🎉", show_alert=True)
    lines = []
    for w in rows:
        name = f"@{w['username']}" if w["username"] else (w["first_name"] or str(w["tg_id"]))
        ago = (datetime.now(timezone.utc) - w["created_at"]).total_seconds() / 3600
        wait = f"{int(ago)}ч" if ago >= 1 else f"{int(ago * 60)}м"
        lines.append(f"#{w['id']} — {name} <code>{w['tg_id']}</code>\n"
                     f"   {fmt(w['amount'])} {sx['e_' + w['currency']]} · ждёт {wait}")
    await ui.edit(
        c.message,
        f"💸 <b>Открытые заявки на вывод</b> ({len(rows)})\n\n"
        + "\n".join(lines)
        + "\n\n<i>Карточки с кнопками уже пришли в ЛС. "
          "Тут — просто чтобы ничего не потерять.</i>",
        reply_markup=await kb.pending_list(rows))
    await c.answer()


@router.callback_query(F.data.startswith("a_wdcard:"))
async def cb_resend_card(c: CallbackQuery):
    from config import PAYOUT_ADMINS
    from services import notify
    if (not await db.admin_chats(c.from_user.id) and c.from_user.id not in SUPER_ADMINS
            and c.from_user.id not in PAYOUT_ADMINS):
        return await c.answer("Нет доступа.", show_alert=True)
    wid = int(c.data.split(":")[1])
    wd = await db.pool().fetchrow(
        "SELECT * FROM rb_withdrawals WHERE id=$1 AND status='pending'", wid)
    if not wd:
        return await c.answer("Заявка уже обработана.", show_alert=True)
    await notify.push_admin_card(c.bot, dict(wd))
    await c.answer("Карточка прислана в ЛС.")


# ---------------- баны ----------------
class Unban(StatesGroup):
    query = State()


def _admin_only(uid: int):
    """None если можно, иначе текст отказа. Payout-админам сюда нельзя."""
    return None


async def _render_bans(c, links_mode: bool):
    rows = await db.banned_users(100)
    sx = await settings.ctx()
    if not rows:
        body = "Забаненных нет."
    else:
        lines = []
        for r in rows:
            name = f"@{r['username']}" if r["username"] else (r["first_name"] or "—")
            reason = f" — {r['ban_reason']}" if r["ban_reason"] else ""
            lines.append(f"🚫 {name}  <code>{r['tg_id']}</code>{reason}")
        body = "\n".join(lines)
    hint = ("\n\n<i>Кнопки ниже — ссылки на профили.</i>" if links_mode and rows
            else "\n\n<i>ID указаны рядом с именами — для разбана.</i>" if rows else "")
    await ui.edit(c.message, f"🚫 <b>Баны</b> ({len(rows)})\n\n{body}{hint}",
                  reply_markup=await kb.bans_panel(rows, links_mode, await can_manage(c.from_user.id)))


@router.callback_query(F.data == "a_bans")
async def cb_bans(c: CallbackQuery, state: FSMContext):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.clear()
    await _render_bans(c, links_mode=False)
    await c.answer()


@router.callback_query(F.data == "a_bans_links")
async def cb_bans_links(c: CallbackQuery):
    if not await can_view(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await _render_bans(c, links_mode=True)
    await c.answer()


@router.callback_query(F.data == "a_unban")
async def cb_unban(c: CallbackQuery, state: FSMContext):
    if not await can_manage(c.from_user.id):
        return await c.answer("Только главный админ может разбанивать.", show_alert=True)
    await state.set_state(Unban.query)
    await ui.edit(c.message, "♻️ Пришли ID человека, которого разбанить.",
                  reply_markup=await kb.back_menu())
    await c.answer()


@router.message(Unban.query)
async def unban_input(msg: Message, state: FSMContext):
    await state.clear()
    q = (msg.text or "").strip()
    if not q.isdigit():
        return await ui.answer(msg, "ID — это число. Попробуй ещё раз через «🚫 Баны».",
                               reply_markup=await kb.admin_menu(await can_manage(msg.from_user.id)))
    tg_id = int(q)
    u = await db.get_user(tg_id)
    if not u or not u["banned"]:
        return await ui.answer(msg, "Этот ID не в бане.", reply_markup=await kb.admin_menu(await can_manage(msg.from_user.id)))
    await db.clear_ban(tg_id)
    await db.audit(msg.from_user.id, "unban", {"tg_id": tg_id})
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, tg_id, "✅ С тебя сняли блокировку в системе.")
    await ui.answer(msg, f"✅ <code>{tg_id}</code> разблокирован.",
                    reply_markup=await kb.admin_menu(await can_manage(msg.from_user.id)))


@router.callback_query(F.data.startswith("a_toggleban:"))
async def cb_toggle_ban(c: CallbackQuery):
    if not await can_manage(c.from_user.id):
        return await c.answer("Только главный админ может банить.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    u = await db.get_user(tg_id)
    if not u:
        return await c.answer("Юзер не найден.", show_alert=True)
    now_banned = not u["banned"]
    if now_banned:
        await db.set_ban(tg_id, "бан из поиска", c.from_user.id)
        await db.audit(c.from_user.id, "ban", {"tg_id": tg_id})
        with contextlib.suppress(Exception):
            await ui.send(c.bot, tg_id, "🚫 Тебя заблокировали в системе. "
                                        "Начисления и выводы недоступны.")
        await c.answer("Забанен.")
    else:
        await db.clear_ban(tg_id)
        await db.audit(c.from_user.id, "unban", {"tg_id": tg_id})
        with contextlib.suppress(Exception):
            await ui.send(c.bot, tg_id, "✅ С тебя сняли блокировку в системе.")
        await c.answer("Разбанен.")
    with contextlib.suppress(Exception):
        await c.message.edit_reply_markup(reply_markup=await kb.find_card(tg_id, now_banned, await can_manage(c.from_user.id)))


# ---------------- подтверждение вывода ----------------
@router.callback_query(F.data.startswith("wdok:"))
async def cb_wd_ok(c: CallbackQuery):
    _, wid, ver = c.data.split(":")
    row, err = await withdrawals.confirm(c.from_user.id, int(wid), int(ver))
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    with contextlib.suppress(Exception):
        await ui.edit(c.message, 
            c.message.html_text.split("\n\n⚠️")[0] +
            f"\n\n✅ <b>ВЫПЛАЧЕНО</b> — подтвердил {c.from_user.first_name}", reply_markup=None)
    with contextlib.suppress(Exception):
        await ui.send(
            c.bot, row["tg_id"],
            f"✅ Вывод <b>{fmt(row['amount'])}</b> "
            f"{(await settings.ctx())['e_' + row['currency']]} подтверждён.\n"
            f"Сумма списана с баланса. Если валюту не получил — сразу пиши админу.")
    await c.answer("Проведено.")


@router.callback_query(F.data.startswith("wdno:"))
async def cb_wd_no(c: CallbackQuery):
    _, wid, ver = c.data.split(":")
    row, err = await withdrawals.reject(c.from_user.id, int(wid), "отклонено админом")
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    with contextlib.suppress(Exception):
        await ui.edit(c.message, f"🚫 Заявка #{wid} отклонена.", reply_markup=None)
    with contextlib.suppress(Exception):
        await ui.send(c.bot, row["tg_id"], "🚫 Твоя заявка на вывод отклонена админом. "
                                           "Баланс не тронут.")
    await c.answer("Отклонено.")


# ==================== рассылка reply-клавиатуры ====================
# защита от параллельных рассылок
_kbcast_running = False


@router.callback_query(F.data == "a_kbcast")
async def cb_kbcast_ask(c: CallbackQuery):
    if c.from_user.id not in SUPER_ADMINS and not await db.admin_chats(c.from_user.id):
        return await c.answer("Только для главного админа.", show_alert=True)
    ids = await db.all_active_user_ids()
    await ui.edit(c.message,
        f"📢 <b>Рассылка меню</b>\n\n"
        f"Отправлю кнопку «☰ Меню» всем незабаненным пользователям "
        f"(<b>{len(ids)}</b> чел.). У кого пропала клавиатура — вернётся.\n\n"
        f"Идёт с паузами (~20/сек), займёт ~{max(1, len(ids)//20//60)} мин. "
        f"Прогресс буду показывать здесь.\n\n"
        f"Запустить?",
        reply_markup=await kb.confirm_kbcast())
    await c.answer()


@router.callback_query(F.data == "a_kbcast_go")
async def cb_kbcast_go(c: CallbackQuery):
    global _kbcast_running
    if c.from_user.id not in SUPER_ADMINS and not await db.admin_chats(c.from_user.id):
        return await c.answer("Только для главного админа.", show_alert=True)
    if _kbcast_running:
        return await c.answer("Рассылка уже идёт.", show_alert=True)
    _kbcast_running = True
    await c.answer("Запускаю…")
    ids = await db.all_active_user_ids()
    total = len(ids)
    sent = 0        # успешно доставлено
    blocked = 0     # заблокировали бота / недоступны
    done = 0        # обработано всего
    last_edit = 0.0

    async def refresh(force=False):
        nonlocal last_edit
        import time as _t
        now = _t.time()
        if not force and now - last_edit < 3:
            return
        last_edit = now
        with contextlib.suppress(Exception):
            await ui.edit(c.message,
                f"📢 <b>Рассылка меню</b>\n\n"
                f"Обработано: <b>{done}/{total}</b>\n"
                f"✅ Доставлено: <b>{sent}</b>\n"
                f"🚫 Заблокировали бота: <b>{blocked}</b>",
                reply_markup=None)

    for uid in ids:
        try:
            await c.bot.send_message(uid, "\u2063", reply_markup=kb.menu_reply())  # noqa: ui
            sent += 1
        except Exception:
            blocked += 1
        done += 1
        # темп ~20/сек — безопасно ниже лимита Telegram (30/сек)
        await asyncio.sleep(0.05)
        if done % 25 == 0:
            await refresh()
    await refresh(force=True)
    _kbcast_running = False
    with contextlib.suppress(Exception):
        await ui.edit(c.message,
            f"✅ <b>Рассылка завершена</b>\n\n"
            f"Всего: <b>{total}</b>\n"
            f"✅ Доставлено: <b>{sent}</b>\n"
            f"🚫 Заблокировали бота: <b>{blocked}</b>\n\n"
            f"У всех, кто получил, клавиатура теперь на месте.",
            reply_markup=await kb.back_menu())


# ==================== админ: начисление / изъятие валюты ====================
async def _adj_show_user_card(c, tg_id):
    """Перерисовать карточку юзера (после операции или отмены)."""
    row = await db.get_user(tg_id)
    if not row:
        return
    b = await db.balances(tg_id)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"{sx['e_profile']} <b>{row['first_name']}</b> @{row['username'] or '—'}\n"
        f"<code>{row['tg_id']}</code>{' 🚫 БАН' if row['banned'] else ''}\n\n"
        f"{sx['e_balance']} {sx['e_mushrooms']} {fmt(b['mushrooms'])} | "
        f"{sx['e_coins']} {fmt(b['coins'])} | "
        f"{sx['e_shimcoins']} {shk_fmt(b['shimcoins'])}"
        f"{_tok_line(b)}",
        reply_markup=await kb.find_card(tg_id, row["banned"], await can_manage(c.from_user.id)))


@router.callback_query(F.data.startswith("a_findback:"))
async def cb_adj_back(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await _adj_show_user_card(c, int(c.data.split(":")[1]))
    await c.answer()


@router.callback_query(F.data.startswith("a_give:"))
async def cb_adj_give(c: CallbackQuery):
    if not await can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    await ui.edit(c.message, "➕ <b>Зачисление</b>\n\nВыбери валюту:",
                  reply_markup=await kb.adj_currency(tg_id, "give"))
    await c.answer()


@router.callback_query(F.data.startswith("a_take:"))
async def cb_adj_take(c: CallbackQuery):
    if not await can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    await ui.edit(c.message, "➖ <b>Изъятие</b>\n\nВыбери валюту:",
                  reply_markup=await kb.adj_currency(tg_id, "take"))
    await c.answer()


@router.callback_query(F.data.startswith("a_adjcur:"))
async def cb_adj_currency(c: CallbackQuery, state: FSMContext):
    if not await can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    _, action, tg_id_s, cur = c.data.split(":")
    tg_id = int(tg_id_s)
    await state.set_state(Adjust.amount)
    await state.update_data(adj={"action": action, "tg_id": tg_id, "cur": cur})
    sx = await settings.ctx()
    verb = "зачислить" if action == "give" else "изъять"
    b = await db.balances(tg_id)
    bal_s = shk_fmt(b[cur]) if cur == "shimcoins" else fmt(b[cur])
    # для шимкоинов подсказываем дробный ввод
    hint = ("Напиши сумму. Для шимкоинов можно дробно: <code>10</code>, <code>2.28</code>."
            if cur == "shimcoins"
            else "Напиши сумму. Понимаю <code>100000</code>, <code>100к</code>, <code>1м</code>.")
    await ui.edit(c.message,
        f"{'➕' if action=='give' else '➖'} <b>{verb.capitalize()}</b> "
        f"{sx['e_' + cur]}\n\n"
        f"Баланс игрока: {bal_s}\n\n"
        f"{hint}"
        + ("\n\n⚠️ Изъятие может увести баланс в минус (штраф)." if action == "take" else ""),
        reply_markup=await kb.adj_amount(tg_id, action, cur, show_max=(action == "take")))
    await c.answer()


@router.callback_query(F.data.startswith("a_adjmax:"))
async def cb_adj_max(c: CallbackQuery, state: FSMContext):
    if not await can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    _, action, tg_id_s, cur = c.data.split(":")
    tg_id = int(tg_id_s)
    b = await db.balances(tg_id)
    amount = b[cur]  # забрать всё, что есть
    if amount <= 0:
        return await c.answer("У игрока нет этой валюты.", show_alert=True)
    await state.clear()
    await _apply_adjust(c, tg_id, "take", cur, amount)


@router.message(Adjust.amount, ~F.text.startswith("/"), F.text != "☰ Меню")
async def adj_amount_input(msg: Message, state: FSMContext):
    if not await can_manage(msg.from_user.id):
        return await state.clear()
    data = await state.get_data()
    adj = data.get("adj")
    if not adj:
        return await state.clear()
    from services.amount_parse import parse_amount, shk_parse
    # шимкоины вводятся и хранятся в ЦЕНТАХ (поддержка дробей: 228 -> 22800 центов,
    # 228.50 -> 22850). Грибы/коины — целыми.
    if adj["cur"] == "shimcoins":
        amount = shk_parse(msg.text or "")
    else:
        amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.answer(msg, "Нужно положительное число. Например <code>100к</code>.")
    await state.clear()
    await _apply_adjust(msg, adj["tg_id"], adj["action"], adj["cur"], amount)


async def _apply_adjust(event, tg_id: int, action: str, cur: str, amount: int):
    """Применить начисление/изъятие. event — Message или CallbackQuery."""
    admin_id = event.from_user.id
    delta = amount if action == "give" else -amount
    sx = await settings.ctx()
    import time as _t
    idem = f"adminadj:{admin_id}:{tg_id}:{int(_t.time()*1000)}"
    # Изъятие может уводить в минус (штраф) — обычный db.apply не даёт отрицательный
    # баланс (CHECK amount>=0). Поэтому для минуса пишем напрямую с allow_negative.
    new_bal = await db.apply_admin(tg_id, cur, delta, "admin_" + action, idem, allow_negative=True)
    await db.audit(admin_id, "admin_adjust",
                   {"target": tg_id, "action": action, "amount": amount, "cur": cur, "new": new_bal})

    # форматирование по валюте: шимкоины с центами, остальное целыми
    amt_s = shk_fmt(amount) if cur == "shimcoins" else fmt(amount)
    bal_s = shk_fmt(new_bal) if cur == "shimcoins" else fmt(new_bal)

    # уведомляем игрока
    with contextlib.suppress(Exception):
        if action == "give":
            await ui.send(event.bot if hasattr(event, "bot") else event.message.bot, tg_id,
                f"🎁 Тебе начислили <b>{amt_s}</b> {sx['e_' + cur]}!\n"
                f"Баланс: {bal_s} {sx['e_' + cur]}")
        else:
            await ui.send(event.bot if hasattr(event, "bot") else event.message.bot, tg_id,
                f"⚠️ У тебя изъяли <b>{amt_s}</b> {sx['e_' + cur]}.\n"
                f"Баланс: {bal_s} {sx['e_' + cur]}")

    # подтверждение админу
    verb = "зачислено" if action == "give" else "изъято"
    text = (f"✅ <b>{verb.capitalize()}</b> {amt_s} {sx['e_' + cur]}\n"
            f"Игрок {tg_id}, новый баланс: {bal_s} {sx['e_' + cur]}")
    reply_kb = await kb.admin_menu(await can_manage(admin_id))
    if hasattr(event, "message"):  # CallbackQuery
        await ui.edit(event.message, text, reply_markup=reply_kb)
        await event.answer("Готово.")
    else:  # Message
        await ui.answer(event, text, reply_markup=reply_kb)


# ==================== ОБРАБОТКА КОРЗИНЫ ПО ПОЗИЦИЯМ ====================
@router.callback_query(F.data.startswith("wdi_ok:"))
async def cb_wd_item_ok(c: CallbackQuery):
    from config import PAYOUT_ADMINS
    from services import wd_basket, notify
    if (not await db.admin_chats(c.from_user.id) and c.from_user.id not in SUPER_ADMINS
            and c.from_user.id not in PAYOUT_ADMINS):
        return await c.answer("Нет доступа.", show_alert=True)
    item_id = int(c.data.split(":")[1])
    it, err = await wd_basket.confirm_item(c.from_user.id, item_id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("✅ Позиция выдана")
    # уведомить игрока
    w = await db.pool().fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1", it["wid"])
    sx = await settings.ctx()
    e = sx.get("e_" + it["currency"], "🎫")
    amt = shk_fmt(it["amount"]) if it["currency"] == "shimcoins" else fmt(it["amount"])
    with contextlib.suppress(Exception):
        await ui.send(c.bot, w["tg_id"], f"✅ Вывод выдан: <b>{amt}</b> {e}")
    await notify.refresh_basket_cards(c.bot, it["wid"])


@router.callback_query(F.data.startswith("wdi_no:"))
async def cb_wd_item_no(c: CallbackQuery):
    from config import PAYOUT_ADMINS
    from services import wd_basket, notify
    if (not await db.admin_chats(c.from_user.id) and c.from_user.id not in SUPER_ADMINS
            and c.from_user.id not in PAYOUT_ADMINS):
        return await c.answer("Нет доступа.", show_alert=True)
    item_id = int(c.data.split(":")[1])
    it, err = await wd_basket.reject_item(c.from_user.id, item_id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("❌ Позиция отклонена, средства возвращены игроку")
    w = await db.pool().fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1", it["wid"])
    sx = await settings.ctx()
    e = sx.get("e_" + it["currency"], "🎫")
    amt = shk_fmt(it["amount"]) if it["currency"] == "shimcoins" else fmt(it["amount"])
    with contextlib.suppress(Exception):
        await ui.send(c.bot, w["tg_id"],
                      f"❌ Вывод отклонён: <b>{amt}</b> {e}. Средства вернулись на баланс.")
    await notify.refresh_basket_cards(c.bot, it["wid"])
