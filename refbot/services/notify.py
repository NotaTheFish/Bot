"""Карточка вывода у админа. Логика 'изменил сумму -> старое удалили, новое прислали'."""
import contextlib
import logging

import db
import keyboards as kb
from services import settings, ui


log = logging.getLogger(__name__)


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _recipients(chat_id: int) -> list[int]:
    """Кому слать карточку вывода: владелец чата + все payout-админы из переменной."""
    from config import PAYOUT_ADMINS
    owner = await db.pool().fetchval("SELECT owner_id FROM rb_chats WHERE chat_id=$1", chat_id)
    ids = set(PAYOUT_ADMINS)
    if owner:
        ids.add(owner)
    return list(ids)


async def push_admin_card(bot, wd: dict):
    recipients = await _recipients(wd["chat_id"])
    if not recipients:
        return
    u = await db.get_user(wd["tg_id"])
    b = await db.balances(wd["tg_id"])
    stat = await db.pool().fetchrow(
        "SELECT count(*) FILTER (WHERE status='paid') paid, "
        "count(*) FILTER (WHERE status='hold') hold FROM rb_referrals WHERE inviter_id=$1",
        wd["tg_id"])
    spins = await db.pool().fetchval("SELECT count(*) FROM rb_spins WHERE tg_id=$1", wd["tg_id"])
    uname = f"@{u['username']}" if u["username"] else (u["first_name"] or "—")

    # откуда деньги: касса общая на все чаты, поэтому владельцу важно видеть источник
    origin = await db.pool().fetch(
        """
        SELECT ch.title, sum(l.delta) s
        FROM rb_ledger l
        JOIN rb_referrals r ON r.id = l.ref_id
        JOIN rb_chats ch    ON ch.chat_id = r.chat_id
        WHERE l.tg_id = $1 AND l.reason = 'referral' AND l.currency = $2
        GROUP BY ch.title ORDER BY s DESC
        """, wd["tg_id"], wd["currency"])
    spin_sum = await db.pool().fetchval(
        "SELECT COALESCE(sum(delta),0) FROM rb_ledger "
        "WHERE tg_id=$1 AND reason='roulette' AND currency=$2", wd["tg_id"], wd["currency"]) or 0
    sx = await settings.ctx()
    src = "\n".join(f"  {sx['e_chat']} {r['title']}: {_fmt(r['s'])}" for r in origin)
    if spin_sum:
        src += f"\n  {sx['e_roulette']} Рулетка: {_fmt(spin_sum)}"

    text = (
        f"💸 <b>ЗАЯВКА НА ВЫВОД #{wd['id']}</b>\n\n"
        f"{sx['e_profile']} {uname} | <code>{wd['tg_id']}</code>\n"
        f"{sx['e_balance']} Сумма: <b>{_fmt(wd['amount'])}</b> {sx['e_' + wd['currency']]} "
        f"{sx['l_' + wd['currency']]}\n"
        f"📊 Баланс: {sx['e_mushrooms']} {_fmt(b['mushrooms'])} | "
        f"{sx['e_coins']} {_fmt(b['coins'])}\n"
        f"{sx['e_refs']} Рефералов: {sx['e_paid']} {stat['paid']} | "
        f"{sx['e_hold']} {stat['hold']}\n"
        f"{sx['e_roulette']} Прокруток всего: {spins}\n"
        f"🕐 Регистрация в боте: {u['created_at']:%d.%m.%Y}\n\n"
        f"📥 <b>Откуда заработано</b>\n{src or '  —'}\n\n"
        f"⚠️ Сначала <b>отдай валюту в игре</b>, потом жми «Подтвердить».\n"
        f"Подтверждение = списание с баланса. Отката нет."
    )
    import json
    markup = await kb.admin_wd_card(wd["id"], wd["version"], wd["tg_id"], u)
    sent = []
    for admin_id in recipients:
        try:
            m = await ui.send(bot, admin_id, text, reply_markup=markup)
            sent.append([m.chat.id, m.message_id])
        except Exception as e:
            log.warning("не смог отправить карточку админу %s: %s", admin_id, e)
    # все копии в admin_cards — чтобы удалить у ВСЕХ при отмене/смене суммы
    await db.pool().execute(
        "UPDATE rb_withdrawals SET admin_cards=$1::jsonb, admin_chat_id=$2, admin_msg_id=$3 "
        "WHERE id=$4",
        json.dumps(sent), sent[0][0] if sent else None,
        sent[0][1] if sent else None, wd["id"])


async def drop_admin_card(bot, wd: dict):
    """Удаляет карточку у ВСЕХ, кому слали (владелец + payout-админы)."""
    import json
    cards = wd.get("admin_cards")
    if isinstance(cards, str):
        cards = json.loads(cards)
    if not cards and wd.get("admin_chat_id") and wd.get("admin_msg_id"):
        cards = [[wd["admin_chat_id"], wd["admin_msg_id"]]]
    for chat_id, msg_id in (cards or []):
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, msg_id)
    await db.pool().execute(
        "UPDATE rb_withdrawals SET admin_cards=NULL, admin_chat_id=NULL, admin_msg_id=NULL "
        "WHERE id=$1", wd["id"])


# ==================== КОРЗИНА ВЫВОДА ====================
_TOK_E = {"revive": "❤️‍🔥", "max": "🔱", "partials": "🧩"}


def _cur_e(sx, cur):
    return sx.get("e_" + cur, _TOK_E.get(cur, "🎫"))


def _fmt_amt(cur, amt):
    from services.amount_parse import shk_fmt
    return shk_fmt(amt) if cur == "shimcoins" else f"{amt:,}".replace(",", " ")


async def push_basket_card(bot, wid: int):
    """Карточка корзины админам с кнопками обработки по каждой позиции."""
    from services import wd_basket
    w, items = await wd_basket.get_basket(wid)
    if not w:
        return
    recipients = await _recipients(w["chat_id"])
    if not recipients:
        return
    u = await db.get_user(w["tg_id"])
    uname = f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else "—")
    sx = await settings.ctx()
    text = _basket_card_text(sx, wid, uname, w, items)
    markup = _basket_card_kb(wid, items)
    sent = []
    for admin_id in recipients:
        try:
            m = await ui.send(bot, admin_id, text, reply_markup=markup)
            sent.append([m.chat.id, m.message_id])
        except Exception as e:
            log.warning("не смог отправить корзину-карточку админу %s: %s", admin_id, e)
    import json
    await db.pool().execute(
        "UPDATE rb_withdrawals SET admin_cards=$1::jsonb WHERE id=$2",
        json.dumps(sent), wid)


def _basket_card_text(sx, wid, uname, w, items) -> str:
    lines = [f"💸 <b>ЗАЯВКА НА ВЫВОД #{wid}</b>\n",
             f"{sx['e_profile']} {uname} | <code>{w['tg_id']}</code>\n",
             "<b>Позиции:</b>"]
    for it in items:
        st = {"pending": "⏳", "confirmed": "✅", "rejected": "❌",
              "cancelled": "🚫"}.get(it["status"], "•")
        lines.append(f"  {st} {_cur_e(sx, it['currency'])} {_fmt_amt(it['currency'], it['amount'])}")
    left = sum(1 for it in items if it["status"] == "pending")
    if left:
        lines.append(f"\n⚠️ Сначала <b>отдай в игре</b>, потом жми ✅ по позиции.\n"
                     f"Осталось обработать: {left}")
    else:
        lines.append("\n✅ Все позиции обработаны.")
    return "\n".join(lines)


def _basket_card_kb(wid, items):
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    k = InlineKeyboardBuilder()
    tok_e = {"revive": "❤️‍🔥", "max": "🔱", "partials": "🧩",
             "mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}
    has_pending = False
    for it in items:
        if it["status"] != "pending":
            continue
        has_pending = True
        e = tok_e.get(it["currency"], "🎫")
        amt = _fmt_amt(it["currency"], it["amount"])
        k.button(text=f"✅ Выдал {e} {amt}", callback_data=f"wdi_ok:{it['id']}")
        k.button(text=f"❌ Отклонить {e} {amt}", callback_data=f"wdi_no:{it['id']}")
    k.adjust(2)
    return k.as_markup() if has_pending else None


async def drop_basket_card(bot, wid: int):
    import json
    row = await db.pool().fetchrow("SELECT admin_cards FROM rb_withdrawals WHERE id=$1", wid)
    if not row:
        return
    cards = row["admin_cards"]
    if isinstance(cards, str):
        cards = json.loads(cards)
    for chat_id, msg_id in (cards or []):
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, msg_id)
    await db.pool().execute("UPDATE rb_withdrawals SET admin_cards=NULL WHERE id=$1", wid)


async def refresh_basket_cards(bot, wid: int):
    """Перерисовать карточку заявки у всех админов (после обработки позиции)."""
    import json
    from services import wd_basket
    w, items = await wd_basket.get_basket(wid)
    if not w:
        return
    row = await db.pool().fetchrow("SELECT admin_cards FROM rb_withdrawals WHERE id=$1", wid)
    cards = row["admin_cards"] if row else None
    if isinstance(cards, str):
        cards = json.loads(cards)
    if not cards:
        return
    u = await db.get_user(w["tg_id"])
    uname = f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else "—")
    sx = await settings.ctx()
    text = _basket_card_text(sx, wid, uname, w, items)
    markup = _basket_card_kb(wid, items)
    for chat_id, msg_id in cards:
        with contextlib.suppress(Exception):
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id,
                                        reply_markup=markup, parse_mode="HTML")
