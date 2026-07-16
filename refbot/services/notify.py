"""Карточка вывода у админа. Логика 'изменил сумму -> старое удалили, новое прислали'."""
import contextlib
import logging

import db
import keyboards as kb
from services import settings, ui


log = logging.getLogger(__name__)


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _responsible(chat_id: int) -> int | None:
    return await db.pool().fetchval("SELECT owner_id FROM rb_chats WHERE chat_id=$1", chat_id)


async def push_admin_card(bot, wd: dict):
    admin_id = await _responsible(wd["chat_id"])
    if not admin_id:
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
    try:
        m = await ui.send(bot, admin_id, text,
                          reply_markup=await kb.admin_wd_card(wd["id"], wd["version"]))
        await db.pool().execute(
            "UPDATE rb_withdrawals SET admin_chat_id=$1, admin_msg_id=$2 WHERE id=$3",
            m.chat.id, m.message_id, wd["id"])
    except Exception as e:
        log.warning("не смог отправить карточку админу %s: %s", admin_id, e)


async def drop_admin_card(bot, wd: dict):
    if not wd.get("admin_chat_id") or not wd.get("admin_msg_id"):
        return
    with contextlib.suppress(Exception):
        await bot.delete_message(wd["admin_chat_id"], wd["admin_msg_id"])
    await db.pool().execute(
        "UPDATE rb_withdrawals SET admin_chat_id=NULL, admin_msg_id=NULL WHERE id=$1", wd["id"])
