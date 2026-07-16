"""Карточка вывода у админа. Логика 'изменил сумму -> старое удалили, новое прислали'."""
import contextlib
import logging

import db
import keyboards as kb
from config import CURRENCY_EMOJI, CURRENCY_NAME

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

    text = (
        f"💸 <b>ЗАЯВКА НА ВЫВОД #{wd['id']}</b>\n\n"
        f"👤 {uname} | <code>{wd['tg_id']}</code>\n"
        f"💰 Сумма: <b>{_fmt(wd['amount'])}</b> {CURRENCY_EMOJI[wd['currency']]} "
        f"{CURRENCY_NAME[wd['currency']]}\n"
        f"📊 Баланс: 🍄 {_fmt(b['mushrooms'])} | 🪙 {_fmt(b['coins'])}\n"
        f"👥 Рефералов: ✅ {stat['paid']} | ⏳ {stat['hold']}\n"
        f"🎰 Прокруток всего: {spins}\n"
        f"🕐 Регистрация в боте: {u['created_at']:%d.%m.%Y}\n\n"
        f"⚠️ Сначала <b>отдай валюту в игре</b>, потом жми «Подтвердить».\n"
        f"Подтверждение = списание с баланса. Отката нет."
    )
    try:
        m = await bot.send_message(admin_id, text,
                                   reply_markup=kb.admin_wd_card(wd["id"], wd["version"]))
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
