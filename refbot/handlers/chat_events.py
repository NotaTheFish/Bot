"""
Отслеживание входов/выходов.

ВАЖНО: allowed_updates обязан содержать "chat_member", иначе Telegram
эти апдейты просто не пришлёт и вся механика будет мёртвой.
"""
import contextlib
import logging

from aiogram import Bot, F, Router
from aiogram.filters import ChatMemberUpdatedFilter, IS_MEMBER, IS_NOT_MEMBER
from aiogram.types import ChatMemberUpdated

import db
from config import CURRENCY_EMOJI
from services import referrals

router = Router()
log = logging.getLogger(__name__)

IN = frozenset({"member", "administrator", "creator", "restricted"})


@router.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def on_join(ev: ChatMemberUpdated, bot: Bot):
    u = ev.new_chat_member.user
    if u.is_bot:
        return
    await db.upsert_user(u.id, u.username, u.first_name)
    name = ev.invite_link.name if ev.invite_link else None
    await referrals.on_join(bot, ev.chat.id, u.id, name)

    ref = await db.pool().fetchrow(
        "SELECT * FROM rb_referrals WHERE chat_id=$1 AND invitee_id=$2 AND status='hold'",
        ev.chat.id, u.id)
    if not ref:
        return
    with contextlib.suppress(Exception):
        await bot.send_message(
            ref["inviter_id"],
            f"👥 <b>+1 реферал</b>: {u.first_name}\n"
            f"⏳ <b>{ref['amount']:,}</b> {CURRENCY_EMOJI[ref['currency']]} на удержании.\n"
            f"Зачислится через 3 дня, если он останется в чате."
            .replace(",", " "))


@router.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
async def on_leave(ev: ChatMemberUpdated, bot: Bot):
    u = ev.new_chat_member.user
    if u.is_bot:
        return
    res = await referrals.on_leave(ev.chat.id, u.id)
    if not res:
        return
    kind, ref = res
    if kind == "hold_lost":
        with contextlib.suppress(Exception):
            await bot.send_message(
                ref["inviter_id"],
                f"❌ Реферал {u.first_name} вышел из чата.\n"
                f"<b>{ref['amount']:,}</b> {CURRENCY_EMOJI[ref['currency']]} с удержания сгорели.\n"
                f"Если он вернётся — отсчёт 3 дней начнётся заново."
                .replace(",", " "))
    else:  # burned
        with contextlib.suppress(Exception):
            await bot.send_message(
                ref["inviter_id"],
                f"⚠️ Реферал {u.first_name} вышел из чата уже ПОСЛЕ выплаты.\n"
                f"Выплату не отбираем, но за этот аккаунт больше никто и никогда "
                f"награду не получит.")


@router.my_chat_member(F.new_chat_member.status.in_({"left", "kicked"}))
async def bot_removed(ev: ChatMemberUpdated):
    """Бота выкинули из чата — гасим программу, чтобы не копились мёртвые холды."""
    await db.pool().execute("UPDATE rb_chats SET active=FALSE WHERE chat_id=$1", ev.chat.id)
    log.warning("бот удалён из чата %s, программа отключена", ev.chat.id)
