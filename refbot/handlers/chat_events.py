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
from services import referrals, settings, ui

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
        s = await settings.ctx()
        await ui.send(
            bot, ref["inviter_id"],
            f"{s['e_refs']} <b>+1 реферал</b>: {u.first_name}\n"
            f"{s['e_hold']} <b>{ref['amount']:,}</b> {s['e_' + ref['currency']]} на удержании.\n"
            f"Зачислится через 3 дня, если он останется в чате."
            .replace(",", " "))

    # публичное приветствие реферала в чате, куда он зашёл
    with contextlib.suppress(Exception):
        invitee_link = f'<a href="tg://user?id={u.id}">{u.first_name}</a>'
        # имя пригласившего: кликабельная ссылка если знаем, иначе «Грибной Гурман»
        inv = await db.get_user(ref["inviter_id"])
        if inv and inv["first_name"]:
            inviter_txt = f'<a href="tg://user?id={ref["inviter_id"]}">{inv["first_name"]}</a>'
        else:
            inviter_txt = "Грибной Гурман"
        await ui.send(
            bot, ev.chat.id,
            f"🎉 {invitee_link} залетел по приглашению от {inviter_txt}!\n"
            f"Реферал засчитан. Добро пожаловать 🔥")


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
            s = await settings.ctx()
            await ui.send(
                bot, ref["inviter_id"],
                f"{s['e_lost']} Реферал {u.first_name} вышел из чата.\n"
                f"<b>{ref['amount']:,}</b> {s['e_' + ref['currency']]} с удержания сгорели.\n"
                f"Если он вернётся — отсчёт 3 дней начнётся заново."
                .replace(",", " "))
    else:  # burned
        with contextlib.suppress(Exception):
            await ui.send(
                bot, ref["inviter_id"],
                f"⚠️ Реферал {u.first_name} вышел из чата уже ПОСЛЕ выплаты.\n"
                f"Выплату не отбираем, но за этот аккаунт больше никто и никогда "
                f"награду не получит.")


@router.my_chat_member(F.new_chat_member.status.in_({"left", "kicked"}))
async def bot_removed(ev: ChatMemberUpdated):
    """Бота выкинули из чата — гасим программу, чтобы не копились мёртвые холды."""
    await db.pool().execute("UPDATE rb_chats SET active=FALSE WHERE chat_id=$1", ev.chat.id)
    log.warning("бот удалён из чата %s, программа отключена", ev.chat.id)
