"""
Хеллоуин-команда !буу (трик-о-трит) и ответы цели !Сладость / !Гадость.

!буу  — реплай на сообщение игрока (раз в день). Пугаем цель.
Цель за 5 минут:
  !Сладость — сладости (грибы/коины по шансам !шайн, вплоть до джекпота)
  !Гадость  — осознанно берёт проклятье
  молчит    — по истечении 5 минут гарантированно проклятье (воркер)
"""
import contextlib

from aiogram import F, Router
from aiogram.types import Message

import db
from services import ui, settings, profile, curses

router = Router()


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _target(msg: Message):
    """Цель = автор сообщения, на которое ответили (не бот, не ты сам)."""
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        if u.is_bot:
            return None
        return u.id
    return None


def _curse_card(kind: str) -> str:
    e, name, desc = curses.CURSE_KINDS.get(kind, ("🔮", kind, ""))
    return f"{e} <b>{name}</b> — {desc}"


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.func(lambda t: t and t.lower().split()[0] in ("!буу", "/буу", "!бу")))
async def cmd_boo(msg: Message):
    caster = msg.from_user.id
    target = await _target(msg)
    if not target:
        return await ui.reply(msg,
            "👻 Кого пугаем? Ответь командой <code>!буу</code> на сообщение игрока.")
    await db.upsert_user(caster, msg.from_user.username, msg.from_user.first_name)
    m, err = await curses.start_boo(caster, target, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    cname = await profile.display_name(caster)
    tname = await profile.display_name(target)
    await ui.send(msg.bot, msg.chat.id,
        f"👻 <b>БУУ!</b>\n\n{cname} подкрался к {tname}!\n\n"
        f"{tname}, у тебя <b>5 минут</b>:\n"
        f"🍬 <code>!Сладость</code> — выпросить сладости (грибы/коины)\n"
        f"👹 <code>!Гадость</code> — дерзко принять проклятие\n\n"
        f"Промолчишь — проклятье прилетит само.")
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, target,
            f"👻 Тебя напугал {cname}! Ответь в чате <code>!Сладость</code> "
            f"или <code>!Гадость</code> за 5 минут, иначе получишь проклятье.")


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.func(lambda t: t and t.lower().split()[0] in ("!сладость", "/сладость")))
async def cmd_treat(msg: Message):
    uid = msg.from_user.id
    m, err = await curses.treat(uid)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    sx = await settings.ctx()
    e_cur = sx.get("e_" + m["currency"], "")
    await ui.send(msg.bot, msg.chat.id,
        f"🍬 <b>Сладость!</b>\n\n{await profile.display_name(uid)} выпросил "
        f"<b>{_fmt(m['amount'])}</b> {e_cur}. Счастливого Хеллоуина! 🎃")


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.func(lambda t: t and t.lower().split()[0] in ("!гадость", "/гадость")))
async def cmd_trick(msg: Message):
    uid = msg.from_user.id
    m, err = await curses.trick(uid, deliberate=True)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    kind = m["curse"]["kind"]
    await ui.send(msg.bot, msg.chat.id,
        f"👹 <b>Гадость так гадость!</b>\n\n{await profile.display_name(uid)} "
        f"сам накликал проклятье:\n{_curse_card(kind)}\n\n"
        f"Снять: эликсиром из магазина или оно спадёт в 00:00 по МСК.")
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, uid, f"👹 Ты принял проклятье: {_curse_card(kind)}")


async def boo_worker(bot):
    """Истёкшие «бу!» (5 мин без ответа) → гарантированное проклятье.
    Плюс: сброс просроченных проклятий (полночь МСК) и «дырявый мешок»."""
    import asyncio
    import logging
    log = logging.getLogger("refbot")
    log.info("boo worker запущен")
    tick = 0
    while True:
        try:
            # 1) истёкшие окна трик-о-трит
            for m in await curses.expire_boos():
                kind = m["curse"]["kind"]
                tname = await profile.display_name(m["target"])
                with contextlib.suppress(Exception):
                    if m.get("chat_id"):
                        await ui.send(bot, m["chat_id"],
                            f"🕯 <b>Время вышло!</b>\n\n{tname} не ответил — "
                            f"проклятье настигло его:\n{_curse_card(kind)}")
                with contextlib.suppress(Exception):
                    await ui.send(bot, m["target"],
                        f"🕯 Ты не успел ответить на «бу!» — проклятье: {_curse_card(kind)}")
        except Exception as e:
            log.warning("boo worker (expire): %s", e)

        # 2) снять истёкшие проклятья (полночь МСК)
        with contextlib.suppress(Exception):
            await curses.clear_expired()

        # 3) «дырявый мешок» — раз в ~5 минут (каждый 10-й тик по 30с)
        if tick % 10 == 0:
            try:
                for uid, amount in await curses.leak_tick():
                    with contextlib.suppress(Exception):
                        await ui.send(bot, uid,
                            f"🕳 Из дырявого мешка выпало <b>{_fmt(amount)}</b> 🍄…")
            except Exception as e:
                log.warning("boo worker (leak): %s", e)

        tick += 1
        await asyncio.sleep(30)
