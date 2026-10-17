"""
Хеллоуин-команда !буу (трик-о-трит) и ответы цели !Сладость / !Гадость.

!буу  — реплай на сообщение игрока (раз в день). Пугаем цель.
Цель за 5 минут:
  !Сладость — сладости (грибы/коины по шансам !шайн) + 20% шанс эликсира в довесок
  !Гадость  — осознанно берёт проклятье (20% шанс вернуть его отправителю; 70% если
              отправитель под Сглазом)
  молчит    — по истечении 5 минут гарантированно проклятье (воркер)

Сглаз у пугающего: 50% проклясть самого себя вместо жертвы.
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
    # Сглаз обернулся против пугающего
    if m.get("backfire"):
        kind = m["curse"]["kind"]
        await ui.send(msg.bot, msg.chat.id,
            f"👁 <b>Сглаз обернулся против тебя!</b>\n\n{cname} хотел напугать, "
            f"но проклял сам себя:\n{_curse_card(kind)}")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, caster, f"👁 Сглаз сработал по тебе: {_curse_card(kind)}")
        return
    tname = await profile.display_name(target)
    await ui.send(msg.bot, msg.chat.id,
        f"👻 <b>БУУ!</b>\n\n{cname} подкрался к {tname}!\n\n"
        f"{tname}, у тебя <b>5 минут</b>:\n"
        f"🍬 <code>!Сладость</code> — выпросить сладости\n"
        f"👹 <code>!Гадость</code> — дерзко принять проклятие")
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
    txt = (f"🍬 <b>Сладость!</b>\n\n{await profile.display_name(uid)} выпросил "
           f"<b>{_fmt(m['amount'])}</b> {e_cur}. Счастливого Хеллоуина! 🎃")
    if m.get("bonus_elixir"):
        txt += "\n\n🧪 <b>Бонус!</b> В инвентарь упал эликсир снятия проклятия."
    await ui.send(msg.bot, msg.chat.id, txt)


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.func(lambda t: t and t.lower().split()[0] in ("!гадость", "/гадость")))
async def cmd_trick(msg: Message):
    uid = msg.from_user.id
    m, err = await curses.trick(uid, deliberate=True)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    kind = m["curse"]["kind"]
    caster_name = await profile.display_name(m["caster"])
    target_name = await profile.display_name(m["target"])
    if m["reflected"]:
        await ui.send(msg.bot, msg.chat.id,
            f"🔄 <b>Срикошетило!</b>\n\n{target_name} принял Гадость — и проклятие "
            f"отскочило в {caster_name}:\n{_curse_card(kind)}\n\n"
            f"{caster_name} проклят и отказаться не может, а {target_name} — чист.")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, m["caster"],
                f"🔄 Твоё проклятье вернулось к тебе: {_curse_card(kind)}")
    else:
        await ui.send(msg.bot, msg.chat.id,
            f"👹 <b>Гадость так гадость!</b>\n\n{target_name} сам накликал проклятье:\n"
            f"{_curse_card(kind)}\n\nСнять: эликсиром из магазина или в 00:00 по МСК.")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, m["target"], f"👹 Ты принял проклятье: {_curse_card(kind)}")


async def boo_worker(bot):
    """Истёкшие «бу!» (5 мин без ответа) → гарантированное проклятье цели.
    Плюс: сброс просроченных проклятий (полночь МСК) и «дырявый мешок»."""
    import asyncio
    import logging
    log = logging.getLogger("refbot")
    log.info("boo worker запущен")
    tick = 0
    while True:
        try:
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

        # снять истёкшие проклятья (полночь МСК) — тыквам вернуть имя/эмодзи
        with contextlib.suppress(Exception):
            await curses.clear_expired()

        # «дырявый мешок» — раз в ~10 минут (каждый 20-й тик по 30с)
        if tick % 20 == 0:
            try:
                for uid, amount in await curses.leak_tick():
                    with contextlib.suppress(Exception):
                        await ui.send(bot, uid,
                            f"🕳 Из дырявого мешка выпало <b>{_fmt(amount)}</b> 🍄…")
            except Exception as e:
                log.warning("boo worker (leak): %s", e)

        tick += 1
        await asyncio.sleep(30)
