"""
Команды воровства: !сшайнить (атака) и !шимщит (защита).
Атака: reply на сообщение жертвы, или !сшайнить @ник / id.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import Message

import db
from services import ui, settings, steal, profile

router = Router()


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _resolve_target(msg: Message):
    """Определить жертву: reply -> автор сообщения; иначе @ник/id из текста."""
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        if u.is_bot:
            return None
        return u.id
    parts = (msg.text or "").split()
    for w in parts[1:]:
        w = w.strip().lstrip("@")
        if not w:
            continue
        uid = await db.resolve_username(w.lower())
        if not uid and w.isdigit():
            uid = int(w)
        if uid:
            return uid
    return None


@router.message(F.text.func(lambda t: t and t.lower().split()[0] in ("!сшайнить", "/сшайнить")))
async def cmd_steal(msg: Message):
    thief = msg.from_user.id
    victim = await _resolve_target(msg)
    if not victim:
        return await ui.reply(msg,
            "Кого обворовать? Ответь на сообщение игрока или укажи: "
            "<code>!сшайнить @ник</code>")
    m, err = await steal.start_steal(thief, victim, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    tname = await profile.display_name(thief)
    vname = await profile.display_name(victim)
    if m.get("instant_shield"):
        # у жертвы был временный щит — атака отбита мгновенно
        await ui.reply(msg,
            f"🛡 {vname} под защитой (щит)! Кража сорвалась, {tname} ушёл ни с чем.")
        return
    # сигнал жертве и вору
    await ui.send(msg.bot, msg.chat.id,
        f"🥷 <b>Налёт!</b>\n\n{tname} пытается обчистить {vname}!\n\n"
        f"{vname}, у тебя <b>20 минут</b> — напиши <code>!шимщит</code>, чтобы отбиться "
        f"и наказать вора. Иначе грибы уплывут.")
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, victim,
            f"🚨 <b>Тебя обворовывают!</b>\n\n{tname} совершает налёт. "
            f"У тебя 20 минут — напиши <code>!шимщит</code> в чате, чтобы отбиться.")


@router.message(F.text.func(lambda t: t and t.lower().split()[0] in ("!шимщит", "/шимщит", "!шимщ")))
async def cmd_defend(msg: Message):
    uid = msg.from_user.id
    m, err = await steal.defend(uid)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    pay = m.get("_pay", 0)
    tname = await profile.display_name(m["thief"])
    vname = await profile.display_name(uid)
    await ui.send(msg.bot, msg.chat.id,
        f"🛡 <b>Отбито!</b>\n\n{vname} дал отпор! Вор {tname} наказан "
        f"и отдаёт <b>{_fmt(pay)}</b> 🍄 в качестве компенсации.")
    # счётчики исхода (защита удачна для жертвы, неудача для вора)
    await _apply_counters(m, defended=True)


async def _apply_counters(m: dict, defended: bool):
    """Обновить счётчики воровства/защиты + стрики. Вызывается при завершении атаки."""
    from services import counters as _cnt
    thief, victim = m["thief"], m["victim"]
    pay = m.get("_pay", 0)
    try:
        if defended:
            # жертва защитилась: удача защиты (victim), неудача кражи (thief)
            await _cnt.bump(victim, _cnt.C_DEF_WIN)
            await _cnt.bump_max(victim, _cnt.C_DEF_WIN_MAX, pay)
            await _streak(victim, _cnt.C_DEF_WIN_STREAK, _cnt.C_DEF_LOSS_STREAK)
            await _cnt.bump(thief, _cnt.C_STEAL_LOSS)
            await _cnt.bump_max(thief, _cnt.C_STEAL_LOSS_MAX, pay)
            await _streak(thief, _cnt.C_STEAL_LOSS_STREAK, _cnt.C_STEAL_WIN_STREAK)
        else:
            # кража удалась: удача кражи (thief), неудача защиты (victim)
            await _cnt.bump(thief, _cnt.C_STEAL_WIN)
            await _cnt.bump_max(thief, _cnt.C_STEAL_WIN_MAX, pay)
            await _streak(thief, _cnt.C_STEAL_WIN_STREAK, _cnt.C_STEAL_LOSS_STREAK)
            await _cnt.bump(victim, _cnt.C_DEF_LOSS)
            await _cnt.bump_max(victim, _cnt.C_DEF_LOSS_MAX, pay)
            await _streak(victim, _cnt.C_DEF_LOSS_STREAK, _cnt.C_DEF_WIN_STREAK)
    except Exception:
        pass


async def _streak(uid: int, inc_counter: str, reset_counter: str):
    """Инкремент стрика inc и сброс противоположного reset."""
    from services import counters as _cnt
    await _cnt.bump(uid, inc_counter)
    # сбросить противоположный стрик в 0
    with contextlib.suppress(Exception):
        await db.pool().execute(
            "INSERT INTO rb_counters (tg_id, counter_type, value, updated_at) "
            "VALUES ($1,$2,0, now()) ON CONFLICT (tg_id, counter_type) DO UPDATE SET value=0",
            uid, reset_counter)


async def steal_worker(bot):
    """Завершает просроченные атаки (20 мин без защиты -> кража)."""
    import asyncio, logging
    log = logging.getLogger("refbot")
    log.info("steal worker запущен")
    while True:
        try:
            finished = await steal.expire_due(bot)
            for m in finished:
                defended = m.get("auto_shield", False)
                pay = m.get("_pay", 0)
                tname = await profile.display_name(m["thief"])
                vname = await profile.display_name(m["victim"])
                chat_id = m.get("chat_id")
                if defended:
                    # сработал разовый авто-щит жертвы
                    txt = (f"🛡 <b>Щит сработал!</b>\n\n{vname} был под защитой — "
                           f"кража сорвалась. {tname} наказан на <b>{_fmt(pay)}</b> 🍄.")
                    await _apply_counters(m, defended=True)
                else:
                    txt = (f"🥷 <b>Налёт удался!</b>\n\n{tname} обчистил {vname} "
                           f"на <b>{_fmt(pay)}</b> 🍄. Надо было защищаться!")
                    await _apply_counters(m, defended=False)
                with contextlib.suppress(Exception):
                    if chat_id:
                        await ui.send(bot, chat_id, txt)
                # личка обоим
                with contextlib.suppress(Exception):
                    if defended:
                        await ui.send(bot, m["victim"], f"🛡 Твой щит отбил налёт {tname}! +{_fmt(pay)} 🍄")
                        await ui.send(bot, m["thief"], f"🛡 {vname} был под щитом — ты потерял {_fmt(pay)} 🍄.")
                    else:
                        await ui.send(bot, m["victim"], f"🥷 Тебя обокрали на {_fmt(pay)} 🍄 (не успел !шимщит).")
                        await ui.send(bot, m["thief"], f"🥷 Налёт удался! +{_fmt(pay)} 🍄 с {vname}.")
        except Exception as e:
            log.warning("steal worker: %s", e)
        await __import__("asyncio").sleep(15)
