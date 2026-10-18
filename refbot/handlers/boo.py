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
    """Цель: reply -> автор сообщения; иначе @ник / id из текста."""
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


def _curse_card(kind: str) -> str:
    e, name, desc = curses.CURSE_KINDS.get(kind, ("🔮", kind, ""))
    return f"{e} <b>{name}</b> — {desc}"


def _souvenir_line(curse: dict) -> str:
    if curse.get("souvenir"):
        return ("\n\n🎁 <b>Первая тыква!</b> Титул «Тыквоголовый» и эмодзи 🎃 "
                "теперь твои навсегда — загляни в профиль.")
    return ""


@router.message(F.chat.type.in_({"group", "supergroup"}),
                F.text.func(lambda t: t and t.lower().split()[0] in ("!буу", "/буу", "!бу")))
async def cmd_boo(msg: Message):
    caster = msg.from_user.id
    target = await _target(msg)
    if not target:
        return await ui.reply(msg,
            "👻 Кого пугаем? Ответь командой <code>!буу</code> на сообщение игрока "
            "или укажи: <code>!буу @ник</code>")
    await db.upsert_user(caster, msg.from_user.username, msg.from_user.first_name)
    m, err = await curses.start_boo(caster, target, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")
    cname = await profile.display_name(caster)
    # Сглаз обернулся против пугающего
    if m.get("backfire"):
        kind = m["curse"]["kind"]
        souv = _souvenir_line(m["curse"])
        await ui.send(msg.bot, msg.chat.id,
            f"👁 <b>Сглаз обернулся против тебя!</b>\n\n{cname} хотел напугать, "
            f"но проклял сам себя:\n{_curse_card(kind)}{souv}")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, caster, f"👁 Сглаз сработал по тебе: {_curse_card(kind)}{souv}")
        return
    # цель под Сглазом — мгновенное проклятие, без окна и выбора
    if m.get("instant_curse"):
        kind = m["curse"]["kind"]
        souv = _souvenir_line(m["curse"])
        tname = await profile.display_name(target)
        await ui.send(msg.bot, msg.chat.id,
            f"👁 <b>БУУ!</b> {cname} напугал {tname} — под Сглазом не отвертеться, "
            f"проклятие сразу:\n{_curse_card(kind)}{souv}")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, target, f"👁 Тебя мгновенно прокляли: {_curse_card(kind)}{souv}")
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


@router.message(F.text.func(lambda t: t and t.lower().split()[0] in ("!проклятия", "/проклятия", "!проклятья")))
async def cmd_curses(msg: Message):
    uid = msg.from_user.id
    rows = await curses.active(uid)
    if not rows:
        return await ui.reply(msg, "✨ На тебе нет проклятий. Чисто!")
    from collections import Counter
    cnt = Counter(r["kind"] for r in rows)
    lines = [f"🔮 <b>Твои проклятия ({len(rows)}):</b>", ""]
    for kind, n in cnt.items():
        e, name, desc = curses.CURSE_KINDS.get(kind, ("🔮", kind, ""))
        extra = f" ×{n}" if n > 1 else ""
        if kind == "evil":
            hp = min((int(r["meta"].get("hp", 5)) for r in rows if r["kind"] == "evil"), default=5)
            extra = f" — осталось зарядов {hp}/5"
        lines.append(f"{e} <b>{name}</b>{extra}\n    <i>{desc}</i>")
    lines.append("")
    lines.append("Снять: 🧪 эликсир из магазина (Сглаз — 5 эликсиров) или само спадёт в 00:00 по МСК.")
    await ui.reply(msg, "\n".join(lines))


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
    reward_line = ("\n\n🍶 <b>Повезло!</b> За смелость выпало зелье <b>«Гнилая настойка»</b> "
                   "— эффект неизвестен. Выпей его в инвентаре.") if m.get("reward") else ""
    souv = _souvenir_line(m["curse"])
    if m["reflected"]:
        await ui.send(msg.bot, msg.chat.id,
            f"🔄 <b>Срикошетило!</b>\n\n{target_name} принял Гадость — и проклятие "
            f"отскочило в {caster_name}:\n{_curse_card(kind)}\n\n"
            f"{caster_name} проклят и отказаться не может, а {target_name} — чист.{reward_line}")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, m["caster"],
                f"🔄 Твоё проклятье вернулось к тебе: {_curse_card(kind)}{souv}")
    else:
        await ui.send(msg.bot, msg.chat.id,
            f"👹 <b>Гадость так гадость!</b>\n\n{target_name} сам накликал проклятье:\n"
            f"{_curse_card(kind)}{souv}\n\nСнять: эликсиром из магазина или в 00:00 по МСК.{reward_line}")
        with contextlib.suppress(Exception):
            await ui.send(msg.bot, m["target"], f"👹 Ты принял проклятье: {_curse_card(kind)}{souv}")


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
                souv = _souvenir_line(m["curse"])
                tname = await profile.display_name(m["target"])
                with contextlib.suppress(Exception):
                    if m.get("chat_id"):
                        await ui.send(bot, m["chat_id"],
                            f"🕯 <b>Время вышло!</b>\n\n{tname} не ответил — "
                            f"проклятье настигло его:\n{_curse_card(kind)}{souv}")
                with contextlib.suppress(Exception):
                    await ui.send(bot, m["target"],
                        f"🕯 Ты не успел ответить на «бу!» — проклятье: {_curse_card(kind)}{souv}")
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
