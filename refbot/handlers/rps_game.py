"""
Камень-ножницы-бумага: раунды, тайный выбор, вскрытие, выбывание.
Одно общее поле в чате: показывает счётчик «выбрали M из N» и кнопки знаков.
Сам выбор тайный (никто не видит, что выбрал другой, до вскрытия).
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, rps, settings, ui, render

router = Router()


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


async def _round_text(mid: int, note: str = "") -> str:
    m = await matches.get(mid)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    chosen, total = await matches.round_progress(mid)
    players = await matches.lobby_players(mid, "playing")
    if total == 0:
        # диагностика: почему ноль игроков
        import logging
        allst = await db.pool().fetch(
            "SELECT tg_id, status, staked FROM rb_match_players WHERE mid=$1", mid)
        logging.getLogger("refbot").warning(
            "rps _round_text mid=%s: playing=0! все игроки: %s",
            mid, [(r["tg_id"], r["status"], r["staked"]) for r in allst])
    names = ", ".join([await _name(p["tg_id"]) for p in players])
    staked = await db.pool().fetchval(
        "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND staked", mid)
    head = (f"🤓😎🥸 <b>Камень-ножницы-бумага</b>\n"
            f"Банк: <b>{_fmt(m['stake']*(staked or 0))}</b> {e}\n"
            f"В игре: <b>{total}</b> — {names}\n")
    if note:
        head += f"\n{note}\n"
    head += f"\n✅ Выбрали ход: <b>{chosen} из {total}</b>"
    return head


async def _round_kb(mid: int):
    from services.ui import btn
    er = await settings.emoji("rps_rock")
    es = await settings.emoji("rps_scissors")
    ep = await settings.emoji("rps_paper")
    kb = InlineKeyboardBuilder()
    await btn(kb, f"{er} Камень", f"rps_ch:{mid}:rock")
    await btn(kb, f"{es} Ножницы", f"rps_ch:{mid}:scissors")
    await btn(kb, f"{ep} Бумага", f"rps_ch:{mid}:paper")
    kb.adjust(3)
    return kb.as_markup()


async def _sync(bot, mid: int, note: str = "", kb=True):
    m = await matches.get(mid)
    if not m or not m.get("board_msg_id"):
        return
    if m["status"] != "active":
        return   # игра завершена — не перерисовываем поле раунда
    # не рисуем поле без игроков (гонка после финиша)
    players = await matches.lobby_players(mid, "playing")
    if len(players) < 1:
        return
    em = await settings.emoji_map()
    markup = await _round_kb(mid) if kb else None
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, m["board_chat_id"], m["board_msg_id"],
                                await _round_text(mid, note), em, reply_markup=markup)


async def start_round(bot, mid: int, first: bool = False):
    """Запустить раунд выбора: активировать матч, поставить дедлайн, показать кнопки."""
    m = await matches.get(mid)
    if not m:
        return
    players = await matches.lobby_players(mid, "playing")
    if len(players) < 2:
        # защита: не запускаем раунд без игроков (иначе «В игре: 0»)
        import logging
        logging.getLogger("refbot").warning(
            "rps start_round mid=%s: игроков %d < 2, отмена", mid, len(players))
        m, _ = await matches.cancel_lobby(mid)
        return
    if first:
        await db.pool().execute("UPDATE rb_matches SET status='active' WHERE id=$1", mid)
    await matches.set_round_deadline(mid, matches.RPS_MOVE_MINUTES)
    await _sync(bot, mid, note="Выбирайте ход! У вас 5 минут.")


@router.callback_query(F.data.startswith("rps_ch:"))
async def cb_choice(c: CallbackQuery):
    _, mid_s, choice = c.data.split(":")
    mid = int(mid_s)
    # игра ещё идёт?
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return await c.answer("Игра уже завершена.", show_alert=True)
    ok, err = await matches.set_choice(mid, c.from_user.id, choice)
    if err:
        return await c.answer(err, show_alert=True)
    await c.answer(f"Ты выбрал: {rps.NAMES[choice]}")
    # обновим счётчик (тайно). DM-режим или чат?
    is_dm = mid in _dm_msgs
    if is_dm:
        await _sync_dm(c.bot, mid, note="Выбирайте ход! У вас 5 минут.")
    else:
        await _sync(c.bot, mid, note="Выбирайте ход! У вас 5 минут.")
    chosen, total = await matches.round_progress(mid)
    if chosen >= total and total > 0:
        await _reveal(c.bot, mid)


async def _sync_any(bot, mid: int, note: str = "", kb=True):
    """Универсальная синхронизация: DM-режим (поля в личках) или чат (одно поле)."""
    if mid in _dm_msgs:
        await _sync_dm(bot, mid, note, kb)
    else:
        await _sync(bot, mid, note, kb)


async def _reveal(bot, mid: int):
    """Вскрытие раунда: определить выбывших, продолжить или финал."""
    choices = await matches.active_choices(mid)
    if not choices:
        return
    outcome, survivors, eliminated = rps.resolve_round(choices)
    summary = rps.round_summary(choices, _name)

    if outcome == "draw":
        # ничья — новый раунд теми же игроками
        await matches.apply_elimination(mid, [])   # очистить выборы
        await matches.set_round_deadline(mid, matches.RPS_MOVE_MINUTES)
        await _sync_any(bot, mid, note=f"{summary}\nИграем заново!")
        return

    # есть выбывшие
    await matches.apply_elimination(mid, eliminated)
    # уведомим выбывших
    for uid in eliminated:
        with contextlib.suppress(Exception):
            await ui.send(bot, uid, "❌ Ты выбыл из игры камень-ножницы-бумага.")

    remaining = await matches.lobby_players(mid, "playing")
    if len(remaining) == 1:
        # победитель
        winner = remaining[0]["tg_id"]
        await _finish(bot, mid, winner, summary)
        return
    # ещё несколько — новый раунд
    await matches.set_round_deadline(mid, matches.RPS_MOVE_MINUTES)
    elim_names = ", ".join([await _name(u) for u in eliminated])
    await _sync_any(bot, mid, note=f"{summary}\nВыбыли: {elim_names}. Следующий раунд!")


async def _finish(bot, mid: int, winner: int, summary: str):
    m, err = await matches.finish_multiplayer(mid, winner)
    if err:
        return
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    staked = await db.pool().fetchval(
        "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND staked", mid)
    from config import MATCH_FEE_PCT
    bank = m["stake"] * (staked or 0)
    losers_pot = m["stake"] * ((staked or 1) - 1)
    payout = bank - int(losers_pot * MATCH_FEE_PCT / 100)
    wname = await _name(winner)
    text = (f"🤓😎🥸 <b>Камень-ножницы-бумага</b>\n\n"
            f"{summary}\n\n"
            f"🏆 <b>{wname} победил(а)!</b>\n"
            f"Выигрыш: <b>{_fmt(payout)}</b> {e}")
    em = await settings.emoji_map()

    # кнопка «Играть ещё» — новое лобби с той же ставкой/валютой
    async def _again_kb(chat_id):
        from services.ui import btn
        kb = InlineKeyboardBuilder()
        await btn(kb, "🔁 Играть ещё", f"rps_again:{m['currency']}:{m['stake']}")
        return kb.as_markup()

    if mid in _dm_msgs:
        # DM-режим: обновить поле у каждого игрока + разослать всем итог
        slots = _dm_msgs.get(mid, {})
        allp = await db.pool().fetch(
            "SELECT tg_id FROM rb_match_players WHERE mid=$1 AND status<>'dq' AND status<>'left'", mid)
        for r in allp:
            uid = r["tg_id"]
            mreid = slots.get(uid)
            kb = await _again_kb(uid)
            if mreid:
                with contextlib.suppress(Exception):
                    await render.edit_by_id(bot, uid, mreid, text, em, reply_markup=kb)
            else:
                with contextlib.suppress(Exception):
                    await ui.send(bot, uid, text, reply_markup=kb)
        _dm_msgs.pop(mid, None)
    else:
        m2 = await matches.get(mid)
        kb = await _again_kb(m2["board_chat_id"])
        with contextlib.suppress(Exception):
            await render.edit_by_id(bot, m2["board_chat_id"], m2["board_msg_id"], text, em,
                                    reply_markup=kb)
    with contextlib.suppress(Exception):
        await ui.send(bot, winner, f"🏆 Ты выиграл камень-ножницы-бумага! +{_fmt(payout)} {e}")


async def timeout_worker(bot):
    """Тайм-ауты КНБ: лобби (15 мин без старта) и раунд (5 мин на выбор)."""
    import asyncio
    log = __import__("logging").getLogger("refbot")
    log.info("rps timeout worker запущен")
    while True:
        try:
            # 1) протухшие лобби — отменить, вернуть ставки
            rows = await db.pool().fetch(
                "SELECT id FROM rb_matches WHERE game='rps' AND status='lobby' "
                "AND search_deadline IS NOT NULL AND search_deadline < now()")
            for r in rows:
                m, err = await matches.cancel_lobby(r["id"])
                if not err and m:
                    with contextlib.suppress(Exception):
                        await bot.edit_message_text(
                            "⏳ Набор игроков истёк (15 мин). Игра отменена, ставки возвращены.",
                            chat_id=m["board_chat_id"], message_id=m["board_msg_id"])
                    # уведомить каждого участника в ЛС
                    sx = await settings.ctx()
                    e = sx["e_" + m["currency"]]
                    for uid in m.get("refunded", []):
                        with contextlib.suppress(Exception):
                            await ui.send(bot, uid,
                                f"⏳ Игра камень-ножницы-бумага не набрала игроков за 15 минут "
                                f"и отменена.\nСтавка <b>{_fmt(m['stake'])}</b> {e} возвращена.")
            # 2) протухший раунд — дисквалифицировать не выбравших, довскрыть
            rows = await db.pool().fetch(
                "SELECT id FROM rb_matches WHERE game='rps' AND status='active' "
                "AND move_deadline IS NOT NULL AND move_deadline < now()")
            for r in rows:
                mid = r["id"]
                dq = await matches.disqualify_no_choice(mid)
                for uid in dq:
                    with contextlib.suppress(Exception):
                        await ui.send(bot, uid,
                            "⏳ Ты не выбрал ход за 5 минут — дисквалификация. Ставка возвращена.")
                # после дисквала — вскрыть тем, кто выбрал (или объявить победителя)
                await _resolve_after_timeout(bot, mid)
        except Exception as e:
            log.warning("rps timeout worker: %s", e)
        await __import__("asyncio").sleep(20)


async def _resolve_after_timeout(bot, mid: int):
    """После дисквалификации по тайм-ауту: если остался один — победа; если есть
    выборы — вскрыть; если никого — закрыть."""
    remaining = await matches.lobby_players(mid, "playing")
    if len(remaining) == 0:
        # все дисквалифицированы — просто закрыть матч (ставки уже возвращены)
        await db.pool().execute(
            "UPDATE rb_matches SET status='cancelled', finished_at=now() WHERE id=$1", mid)
        m = await matches.get(mid)
        with contextlib.suppress(Exception):
            await bot.edit_message_text("Игра завершена — никто не сделал ход.",
                                        chat_id=m["board_chat_id"], message_id=m["board_msg_id"])
        return
    if len(remaining) == 1:
        winner = remaining[0]["tg_id"]
        await _finish(bot, mid, winner, "Соперники не сделали ход.")
        return
    # остались несколько выбравших — вскрыть раунд
    choices = await matches.active_choices(mid)
    chosen, total = await matches.round_progress(mid)
    # вскрываем только если все оставшиеся выбрали
    if chosen >= total and total > 0:
        await _reveal(bot, mid)
    else:
        # часть ещё выбирает — продлить дедлайн
        await matches.set_round_deadline(mid, matches.RPS_MOVE_MINUTES)
        await _sync_any(bot, mid, note="Часть игроков выбыла по тайм-ауту. Продолжаем!")


# ==================== КНБ в личках (direct / онлайн-пул) ====================
# Поле раундов у каждого игрока в его ЛС, синхронизируется. Выбор тайный.
# Храним message_id поля каждого игрока в rb_match_players? Нет — шлём заново
# каждый раунд и держим id в памяти.
_dm_msgs: dict[int, dict[int, int]] = {}   # mid -> {tg_id: message_id}


async def _dm_text(mid: int, note: str = "") -> str:
    m = await matches.get(mid)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    chosen, total = await matches.round_progress(mid)
    players = await matches.lobby_players(mid, "playing")
    names = ", ".join([await _name(p["tg_id"]) for p in players])
    staked = await db.pool().fetchval(
        "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND staked", mid)
    head = (f"🤓😎🥸 <b>Камень-ножницы-бумага</b>\n"
            f"Банк: <b>{_fmt(m['stake']*(staked or 0))}</b> {e}\n"
            f"В игре: <b>{total}</b> — {names}\n")
    if note:
        head += f"\n{note}\n"
    head += f"\n✅ Выбрали ход: <b>{chosen} из {total}</b>"
    return head


async def start_round_dm(bot, mid: int, first: bool = False):
    """Запустить раунд в личках всех игроков."""
    m = await matches.get(mid)
    if not m:
        return
    if first:
        await db.pool().execute("UPDATE rb_matches SET status='active' WHERE id=$1", mid)
    await matches.set_round_deadline(mid, matches.RPS_MOVE_MINUTES)
    await _sync_dm(bot, mid, note="Выбирайте ход! У вас 5 минут.")


async def _sync_dm(bot, mid: int, note: str = "", kb=True):
    """Обновить поле у каждого игрока в ЛС (или прислать, если ещё нет)."""
    m = await matches.get(mid)
    if not m or m["status"] != "active":
        return   # игра завершена — не перерисовываем
    players = await matches.lobby_players(mid, "playing")
    if len(players) < 1:
        return
    text = await _dm_text(mid, note)
    em = await settings.emoji_map()
    markup = await _round_kb(mid) if kb else None
    slots = _dm_msgs.setdefault(mid, {})
    for p in players:
        uid = p["tg_id"]
        mreid = slots.get(uid)
        if mreid:
            with contextlib.suppress(Exception):
                await render.edit_by_id(bot, uid, mreid, text, em, reply_markup=markup)
        else:
            with contextlib.suppress(Exception):
                msg = await ui.send(bot, uid, text, reply_markup=markup)
                if msg:
                    slots[uid] = msg.message_id
