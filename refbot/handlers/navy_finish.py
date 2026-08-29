"""
Морской бой: завершение, выплата победителю минус 3% (Этап 5).
Пока минимальная выплата через существующий matches.finish.
"""
import contextlib

from aiogram import Router

from services import matches, settings, ui

router = Router()


async def finish_navy(bot, mid: int, winner: int):
    """Завершить морской бой: выплата победителю (как в matches.finish — 3%)."""
    m, err = await matches.finish(mid, winner)
    if err:
        return
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    from config import MATCH_FEE_PCT
    payout = m["stake"] * 2 - int(m["stake"] * MATCH_FEE_PCT / 100)
    wname = None
    u = await __import__("db").get_user(winner)
    wname = (f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else str(winner)))
    with contextlib.suppress(Exception):
        await ui.send(bot, m["origin_chat"],
            f"⚓ <b>Морской бой окончен!</b>\n\n🏆 {wname} потопил весь флот противника!\n"
            f"Выигрыш: <b>{payout:,}</b> {e}".replace(",", " "))
    with contextlib.suppress(Exception):
        await ui.send(bot, winner, f"🏆 Ты выиграл морской бой! +{payout:,} {e}".replace(",", " "))
