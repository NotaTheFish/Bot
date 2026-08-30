"""
Морской бой: завершение, выплата победителю минус 3% (Этап 5).
Пока минимальная выплата через существующий matches.finish.
"""
import contextlib

from aiogram import Router

from services import matches, settings, ui

router = Router()


async def finish_navy(bot, mid: int, winner: int):
    """Завершить морской бой: выплата победителю (matches.finish — 3%).
    Общее поле в чате уже заменено на итог вызывающей стороной — здесь только
    выплата и личные уведомления."""
    m, err = await matches.finish(mid, winner)
    if err:
        return
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    from config import MATCH_FEE_PCT
    payout = m["stake"] * 2 - int(m["stake"] * MATCH_FEE_PCT / 100)
    with contextlib.suppress(Exception):
        await ui.send(bot, winner, f"🏆 Ты выиграл морской бой! +{payout:,} {e}".replace(",", " "))
    loser = m["p2"] if winner == m["p1"] else m["p1"]
    if loser:
        with contextlib.suppress(Exception):
            await ui.send(bot, loser, "⚓ Твой флот потоплен. В следующий раз повезёт!")
