"""
Викторина: финал — победитель(и) по очкам, выплата минус 3%, делёж при ничьей.
"""
import contextlib

from aiogram import Router

from services import matches, settings, ui, render

router = Router()


async def _name(uid: int):
    u = await __import__("db").get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def finish_quiz(bot, mid: int):
    res, err = await matches.quiz_finish(mid)
    if err:
        return
    sx = await settings.ctx()
    e = sx["e_" + res["currency"]]
    scores = res.get("scores", {})
    # таблица результатов (по убыванию очков)
    ranking = sorted(scores.items(), key=lambda x: -x[1])
    lines = ["🧠 <b>Викторина окончена!</b>", ""]
    for uid_s, sc in ranking:
        nm = await _name(int(uid_s))
        lines.append(f"{nm} — <b>{sc}</b> правильных")
    lines.append("")

    if res.get("draw_all"):
        lines.append("🤝 Полная ничья — ставки возвращены всем.")
    else:
        winners = res.get("winners", [])
        share = res.get("share", 0)
        if len(winners) == 1:
            wname = await _name(winners[0])
            lines.append(f"🏆 Победитель: {wname} (+{_fmt(share)} {e})")
        else:
            wn = ", ".join([await _name(w) for w in winners])
            lines.append(f"🏆 Ничья лидеров: {wn}\nДелёж поровну: +{_fmt(share)} {e} каждому")
        # уведомления победителям
        for w in winners:
            with contextlib.suppress(Exception):
                await ui.send(bot, w, f"🏆 Ты выиграл викторину! +{_fmt(share)} {e}")

    m = await matches.get(mid)
    em = await settings.emoji_map()
    if m and m.get("board_chat_id"):
        # кнопка «Играть ещё»
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        from services.ui import btn
        kb = InlineKeyboardBuilder()
        await btn(kb, "🔁 Играть ещё", f"quiz_again:{res['currency']}:{res['stake']}")
        with contextlib.suppress(Exception):
            await render.edit_by_id(bot, m["board_chat_id"], m["board_msg_id"],
                                    "\n".join(lines), em, reply_markup=kb.as_markup())
