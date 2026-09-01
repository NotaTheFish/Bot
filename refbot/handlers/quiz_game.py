"""
Викторина: игровой цикл. Общее сообщение в чате: «Вопрос N/25» + A/B/C/D.
Игроки жмут вариант (тайно). 30 сек или все ответили -> показ правильного +
кто как ответил -> 3 сек -> следующий. После 25-го -> финал.
"""
import asyncio
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, settings, ui, render

router = Router()

# эмодзи-буквы вариантов — как в морском бою (первые 4 из COL_EMO), заменяемы премиумом
from services.navy import COL_EMO as _COL
LETTER_EMO = _COL[:4]   # 🔲 🟥 🟧 🟨
LETTERS = ["A", "B", "C", "D"]

# фоновые задачи по mid (чтобы не плодить)
_tasks: dict[int, asyncio.Task] = {}


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


async def _board(mid: int):
    m = await matches.get(mid)
    if m and m.get("board_chat_id") and m.get("board_msg_id"):
        return m["board_chat_id"], m["board_msg_id"]
    return None


async def start_quiz(bot, mid: int):
    """Запустить викторину: выбрать вопросы, начать цикл."""
    ok = await matches.quiz_start(mid)
    if not ok:
        for p in await matches.lobby_players(mid):
            with contextlib.suppress(Exception):
                await ui.send(bot, p["tg_id"], "Не хватило вопросов для викторины 😔")
        return
    # запустить фоновый цикл
    t = asyncio.create_task(_run(bot, mid))
    _tasks[mid] = t


async def _question_kb(mid: int, q_index: int):
    from services.ui import btn
    kb = InlineKeyboardBuilder()
    for i in range(4):
        await btn(kb, LETTER_EMO[i], f"quiz_ans:{mid}:{q_index}:{i}")
    kb.adjust(2, 2)
    return kb.as_markup()


async def _question_text(mid: int, st: dict) -> str:
    qi = st["q_index"]
    q = st["questions"][qi]
    answered = len(st.get("answers", {}).get(str(qi), {}))
    total = len(await matches.lobby_players(mid))
    bonus = " 🔥 <b>×2 балла!</b>" if q.get("points", 1) >= 2 else ""
    lines = [f"🧠 <b>Вопрос {qi + 1}/{len(st['questions'])}</b>{bonus}", "",
             f"<b>{q['q']}</b>", ""]
    for i, opt in enumerate(q["opts"]):
        lines.append(f"{LETTER_EMO[i]} {opt}")
    lines.append("")
    lines.append(f"⏱ 30 сек · ответили: {answered}/{total}")
    return "\n".join(lines)


async def _run(bot, mid: int):
    """Основной цикл: 25 вопросов."""
    import logging
    log = logging.getLogger("refbot")
    log.info("quiz _run старт mid=%s", mid)
    try:
        while True:
            st = await matches.quiz_state(mid)
            m = await matches.get(mid)
            if not st or not m or m["status"] != "active":
                return
            qi = st["q_index"]
            # показать вопрос
            await _show_question(bot, mid, st)
            # ждать 30 сек или пока все ответят
            for _ in range(30):
                await asyncio.sleep(1)
                cur = await matches.quiz_state(mid)
                if not cur:
                    return
                answered = len(cur.get("answers", {}).get(str(qi), {}))
                total = len(await matches.lobby_players(mid))
                if total and answered >= total:
                    break
                # обновим счётчик ответивших
                await _sync_question(bot, mid, cur)
            # закрыть вопрос — показать правильный + кто как
            res = await matches.quiz_advance(mid)
            await _show_answer(bot, mid, st, res)
            if res["done"]:
                await asyncio.sleep(3)
                await _finish(bot, mid)
                return
            await asyncio.sleep(3)
    except asyncio.CancelledError:
        return
    except Exception as e:
        import logging
        logging.getLogger("refbot").exception("quiz _run упал mid=%s", mid)


async def _show_question(bot, mid: int, st: dict):
    loc = await _board(mid)
    if not loc:
        return
    em = await settings.emoji_map()
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, loc[0], loc[1], await _question_text(mid, st),
                                em, reply_markup=await _question_kb(mid, st["q_index"]))


async def _sync_question(bot, mid: int, st: dict):
    loc = await _board(mid)
    if not loc:
        return
    em = await settings.emoji_map()
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, loc[0], loc[1], await _question_text(mid, st),
                                em, reply_markup=await _question_kb(mid, st["q_index"]))


async def _show_answer(bot, mid: int, st: dict, res: dict):
    loc = await _board(mid)
    if not loc:
        return
    qi = res["q_index"]
    q = st["questions"][qi]
    correct = res["correct_idx"]
    lines = [f"🧠 <b>Вопрос {qi + 1}/{len(st['questions'])}</b>", "",
             f"<b>{q['q']}</b>", "",
             f"✅ Правильный ответ: {LETTER_EMO[correct]} <b>{q['opts'][correct]}</b>", ""]
    # кто как ответил + всего правильных
    scores = res["scores"]
    for uid_s, ok in res["per_player"].items():
        nm = await _name(int(uid_s))
        mark = "✅" if ok else "❌"
        total_right = scores.get(uid_s, 0)
        lines.append(f"{nm} {mark} · всего: {total_right}")
    em = await settings.emoji_map()
    with contextlib.suppress(Exception):
        await render.edit_by_id(bot, loc[0], loc[1], "\n".join(lines), em, reply_markup=None)


async def _finish(bot, mid: int):
    from handlers.quiz_finish import finish_quiz
    with contextlib.suppress(Exception):
        await finish_quiz(bot, mid)
    _tasks.pop(mid, None)


# ---------------- приём ответа ----------------
@router.callback_query(F.data.startswith("quiz_ans:"))
async def cb_answer(c: CallbackQuery):
    _, mid_s, qi_s, choice_s = c.data.split(":")
    mid, qi, choice = int(mid_s), int(qi_s), int(choice_s)
    ok, err = await matches.quiz_answer(mid, c.from_user.id, qi, choice)
    if err:
        return await c.answer(err, show_alert=False)
    await c.answer(f"Твой ответ: {LETTERS[choice]}")
