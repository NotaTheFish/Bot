"""
Достижения игрока: список с прогресс-баром, страницы, рич-кнопка «Получить».
Скрытые достижения не показывают условия, но показывают прогресс и награду.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, settings, achievements as ach, richmsg

router = Router()

PER_PAGE = 5


def _bar(cur: int, target: int, style: str = "percent") -> str:
    pct = min(100, int(cur / target * 100)) if target else 0
    filled = round(pct / 20)
    b = "▰" * filled + "▱" * (5 - filled)
    if style == "fraction":
        return f"{b} {min(cur, target)}/{target}"
    return f"{b} {pct}%"


def _reward_text(rewards: list) -> str:
    """Человекочитаемая награда."""
    parts = []
    for rw in rewards:
        t = rw.get("type")
        if t in ("mushrooms", "coins", "shimcoins", "revive", "max", "partials"):
            names = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠",
                     "revive": "revive", "max": "max", "partials": "partials"}
            parts.append(f"{rw.get('amount', 0)} {names.get(t, t)}")
        elif t == "title":
            parts.append(f"🏅 титул «{rw.get('title_name', '?')}»")
        elif t == "emoji":
            parts.append(f"😎 {rw.get('emoji', '')}")
        elif t == "luck":
            parts.append(f"🍀 удача ×{rw.get('mult', 2):g} на {rw.get('minutes', 15)} мин")
        elif t == "discount":
            parts.append(f"🏷 скидка {rw.get('percent', 10)}%")
    return ", ".join(parts) if parts else "—"


async def show_achievements(bot, chat_id: int, uid: int, page: int = 0, edit_msg_id: int | None = None):
    import json
    items = await ach.list_for_user(uid)
    total = len(items)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = items[page * PER_PAGE:(page + 1) * PER_PAGE]

    done = sum(1 for i in items if i["completed"])
    header = f"🏆 <b>Достижения</b> ({done}/{total})\n"

    rows = []   # для rich: (текст, [(label, cb)])
    for a in chunk:
        rewards = a["rewards"] if isinstance(a["rewards"], list) else json.loads(a["rewards"])
        mark = "✅" if a["completed"] else ("🔒" if a["hidden"] else "▫️")
        title = a["title"]
        # скрытые невыполненные — прячем условия
        if a["hidden"] and not a["completed"]:
            desc = "🔒 <i>Скрытое достижение</i>"
        else:
            desc = a["description"] or ""
        bar = _bar(a["progress"], a["trigger_target"], a["progress_style"])
        reward = _reward_text(rewards)
        block = (f"{mark} <b>{title}</b>\n{desc}\n{bar}\n🎁 {reward}")
        # рич-кнопка «Получить» только для выполненных и не собранных
        if a["completed"] and not a["claimed"]:
            rows.append((block, [("🎁 Получить", f"ach_claim:{a['id']}")]))
        else:
            claimed_note = " ✔️ собрано" if a["claimed"] else ""
            rows.append((block + claimed_note, []))

    # управляющие кнопки (страницы)
    ctrl = InlineKeyboardBuilder()
    from services.ui import btn
    if pages > 1:
        if page > 0:
            await btn(ctrl, "◀️", f"ach_page:{page - 1}")
        if page < pages - 1:
            await btn(ctrl, "▶️", f"ach_page:{page + 1}")
    await btn(ctrl, "Назад", "profile", "back")
    ctrl.adjust(2, 1)
    fallback = ctrl.as_markup()
    if total > PER_PAGE:
        header += f"<i>Стр. {page + 1}/{pages}</i>\n"

    if edit_msg_id:
        await richmsg.edit_rich(bot, chat_id, edit_msg_id, header, rows,
                                fallback_kb=fallback, reply_markup=fallback)
    else:
        await richmsg.send_rich(bot, chat_id, header, rows,
                                fallback_kb=fallback, reply_markup=fallback)


@router.callback_query(F.data == "ach_open")
async def cb_open(c: CallbackQuery):
    with contextlib.suppress(Exception):
        await c.message.delete()
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, 0)
    await c.answer()


@router.callback_query(F.data.startswith("ach_page:"))
async def cb_page(c: CallbackQuery):
    page = int(c.data.split(":")[1])
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, page,
                            edit_msg_id=c.message.message_id)
    await c.answer()


@router.callback_query(F.data.startswith("ach_claim:"))
async def cb_claim(c: CallbackQuery):
    ach_id = int(c.data.split(":")[1])
    ok, err, given = await ach.claim(c.from_user.id, ach_id)
    if not ok:
        return await c.answer(err, show_alert=True)
    reward_str = ", ".join(given) if given else "награда"
    await c.answer(f"🎁 Получено: {reward_str}", show_alert=True)
    # перерисовать список (кнопка «Получить» исчезнет)
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, 0,
                            edit_msg_id=c.message.message_id)
