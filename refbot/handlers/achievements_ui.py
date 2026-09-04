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
            names = {"mushrooms": "🍄 грибов", "coins": "🪙 коинов", "shimcoins": "💠 шимкоинов",
                     "revive": "revive", "max": "max", "partials": "partials"}
            amt = rw.get("amount", 0)
            if t == "shimcoins":
                from services.amount_parse import shk_fmt
                parts.append(f"{shk_fmt(amt)} {names[t]}")
            else:
                parts.append(f"{amt:,}".replace(",", " ") + f" {names.get(t, t)}")
        elif t == "title":
            parts.append(f"🏅 титул «{rw.get('title_name', '?')}»")
        elif t == "emoji":
            parts.append(f"😎 эмодзи {rw.get('emoji', '')}")
        elif t == "luck":
            parts.append(f"🍀 удача ×{rw.get('mult', 2):g} на {rw.get('minutes', 15)} мин")
        elif t == "discount":
            parts.append(f"🏷 скидка {rw.get('percent', 10)}%")
    return " + ".join(parts) if parts else "—"


async def show_achievements(bot, chat_id: int, uid: int, page: int = 0,
                            edit_msg_id: int | None = None, tab: str = "public"):
    import json
    # обновить пиковые балансы (max_*) перед показом — вдруг набрал баланс до фикса
    from services import counters as _cnt
    await _cnt.sync_peak_balances(uid)
    items = await ach.list_for_user(uid)
    # фильтр по вкладке: public | hidden
    if tab == "hidden":
        items = [i for i in items if i["hidden"]]
    else:
        items = [i for i in items if not i["hidden"]]

    total = len(items)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = items[page * PER_PAGE:(page + 1) * PER_PAGE]

    done = sum(1 for i in items if i["completed"])
    tab_name = "Скрытые" if tab == "hidden" else "Открытые"
    header = f"🏆 <b>Достижения — {tab_name}</b>  ({done} из {total})\n"

    rows = []   # для rich: (текст, [(label, cb)])
    for a in chunk:
        rewards = a["rewards"] if isinstance(a["rewards"], list) else json.loads(a["rewards"])
        # статус: собрано / готово к получению / в процессе
        if a["claimed"]:
            status = "✅ <b>Получено</b>"
        elif a["completed"]:
            status = "🎉 <b>Выполнено — забери награду!</b>"
        else:
            status = ""
        title = a["title"]
        # условие
        if a["hidden"] and not a["completed"]:
            cond = "❔ <i>условие скрыто</i>"
        else:
            cond = a["description"] or "—"
        bar = _bar(a["progress"], a["trigger_target"], a["progress_style"])
        reward = _reward_text(rewards)

        lines = [f"<b>{title}</b>", cond, bar, f"Награда: {reward}"]
        if status:
            lines.append(status)
        block = "\n".join(lines)

        if a["completed"] and not a["claimed"]:
            rows.append((block, [("🎁 Забрать награду", f"ach_claim:{a['id']}")]))
        else:
            rows.append((block, []))

    if not chunk:
        rows.append(("<i>Здесь пока пусто.</i>", []))

    # управляющие кнопки: тумблер вкладок + страницы
    ctrl = InlineKeyboardBuilder()
    from services.ui import btn
    other = "public" if tab == "hidden" else "hidden"
    other_name = "🔓 Открытые" if tab == "hidden" else "🔒 Скрытые"
    await btn(ctrl, other_name, f"ach_tab:{other}")
    nav = 0
    if pages > 1:
        if page > 0:
            await btn(ctrl, "◀️", f"ach_page:{tab}:{page - 1}"); nav += 1
        if page < pages - 1:
            await btn(ctrl, "▶️", f"ach_page:{tab}:{page + 1}"); nav += 1
    await btn(ctrl, "Назад", "profile", "back")
    layout = [1] + ([nav] if nav else []) + [1]
    ctrl.adjust(*layout)
    fallback = ctrl.as_markup()
    if pages > 1:
        header += f"<i>Стр. {page + 1} из {pages}</i>\n"

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
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, 0, tab="public")
    await c.answer()


@router.callback_query(F.data.startswith("ach_tab:"))
async def cb_tab(c: CallbackQuery):
    tab = c.data.split(":")[1]
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, 0,
                            edit_msg_id=c.message.message_id, tab=tab)
    await c.answer()


@router.callback_query(F.data.startswith("ach_page:"))
async def cb_page(c: CallbackQuery):
    _, tab, page = c.data.split(":")
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, int(page),
                            edit_msg_id=c.message.message_id, tab=tab)
    await c.answer()


@router.callback_query(F.data.startswith("ach_claim:"))
async def cb_claim(c: CallbackQuery):
    ach_id = int(c.data.split(":")[1])
    ok, err, given = await ach.claim(c.from_user.id, ach_id)
    if not ok:
        return await c.answer(err, show_alert=True)
    reward_str = ", ".join(given) if given else "награда"
    await c.answer(f"🎁 Получено: {reward_str}", show_alert=True)
    # понять, на какой вкладке был игрок (по достижению)
    a = await ach.get_ach(ach_id)
    tab = "hidden" if (a and a["hidden"]) else "public"
    await show_achievements(c.bot, c.message.chat.id, c.from_user.id, 0,
                            edit_msg_id=c.message.message_id, tab=tab)
