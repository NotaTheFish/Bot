"""
Админка достижений: пошаговое создание. Список триггеров, порог, награды, скрытость.
Награды вводятся строкой в простом формате (см. подсказку).
"""
import json
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, counters
from services.ui import btn
from config import SUPER_ADMINS

router = Router()

TRIG_PER_PAGE = 8


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class AchNew(StatesGroup):
    title = State()
    desc = State()
    trigger = State()
    target = State()
    rewards = State()


@router.message(F.text.lower() == "!достижения")
async def cmd_ach_admin(msg: Message):
    if not await _is_admin(msg.from_user.id):
        return
    total = await db.pool().fetchval("SELECT count(*) FROM rb_achievements")
    active = await db.pool().fetchval("SELECT count(*) FROM rb_achievements WHERE active")
    kb = InlineKeyboardBuilder()
    await btn(kb, "➕ Создать достижение", "acha_new")
    await btn(kb, "📋 Список достижений", "acha_list:0")
    kb.adjust(1)
    await ui.reply(msg,
        f"🏆 <b>Управление достижениями</b>\n\n"
        f"Всего: <b>{total}</b> · активных: <b>{active}</b>",
        reply_markup=kb.as_markup())


@router.callback_query(F.data == "acha_new")
async def cb_new(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    await state.set_state(AchNew.title)
    await state.update_data(ach={})
    await ui.edit(c.message, "🏆 <b>Новое достижение</b>\n\nНазвание (например «Звезда»):",
                  reply_markup=None)
    await c.answer()


@router.message(AchNew.title)
async def s_title(msg: Message, state: FSMContext):
    t = (msg.text or "").strip()
    if len(t) < 2 or len(t) > 60 or "<" in t or ">" in t:
        return await ui.reply(msg, "Название 2-60 символов, без скобок. Ещё раз:")
    d = await state.get_data(); d["ach"]["title"] = t
    await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.desc)
    await ui.reply(msg, "Описание (что нужно сделать). Или «-» чтобы пропустить:")


@router.message(AchNew.desc)
async def s_desc(msg: Message, state: FSMContext):
    desc = (msg.text or "").strip()
    if desc == "-":
        desc = ""
    if "<" in desc or ">" in desc:
        return await ui.reply(msg, "Без скобок. Ещё раз:")
    d = await state.get_data(); d["ach"]["desc"] = desc
    await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.trigger)
    await _show_triggers(msg, 0)


async def _show_triggers(msg_or_c, page: int):
    keys = list(counters.TRIGGER_LABELS.keys())
    pages = (len(keys) + TRIG_PER_PAGE - 1) // TRIG_PER_PAGE
    page = max(0, min(page, pages - 1))
    chunk = keys[page * TRIG_PER_PAGE:(page + 1) * TRIG_PER_PAGE]
    kb = InlineKeyboardBuilder()
    for k in chunk:
        await btn(kb, counters.TRIGGER_LABELS[k], f"acha_trig:{k}")
    nav = []
    if page > 0:
        await btn(kb, "◀️", f"acha_trigpg:{page-1}"); nav.append(1)
    if page < pages - 1:
        await btn(kb, "▶️", f"acha_trigpg:{page+1}"); nav.append(1)
    kb.adjust(*([1]*len(chunk) + ([len(nav)] if nav else [])))
    text = "Выбери <b>условие</b> (что отслеживать):"
    if hasattr(msg_or_c, "message"):  # callback
        await ui.edit(msg_or_c.message, text, reply_markup=kb.as_markup())
    else:
        await ui.reply(msg_or_c, text, reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("acha_trigpg:"))
async def cb_trigpg(c: CallbackQuery):
    await _show_triggers(c, int(c.data.split(":")[1]))
    await c.answer()


@router.callback_query(F.data.startswith("acha_trig:"))
async def cb_trig(c: CallbackQuery, state: FSMContext):
    trig = c.data.split(":", 1)[1]
    d = await state.get_data(); d["ach"]["trigger"] = trig
    await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.target)
    label = counters.TRIGGER_LABELS.get(trig, trig)
    await ui.edit(c.message,
        f"Условие: <b>{label}</b>\n\nВведи <b>порог</b> (число, которого нужно достичь):",
        reply_markup=None)
    await c.answer()


@router.message(AchNew.target)
async def s_target(msg: Message, state: FSMContext):
    from services.amount_parse import parse_amount
    n = parse_amount(msg.text or "")
    if n is None or n <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    d = await state.get_data(); d["ach"]["target"] = n
    await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.rewards)
    await ui.reply(msg,
        "🎁 <b>Награды</b> — по одной в строке. Форматы:\n\n"
        "<code>грибы 5000</code>\n<code>коины 100000</code>\n<code>шимкоины 5</code>\n"
        "<code>титул Звезда</code>\n<code>эмодзи ⭐</code>\n"
        "<code>удача 2 15 all</code> (×2 на 15 мин, scope all/roulette/cases/shine/giveaway/contest)\n"
        "<code>скидка 20 shop</code> (20% на shop/bank/all)\n"
        "<code>revive 3</code> / <code>max 1</code> / <code>partials 10</code>\n\n"
        "Напиши награды (можно несколько строк):")


def _parse_rewards(text: str) -> tuple[list, str]:
    """Разобрать награды из текста. Возвращает (список, ошибка)."""
    from services.amount_parse import shk_parse, parse_amount
    rewards = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        kind = parts[0].lower()
        try:
            if kind in ("грибы", "гриб"):
                rewards.append({"type": "mushrooms", "amount": parse_amount(parts[1])})
            elif kind in ("коины", "коин"):
                rewards.append({"type": "coins", "amount": parse_amount(parts[1])})
            elif kind in ("шимкоины", "шимкоин", "шк"):
                rewards.append({"type": "shimcoins", "amount": shk_parse(parts[1])})
            elif kind in ("revive", "max", "partials"):
                rewards.append({"type": kind, "amount": parse_amount(parts[1])})
            elif kind == "титул":
                rewards.append({"type": "title", "title_name": " ".join(parts[1:])})
            elif kind in ("эмодзи", "эмодзи"):
                rewards.append({"type": "emoji", "emoji": parts[1]})
            elif kind == "удача":
                rewards.append({"type": "luck", "mult": float(parts[1]),
                                "minutes": int(parts[2]), "scope": parts[3] if len(parts) > 3 else "all"})
            elif kind == "скидка":
                rewards.append({"type": "discount", "percent": int(parts[1]),
                                "target": parts[2] if len(parts) > 2 else "shop"})
            else:
                return [], f"Не понял строку: {line}"
        except (IndexError, ValueError):
            return [], f"Ошибка в строке: {line}"
    if not rewards:
        return [], "Нужна хотя бы одна награда."
    return rewards, ""


@router.message(AchNew.rewards)
async def s_rewards(msg: Message, state: FSMContext):
    rewards, err = _parse_rewards(msg.text or "")
    if err:
        return await ui.reply(msg, f"⚠️ {err}\nПопробуй ещё раз:")
    d = await state.get_data(); d["ach"]["rewards"] = rewards
    await state.update_data(ach=d["ach"])
    # выбор скрытое/публичное
    kb = InlineKeyboardBuilder()
    await btn(kb, "👁 Публичное (условия видны)", "acha_save:0")
    await btn(kb, "🔒 Скрытое (условия спрятаны)", "acha_save:1")
    kb.adjust(1)
    await ui.reply(msg, "Тип достижения:", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("acha_save:"))
async def cb_save(c: CallbackQuery, state: FSMContext):
    hidden = c.data.split(":")[1] == "1"
    d = await state.get_data()
    a = d.get("ach", {})
    await state.set_state(None)
    if not a.get("title") or not a.get("trigger"):
        return await c.answer("Данные потеряны, начни заново.", show_alert=True)
    import time
    code = f"ach_{int(time.time())}"
    await db.pool().execute(
        "INSERT INTO rb_achievements (code, title, description, hidden, trigger_type, "
        "trigger_target, progress_style, rewards, created_by) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        code, a["title"], a.get("desc", ""), hidden, a["trigger"], a["target"],
        "fraction" if a["target"] <= 100 else "percent",
        json.dumps(a["rewards"], ensure_ascii=False), c.from_user.id)
    await c.answer("Достижение создано!")
    with contextlib.suppress(Exception):
        await c.message.edit_text(
            f"✅ <b>Достижение создано</b>\n\n"
            f"«{a['title']}» — {counters.TRIGGER_LABELS.get(a['trigger'])} ≥ {a['target']}\n"
            f"{'🔒 скрытое' if hidden else '👁 публичное'}", reply_markup=None)


# ---------------- список/удаление ----------------
@router.callback_query(F.data.startswith("acha_list:"))
async def cb_list(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    page = int(c.data.split(":")[1])
    rows = await db.pool().fetch(
        "SELECT id, title, trigger_type, trigger_target, active, hidden "
        "FROM rb_achievements ORDER BY id DESC LIMIT 10 OFFSET $1", page * 10)
    kb = InlineKeyboardBuilder()
    lines = ["🏆 <b>Достижения</b>\n"]
    for r in rows:
        st = "" if r["active"] else " ⛔"
        h = "🔒" if r["hidden"] else "👁"
        lines.append(f"{h} <b>{r['title']}</b> ({counters.TRIGGER_LABELS.get(r['trigger_type'],'?')} ≥ {r['trigger_target']}){st}")
        await btn(kb, f"❌ {r['title'][:20]}", f"acha_del:{r['id']}")
    if not rows:
        lines.append("<i>Пусто.</i>")
    await btn(kb, "Назад", "profile", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("acha_del:"))
async def cb_del(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    aid = int(c.data.split(":")[1])
    await db.pool().execute("UPDATE rb_achievements SET active=false WHERE id=$1", aid)
    await c.answer("Достижение отключено.")
    c.data = "acha_list:0"
    await cb_list(c)
