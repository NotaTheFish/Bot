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
    claim_text = State()
    secret = State()


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
    label = counters.TRIGGER_LABELS.get(trig, trig)
    if trig == "secret_word":
        # особый режим — вводим само слово, порог не нужен
        await state.set_state(AchNew.secret)
        await ui.edit(c.message,
            "🔑 <b>Секретное слово</b>\n\nВведи слово-триггер (игрок напишет его боту в ЛС, "
            "чтобы открыть достижение):", reply_markup=None)
    else:
        await state.set_state(AchNew.target)
        await ui.edit(c.message,
            f"Условие: <b>{label}</b>\n\nВведи <b>порог</b> (число, которого нужно достичь):",
            reply_markup=None)
    await c.answer()


@router.message(AchNew.secret)
async def s_secret(msg: Message, state: FSMContext):
    word = (msg.text or "").strip()
    if len(word) < 2 or len(word) > 60:
        return await ui.reply(msg, "Слово 2-60 символов. Ещё раз:")
    if "<" in word or ">" in word:
        return await ui.reply(msg, "Без скобок. Ещё раз:")
    # проверим уникальность слова
    import re
    norm = re.sub(r"\s+", " ", word).lower()
    exists = await db.pool().fetchval(
        "SELECT 1 FROM rb_achievements WHERE active AND lower(secret_word)=$1", norm)
    if exists:
        return await ui.reply(msg, "Это слово уже используется другим достижением. Другое:")
    d = await state.get_data(); d["ach"]["secret_word"] = word
    d["ach"]["target"] = 1   # секретное = порог 1 (одноразово)
    await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.rewards)
    await ui.reply(msg,
        "🎁 <b>Награды</b> — по одной в строке. Форматы:\n\n"
        "<code>грибы 5000</code>\n<code>титул Звезда</code>\n<code>эмодзи ⭐</code>\n"
        "<code>удача 2 15 all</code>\n<code>скидка 20 shop</code>\n\n"
        "Напиши награды:")


@router.message(AchNew.target)
async def s_target(msg: Message, state: FSMContext):
    from services.amount_parse import parse_amount
    n = parse_amount(msg.text or "")
    if n is None or n <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    d = await state.get_data(); d["ach"]["target"] = n
    await state.update_data(ach=d["ach"])
    # если у триггера есть «подряд»-версия — предложить выбор режима
    trig = d["ach"]["trigger"]
    if trig in counters.STREAK_VARIANTS:
        kb = InlineKeyboardBuilder()
        await btn(kb, "📊 За всё время", "acha_mode:total")
        await btn(kb, "🔥 Подряд", "acha_mode:streak")
        kb.adjust(1)
        return await ui.reply(msg,
            f"Как считать «{counters.TRIGGER_LABELS.get(trig)}» — за всё время или подряд?",
            reply_markup=kb.as_markup())
    await state.set_state(AchNew.rewards)
    await _ask_rewards(msg)


@router.callback_query(F.data.startswith("acha_mode:"))
async def cb_mode(c: CallbackQuery, state: FSMContext):
    mode = c.data.split(":")[1]
    d = await state.get_data()
    if mode == "streak":
        # заменить триггер на стрик-версию
        trig = d["ach"]["trigger"]
        d["ach"]["trigger"] = counters.STREAK_VARIANTS.get(trig, trig)
        await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.rewards)
    await c.answer("Ок")
    await _ask_rewards(c.message)


async def _ask_rewards(msg):
    await ui.reply(msg,
        "🎁 <b>Награды</b> — по одной в строке. Форматы:\n\n"
        "<code>грибы 5000</code>\n<code>коины 100000</code>\n<code>шимкоины 5</code>\n"
        "<code>титул Звезда</code>\n<code>эмодзи ⭐</code>\n"
        "<code>удача 2 15 all</code>\n<code>скидка 20 shop</code>\n"
        "<code>щит время 60</code> / <code>щит разы 3</code>\n"
        "<code>revive 3</code> / <code>max 1</code> / <code>partials 10</code>\n\n"
        "Напиши награды (можно несколько строк):")


def _parse_rewards(text: str, premium_map: dict = None) -> tuple[list, str]:
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
            elif kind == "эмодзи":
                sym = parts[1]
                # если это премиум (есть в карте entities) — сохраняем как premium-тег
                emoji_val = (premium_map or {}).get(sym, sym)
                rewards.append({"type": "emoji", "emoji": emoji_val})
            elif kind == "удача":
                rewards.append({"type": "luck", "mult": float(parts[1]),
                                "minutes": int(parts[2]), "scope": parts[3] if len(parts) > 3 else "all"})
            elif kind == "скидка":
                rewards.append({"type": "discount", "percent": int(parts[1]),
                                "target": parts[2] if len(parts) > 2 else "shop"})
            elif kind in ("щит", "шимщит"):
                # щит время N  /  щит разы N
                sub = parts[1].lower()
                val = int(parts[2])
                if sub in ("время", "time"):
                    rewards.append({"type": "shield", "kind": "time", "minutes": val})
                else:
                    rewards.append({"type": "shield", "kind": "uses", "uses": val})
            else:
                return [], f"Не понял строку: {line}"
        except (IndexError, ValueError):
            return [], f"Ошибка в строке: {line}"
    if not rewards:
        return [], "Нужна хотя бы одна награда."
    return rewards, ""


@router.message(AchNew.rewards)
async def s_rewards(msg: Message, state: FSMContext):
    # карта премиум-эмодзи из entities: символ -> <tg-emoji> тег.
    # entity offset/length — в UTF-16 единицах, поэтому режем по utf-16.
    pmap = {}
    raw = msg.text or ""
    u16 = raw.encode("utf-16-le")
    for e in (msg.entities or []):
        if e.type == "custom_emoji":
            sym = u16[e.offset * 2:(e.offset + e.length) * 2].decode("utf-16-le")
            pmap[sym] = f'<tg-emoji emoji-id="{e.custom_emoji_id}">{sym}</tg-emoji>'
    rewards, err = _parse_rewards(raw, pmap)
    if err:
        return await ui.reply(msg, f"⚠️ {err}\nПопробуй ещё раз:")
    d = await state.get_data(); d["ach"]["rewards"] = rewards
    await state.update_data(ach=d["ach"])
    await state.set_state(AchNew.claim_text)
    await ui.reply(msg,
        "💬 Подпись при получении награды (например «ласт деп?»).\n"
        "Или «-» чтобы без подписи:")


@router.message(AchNew.claim_text)
async def s_claim_text(msg: Message, state: FSMContext):
    note = (msg.text or "").strip()
    if note == "-":
        note = ""
    if len(note) > 100 or "<" in note or ">" in note:
        return await ui.reply(msg, "До 100 символов, без скобок. Ещё раз:")
    d = await state.get_data(); d["ach"]["claim_text"] = note
    await state.update_data(ach=d["ach"])
    await state.set_state(None)
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
        "trigger_target, progress_style, rewards, created_by, claim_text, secret_word) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        code, a["title"], a.get("desc", ""),
        hidden or bool(a.get("secret_word")),  # секретное слово -> всегда скрытое
        a["trigger"], a["target"],
        "fraction" if a["target"] <= 100 else "percent",
        json.dumps(a["rewards"], ensure_ascii=False), c.from_user.id,
        a.get("claim_text") or None, a.get("secret_word") or None)
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
        if r["active"]:
            await btn(kb, f"⛔ Откл: {r['title'][:16]}", f"acha_del:{r['id']}")
        await btn(kb, f"🗑 Удалить: {r['title'][:16]}", f"acha_kill:{r['id']}")
    if not rows:
        lines.append("<i>Пусто.</i>")
    await btn(kb, "Назад", "profile", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("acha_del:"))
async def cb_del(c: CallbackQuery):
    """Отключить достижение (скрыть, но данные сохранить)."""
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    aid = int(c.data.split(":")[1])
    await db.pool().execute("UPDATE rb_achievements SET active=false WHERE id=$1", aid)
    await c.answer("Достижение отключено.")
    c.data = "acha_list:0"
    await cb_list(c)


@router.callback_query(F.data.startswith("acha_kill:"))
async def cb_kill(c: CallbackQuery):
    """Удалить достижение НАСОВСЕМ (с подтверждением)."""
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    parts = c.data.split(":")
    aid = int(parts[1])
    confirmed = len(parts) > 2 and parts[2] == "yes"
    a = await db.pool().fetchrow("SELECT title FROM rb_achievements WHERE id=$1", aid)
    if not a:
        return await c.answer("Уже удалено.", show_alert=True)
    if not confirmed:
        kb = InlineKeyboardBuilder()
        await btn(kb, "🗑 Да, удалить насовсем", f"acha_kill:{aid}:yes")
        await btn(kb, "Отмена", "acha_list:0", "back")
        kb.adjust(1)
        return await ui.edit(c.message,
            f"🗑 <b>Удалить достижение?</b>\n\n«{a['title']}»\n\n"
            f"Удалится полностью вместе с прогрессом всех игроков. "
            f"Это <b>необратимо</b>.", reply_markup=kb.as_markup())
    # удаляем прогресс + само достижение
    await db.pool().execute("DELETE FROM rb_user_achievements WHERE ach_id=$1", aid)
    await db.pool().execute("DELETE FROM rb_achievements WHERE id=$1", aid)
    await c.answer("Достижение удалено насовсем.")
    c.data = "acha_list:0"
    await cb_list(c)
