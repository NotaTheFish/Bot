"""
Начисление бонусов админом через карточку пользователя (кнопка 🎁 Бонус).
Удача / скидка / щит — с параметрами, выдаются напрямую в инвентарь игрока.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, shop, profile
from services.ui import btn
from config import SUPER_ADMINS

router = Router()


async def _can(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class BonusGrant(StatesGroup):
    params = State()


def _parse_minutes(text: str) -> int:
    import re
    t = text.strip().lower().replace(" ", "")
    m = re.match(r"^(\d+)\s*([дdчhмm]?)", t)
    if not m:
        return int(re.sub(r"\D", "", t) or 0)
    n = int(m.group(1)); suf = m.group(2)
    if suf in ("д", "d"): return n * 1440
    if suf in ("ч", "h"): return n * 60
    return n


@router.callback_query(F.data.startswith("a_bonus:"))
async def cb_bonus(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍀 Удача", f"a_bon_t:luck:{tg_id}")
    await btn(kb, "🏷 Скидка", f"a_bon_t:discount:{tg_id}")
    await btn(kb, "🛡 Щит", f"a_bon_t:shield:{tg_id}")
    await btn(kb, "Назад", f"a_findback:{tg_id}", "back")
    kb.adjust(2, 1, 1)
    await ui.edit(c.message, "🎁 <b>Начислить бонус</b>\n\nЧто выдать?",
                  reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("a_bon_t:"))
async def cb_bonus_type(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    _, t, tg_s = c.data.split(":")
    tg_id = int(tg_s)
    await state.set_state(BonusGrant.params)
    await state.update_data(bonus={"type": t, "tg_id": tg_id})
    hint = {
        "luck": "Удача: <code>множитель минуты область</code>\n"
                "Пример: <code>2 15 all</code>. Область: all/roulette/cases/shine/giveaway/contest",
        "discount": "Скидка: <code>процент цель</code>\nПример: <code>20 shop</code>. Цель: shop/bank/all",
        "shield": "Щит: <code>время 100д</code> (или 5ч/30) либо <code>разы 3</code>",
    }[t]
    await ui.edit(c.message, f"⚙️ {hint}\n\nВведи параметры:", reply_markup=None)
    await c.answer()


@router.message(BonusGrant.params)
async def msg_params(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    data = await state.get_data()
    b = data.get("bonus", {})
    t = b.get("type"); tg_id = b.get("tg_id")
    parts = (msg.text or "").split()
    payload = {}
    try:
        if t == "luck":
            payload = {"mult": float(parts[0]), "minutes": int(parts[1]),
                       "scope": parts[2] if len(parts) > 2 else "all"}
        elif t == "discount":
            payload = {"percent": int(parts[0]), "target": parts[1] if len(parts) > 1 else "shop"}
        elif t == "shield":
            kw = parts[0].lower()
            if kw in ("время", "time"):
                payload = {"kind": "time", "minutes": _parse_minutes(parts[1])}
            else:
                payload = {"kind": "uses", "uses": int(parts[1])}
    except (IndexError, ValueError):
        return await ui.reply(msg, "Не понял параметры. Ещё раз по формату:")
    await state.set_state(None)
    desc = await shop.grant_bonus(tg_id, t, payload)
    pname = await profile.display_name(tg_id, link=False)
    kb = InlineKeyboardBuilder()
    await btn(kb, "К пользователю", f"a_findback:{tg_id}", "back")
    await ui.reply(msg, f"✅ Выдано {pname}: {desc}", reply_markup=kb.as_markup())
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, tg_id, f"🎁 Тебе начислили бонус: {desc}\nЗабери в 🎒 Инвентаре.")
