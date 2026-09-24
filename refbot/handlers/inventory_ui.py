"""
Инвентарь игрока: активация удачи/скидки. Показ активных бонусов.
"""
import contextlib
from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

from services import ui, inventory as inv
from services.ui import btn
from services.amount_parse import fmt_mult

router = Router()

_SCOPE_NAMES = {"all": "всё", "roulette": "рулетка", "cases": "кейсы", "shine": "!шайн",
                "giveaway": "розыгрыши", "contest": "конкурс", "shop": "магазин", "bank": "банк"}


def _fmt_left(expires) -> str:
    if not expires:
        return ""
    left = expires - datetime.now(timezone.utc)
    mins = int(left.total_seconds() // 60)
    if mins <= 0:
        return "истекает"
    if mins < 60:
        return f"{mins} мин"
    return f"{mins // 60} ч {mins % 60} мин"


async def _inv_content(uid: int):
    """Построить (текст, клавиатура) инвентаря."""
    items = await inv.inventory(uid)
    active = await inv.active_bonuses(uid)
    shields = await inv.active_shields(uid)
    kb = InlineKeyboardBuilder()
    lines = ["🎒 <b>Инвентарь</b>", ""]
    if active or shields:
        lines.append("⚡ <b>Активно сейчас:</b>")
        for a in active:
            if a["bonus_type"] == "luck":
                left = _fmt_left(a["expires_at"])
                lines.append(f"🍀 Удача ×{fmt_mult(a['multiplier'])} "
                             f"({_SCOPE_NAMES.get(a['scope'], a['scope'])}) — {left}")
            else:
                pct = a["payload"].get("percent", 0)
                lines.append(f"🏷 Скидка {pct}% ({_SCOPE_NAMES.get(a['scope'], a['scope'])})")
            await btn(kb, f"🚫 Отключить: {a['bonus_type']}", f"inv_off:{a['id']}")
        for sh in shields:
            if sh["kind"] == "time":
                lines.append(f"🛡 Щит — до {_fmt_left(sh['expires_at'])}")
            else:
                lines.append(f"🛡 Щит — осталось {sh['uses_left']} исп.")
            await btn(kb, "🚫 Снять щит", f"inv_shoff:{sh['id']}")
        lines.append("")
    if not items:
        lines.append("<i>Нет предметов. Купи в магазине или получи за достижения.</i>")
    else:
        lines.append("📦 <b>Доступно к активации:</b>")
        for it in items:
            p = it["payload"]; t = it["item_type"]
            if t == "luck":
                label = (f"🍀 Удача ×{fmt_mult(p.get('mult',2))} на {p.get('minutes',15)} мин "
                         f"({_SCOPE_NAMES.get(p.get('scope','all'), p.get('scope','all'))})")
            elif t == "discount":
                label = f"🏷 Скидка {p.get('percent',10)}% ({_SCOPE_NAMES.get(p.get('target','shop'))})"
            elif t == "shield":
                if p.get("kind") == "time":
                    label = f"🛡 Щит на {p.get('minutes',60)} мин"
                else:
                    label = f"🛡 Щит ×{p.get('uses',1)}"
            else:
                label = t
            lines.append(f"• {label}")
            await btn(kb, f"Активировать: {label[:26]}", f"inv_use:{it['id']}")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    return "\n".join(lines), kb.as_markup()


async def _refresh(c: CallbackQuery):
    """Обновить инвентарь надёжно: edit, а если не вышло — удалить+послать заново."""
    text, markup = await _inv_content(c.from_user.id)
    try:
        await ui.edit(c.message, text, reply_markup=markup)
    except Exception:
        with contextlib.suppress(Exception):
            await c.message.delete()
        await ui.send(c.bot, c.message.chat.id, text, reply_markup=markup)


@router.callback_query(F.data == "inv_open")
async def cb_inv(c: CallbackQuery):
    text, markup = await _inv_content(c.from_user.id)
    await ui.edit(c.message, text, reply_markup=markup)
    await c.answer()


@router.callback_query(F.data.startswith("inv_off:"))
async def cb_disable(c: CallbackQuery):
    await inv.disable_bonus(c.from_user.id, int(c.data.split(":")[1]))
    await c.answer("Бонус отключён.")
    await _refresh(c)


@router.callback_query(F.data.startswith("inv_shoff:"))
async def cb_shield_off(c: CallbackQuery):
    await inv.disable_shield(c.from_user.id, int(c.data.split(":")[1]))
    await c.answer("Щит снят.")
    await _refresh(c)


@router.callback_query(F.data.startswith("inv_use:"))
async def cb_use(c: CallbackQuery):
    inv_id = int(c.data.split(":")[1])
    ok, err = await inv.activate(c.from_user.id, inv_id)
    if not ok:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("✅ Бонус активирован!", show_alert=True)
    await _refresh(c)
