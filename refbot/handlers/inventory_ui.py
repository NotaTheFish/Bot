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


@router.callback_query(F.data == "inv_open")
async def cb_inv(c: CallbackQuery):
    items = await inv.inventory(c.from_user.id)
    active = await inv.active_bonuses(c.from_user.id)
    kb = InlineKeyboardBuilder()
    lines = ["🎒 <b>Инвентарь</b>", ""]

    # активные бонусы
    if active:
        lines.append("⚡ <b>Активно сейчас:</b>")
        for a in active:
            if a["bonus_type"] == "luck":
                left = _fmt_left(a["expires_at"])
                lines.append(f"🍀 Удача ×{float(a['multiplier']):g} "
                             f"({_SCOPE_NAMES.get(a['scope'], a['scope'])}) — {left}")
            else:
                pct = a["payload"].get("percent", 0)
                lines.append(f"🏷 Скидка {pct}% ({_SCOPE_NAMES.get(a['scope'], a['scope'])})")
        lines.append("")

    # предметы для активации
    if not items:
        lines.append("<i>Пусто. Купи бонусы в магазине или получи за достижения.</i>")
    else:
        lines.append("📦 <b>Доступно к активации:</b>")
        for it in items:
            p = it["payload"]
            if it["item_type"] == "luck":
                label = (f"🍀 Удача ×{p.get('mult',2):g} на {p.get('minutes',15)} мин "
                         f"({_SCOPE_NAMES.get(p.get('scope','all'), p.get('scope','all'))})")
            else:
                label = f"🏷 Скидка {p.get('percent',10)}% ({_SCOPE_NAMES.get(p.get('target','shop'))})"
            lines.append(f"• {label}")
            await btn(kb, f"Активировать: {label[:28]}", f"inv_use:{it['id']}")

    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("inv_use:"))
async def cb_use(c: CallbackQuery):
    inv_id = int(c.data.split(":")[1])
    ok, err = await inv.activate(c.from_user.id, inv_id)
    if not ok:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("✅ Бонус активирован!", show_alert=True)
    c.data = "inv_open"
    await cb_inv(c)
