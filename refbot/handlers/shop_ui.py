"""
Магазин для игрока: витрина, покупка. Кнопка в главном меню/казино.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, shop, settings
from services.ui import btn
from services.amount_parse import shk_fmt

router = Router()

_CUR_E = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}
_TYPE_E = {"luck": "🍀", "discount": "🏷", "title": "🏅", "emoji": "😎"}


def _price_txt(item) -> str:
    if item["currency"] == "shimcoins":
        return f"{shk_fmt(item['price'])} 💠"
    return f"{item['price']:,}".replace(",", " ") + f" {_CUR_E.get(item['currency'],'')}"


def _payload_desc(item) -> str:
    p = item["payload"]; t = item["item_type"]
    if t == "luck":
        return f"×{p.get('mult',2):g} удача на {p.get('minutes',15)} мин ({p.get('scope','all')})"
    if t == "discount":
        return f"скидка {p.get('percent',10)}% на {p.get('target','shop')}"
    if t == "title":
        return f"титул «{p.get('title_name','?')}»"
    if t == "emoji":
        return f"эмодзи {p.get('emoji','')}"
    return ""


@router.callback_query(F.data == "shop_open")
async def cb_shop(c: CallbackQuery):
    items = await shop.list_items(active_only=True)
    b = await db.balances(c.from_user.id)
    kb = InlineKeyboardBuilder()
    lines = [f"🛒 <b>Магазин</b>\n"
             f"Баланс: 🍄 {b['mushrooms']:,} · 🪙 {b['coins']:,} · 💠 {shk_fmt(b['shimcoins'])}".replace(",", " "),
             ""]
    if not items:
        lines.append("<i>Пока пусто. Загляни позже!</i>")
    for it in items:
        e = _TYPE_E.get(it["item_type"], "•")
        desc = it["description"] or _payload_desc(it)
        lines.append(f"{e} <b>{it['name']}</b> — {_price_txt(it)}\n   <i>{desc}</i>")
        await btn(kb, f"Купить: {it['name'][:22]} ({_price_txt(it)})", f"shop_buy:{it['id']}")
    await btn(kb, "🎒 Инвентарь", "inv_open")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("shop_buy:"))
async def cb_buy(c: CallbackQuery):
    item_id = int(c.data.split(":")[1])
    ok, err, item = await shop.buy(c.from_user.id, item_id)
    if not ok:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    # что выдали
    t = item["item_type"]
    if t in ("luck", "discount"):
        note = "Добавлено в инвентарь — активируй, когда нужно."
    elif t == "title":
        note = "Титул выдан — выбери его в профиле."
    else:
        note = "Эмодзи выдан — выбери его в профиле."
    await c.answer(f"✅ Куплено: {item['name']}\n{note}", show_alert=True)
    # обновить витрину
    c.data = "shop_open"
    await cb_shop(c)
