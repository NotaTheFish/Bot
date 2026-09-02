"""
Управление магазином (админ): добавить/убрать товары, задать цены.
"""
import json
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, shop
from services.ui import btn
from services.amount_parse import shk_fmt
from config import SUPER_ADMINS


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


router = Router()

_CUR_NAMES = {"mushrooms": "🍄 грибы", "coins": "🪙 коины", "shimcoins": "💠 шимкоины"}


class ShopNew(StatesGroup):
    name = State()
    desc = State()
    payload = State()
    price = State()


@router.message(F.text.lower() == "!магазин")
async def cmd_shop_admin(msg: Message):
    if not await _is_admin(msg.from_user.id):
        return
    items = await shop.list_items(active_only=False)
    active = sum(1 for i in items if i["active"])
    kb = InlineKeyboardBuilder()
    await btn(kb, "➕ Добавить товар", "shopa_new")
    await btn(kb, "📋 Список товаров", "shopa_list")
    kb.adjust(1)
    await ui.reply(msg,
        f"🛒 <b>Управление магазином</b>\n\nАктивных товаров: <b>{active}</b>",
        reply_markup=kb.as_markup())


@router.callback_query(F.data == "shopa_new")
async def cb_new(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍀 Удача", "shopa_type:luck")
    await btn(kb, "🏷 Скидка", "shopa_type:discount")
    await btn(kb, "🏅 Титул", "shopa_type:title")
    await btn(kb, "😎 Эмодзи", "shopa_type:emoji")
    kb.adjust(2)
    await state.update_data(shop={})
    await ui.edit(c.message, "🛒 <b>Новый товар</b>\n\nЧто продаём?", reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("shopa_type:"))
async def cb_type(c: CallbackQuery, state: FSMContext):
    t = c.data.split(":")[1]
    d = await state.get_data(); s = d.get("shop", {}); s["type"] = t
    await state.update_data(shop=s)
    await state.set_state(ShopNew.name)
    await ui.edit(c.message, "Название товара:", reply_markup=None)
    await c.answer()


@router.message(ShopNew.name)
async def s_name(msg: Message, state: FSMContext):
    t = (msg.text or "").strip()
    if len(t) < 2 or len(t) > 60 or "<" in t or ">" in t:
        return await ui.reply(msg, "Название 2-60 символов, без скобок. Ещё раз:")
    d = await state.get_data(); s = d["shop"]; s["name"] = t
    await state.update_data(shop=s)
    await state.set_state(ShopNew.desc)
    await ui.reply(msg, "Описание (или «-» чтобы пропустить):")


@router.message(ShopNew.desc)
async def s_desc(msg: Message, state: FSMContext):
    desc = (msg.text or "").strip()
    if desc == "-":
        desc = ""
    d = await state.get_data(); s = d["shop"]; s["desc"] = desc
    await state.update_data(shop=s)
    await state.set_state(ShopNew.payload)
    t = s["type"]
    hint = {
        "luck": "Параметры удачи: <code>множитель минуты область</code>\n"
                "Пример: <code>2 15 all</code> (×2 на 15 мин).\n"
                "Область: all/roulette/cases/shine/giveaway/contest",
        "discount": "Параметры скидки: <code>процент цель</code>\n"
                    "Пример: <code>20 shop</code> (20% на магазин).\nЦель: shop/bank/all",
        "title": "Название титула, который получит покупатель:",
        "emoji": "Эмодзи, который получит покупатель:",
    }[t]
    await ui.reply(msg, f"⚙️ {hint}")


@router.message(ShopNew.payload)
async def s_payload(msg: Message, state: FSMContext):
    d = await state.get_data(); s = d["shop"]; t = s["type"]
    txt = (msg.text or "").strip()
    payload = {}
    try:
        if t == "luck":
            parts = txt.split()
            payload = {"mult": float(parts[0]), "minutes": int(parts[1]),
                       "scope": parts[2] if len(parts) > 2 else "all"}
        elif t == "discount":
            parts = txt.split()
            payload = {"percent": int(parts[0]), "target": parts[1] if len(parts) > 1 else "shop"}
        elif t == "title":
            payload = {"title_name": txt}
        elif t == "emoji":
            payload = {"emoji": txt.split()[0]}
    except (IndexError, ValueError):
        return await ui.reply(msg, "Не понял параметры. Попробуй ещё раз по формату:")
    s["payload"] = payload
    await state.update_data(shop=s)
    await state.set_state(ShopNew.price)
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибы", "shopa_cur:mushrooms")
    await btn(kb, "🪙 Коины", "shopa_cur:coins")
    await btn(kb, "💠 Шимкоины", "shopa_cur:shimcoins")
    kb.adjust(3)
    await ui.reply(msg, "В какой валюте цена?", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("shopa_cur:"))
async def cb_cur(c: CallbackQuery, state: FSMContext):
    cur = c.data.split(":")[1]
    d = await state.get_data(); s = d["shop"]; s["currency"] = cur
    await state.update_data(shop=s)
    await ui.edit(c.message,
        f"Цена в {_CUR_NAMES[cur]} — введи число:" +
        ("\n<i>(шимкоины: 5 = 5.00)</i>" if cur == "shimcoins" else ""),
        reply_markup=None)
    await c.answer()


@router.message(ShopNew.price)
async def s_price(msg: Message, state: FSMContext):
    from services.amount_parse import parse_amount, shk_parse
    d = await state.get_data(); s = d["shop"]
    cur = s.get("currency")
    if not cur:
        return await ui.reply(msg, "Сначала выбери валюту кнопкой выше.")
    price = shk_parse(msg.text or "") if cur == "shimcoins" else parse_amount(msg.text or "")
    if price is None or price <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    await state.set_state(None)
    iid = await shop.add_item(s["name"], s.get("desc", ""), s["type"], price, cur,
                              s["payload"], None, msg.from_user.id)
    price_txt = shk_fmt(price) if cur == "shimcoins" else f"{price:,}".replace(",", " ")
    kb = InlineKeyboardBuilder()
    await btn(kb, "В магазин", "shopa_list", "back")
    await ui.reply(msg,
        f"✅ <b>Товар добавлен</b>\n\n«{s['name']}» — {price_txt} {_CUR_NAMES[cur]}",
        reply_markup=kb.as_markup())


@router.callback_query(F.data == "shopa_list")
async def cb_list(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    items = await shop.list_items(active_only=False)
    kb = InlineKeyboardBuilder()
    lines = ["🛒 <b>Товары магазина</b>\n"]
    for it in items:
        st = "" if it["active"] else " ⛔"
        price_txt = shk_fmt(it["price"]) if it["currency"] == "shimcoins" else f"{it['price']:,}".replace(",", " ")
        lines.append(f"• <b>{it['name']}</b> — {price_txt} {_CUR_NAMES.get(it['currency'],'')}{st}")
        if it["active"]:
            await btn(kb, f"❌ {it['name'][:20]}", f"shopa_del:{it['id']}")
    if not items:
        lines.append("<i>Пусто.</i>")
    await btn(kb, "Назад", "admin", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("shopa_del:"))
async def cb_del(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    await shop.remove_item(int(c.data.split(":")[1]))
    await c.answer("Товар убран из магазина.")
    c.data = "shopa_list"
    await cb_list(c)
