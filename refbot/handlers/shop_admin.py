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

def _parse_minutes(text: str) -> int:
    """Длительность в минуты: '100д'/'100d'=дни, '5ч'/'5h'=часы, '30м'/'30'=минуты."""
    t = text.strip().lower().replace(" ", "")
    import re
    m = re.match(r"^(\d+)\s*([дdчhмm]?)", t)
    if not m:
        return int(re.sub(r"\D", "", t) or 0)
    n = int(m.group(1)); suf = m.group(2)
    if suf in ("д", "d"): return n * 1440
    if suf in ("ч", "h"): return n * 60
    return n   # минуты по умолчанию



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
    await btn(kb, "📋 Товары (удалить)", "shopa_list")
    await btn(kb, "💲 Цены смены ника/ID", "shopa_prices")
    kb.adjust(1)
    await ui.reply(msg,
        f"🛒 <b>Управление магазином</b>\n\nТоваров: <b>{len(items)}</b>",
        reply_markup=kb.as_markup())


@router.callback_query(F.data == "shopa_prices")
async def cb_prices(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    from services import settings as _s
    s = await _s.load()
    nick_p = int(s.get("nickname_price", 0) or 0)
    id_p = int(s.get("public_id_price", 0) or 0)
    kb = InlineKeyboardBuilder()
    await btn(kb, f"✏️ Ник: {shk_fmt(nick_p)} 💠", "shopa_setprice:nick")
    await btn(kb, f"🆔 ID: {shk_fmt(id_p)} 💠", "shopa_setprice:id")
    await btn(kb, "Назад", "admin", "back")
    kb.adjust(1)
    await ui.edit(c.message,
        "💲 <b>Цены повторной смены</b>\n\nПервая установка — бесплатно, "
        "последующие — за эту цену в шимкоинах. Нажми, чтобы изменить:",
        reply_markup=kb.as_markup())
    await c.answer()


class PriceFSM(StatesGroup):
    value = State()


@router.callback_query(F.data.startswith("shopa_setprice:"))
async def cb_setprice(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    what = c.data.split(":")[1]
    await state.set_state(PriceFSM.value)
    await state.update_data(price_what=what)
    name = "ника" if what == "nick" else "ID"
    await ui.edit(c.message, f"Введи цену смены {name} в шимкоинах (например 5):",
                  reply_markup=None)
    await c.answer()


@router.message(PriceFSM.value)
async def msg_setprice(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    from services.amount_parse import shk_parse
    from services import settings as _s
    price = shk_parse(msg.text or "")
    if price is None or price < 0:
        return await ui.reply(msg, "Нужно число (шимкоины). Ещё раз:")
    data = await state.get_data()
    what = data.get("price_what")
    await state.set_state(None)
    key = "nickname_price" if what == "nick" else "public_id_price"
    await _s.set(key, str(price), msg.from_user.id)
    name = "ника" if what == "nick" else "ID"
    kb = InlineKeyboardBuilder()
    await btn(kb, "К ценам", "shopa_prices", "back")
    await ui.reply(msg, f"✅ Цена смены {name}: <b>{shk_fmt(price)}</b> 💠",
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
    await btn(kb, "🛡 Щит (шимщит)", "shopa_type:shield")
    await btn(kb, "🧪 Эликсир (снятие проклятия)", "shopa_type:uncurse")
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
        "shield": "Параметры щита: <code>тип значение</code>\n"
                  "• <code>время 60</code> — 60 минут. Можно <code>время 100д</code> (дни), <code>время 5ч</code> (часы)\n"
                  "• <code>разы 3</code> — 3 авто-блока атак",
        "uncurse": "Сколько проклятий снимает эликсир (число, обычно <code>1</code>).\n"
                   "Снимает случайные проклятья. Напиши число:",
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
            # премиум-эмодзи -> сохраняем как <tg-emoji> тег (не подменяется маппингом)
            sym = txt.split()[0]
            emoji_val = sym
            _u16 = txt.encode("utf-16-le")
            for e in (msg.entities or []):
                if e.type == "custom_emoji":
                    s2 = _u16[e.offset*2:(e.offset+e.length)*2].decode("utf-16-le")
                    emoji_val = f'<tg-emoji emoji-id="{e.custom_emoji_id}">{s2}</tg-emoji>'
                    break
            payload = {"emoji": emoji_val}
        elif t == "shield":
            parts = txt.split()
            kind_word = parts[0].lower()
            if kind_word in ("время", "time", "мин", "минуты"):
                payload = {"kind": "time", "minutes": _parse_minutes(parts[1])}
            elif kind_word in ("разы", "раз", "uses"):
                payload = {"kind": "uses", "uses": int(parts[1])}
            else:
                return await ui.reply(msg, "Тип щита: «время» или «разы». Ещё раз:")
        elif t == "uncurse":
            payload = {"count": max(1, int(txt.split()[0]))}
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
    items = await shop.list_items(active_only=True)
    kb = InlineKeyboardBuilder()
    lines = ["🛒 <b>Товары магазина</b>\n\nНажми товар, чтобы удалить:\n"]
    for it in items:
        price_txt = shk_fmt(it["price"]) if it["currency"] == "shimcoins" else f"{it['price']:,}".replace(",", " ")
        lines.append(f"• <b>{it['name']}</b> — {price_txt} {_CUR_NAMES.get(it['currency'],'')}")
        await btn(kb, f"{it['name'][:24]}", f"shopa_pick:{it['id']}")
    if not items:
        lines.append("<i>Пусто.</i>")
    await btn(kb, "Назад", "admin", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("shopa_pick:"))
async def cb_pick(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    iid = int(c.data.split(":")[1])
    it = await shop.get_item(iid)
    if not it:
        c.data = "shopa_list"
        return await cb_list(c)
    price_txt = shk_fmt(it["price"]) if it["currency"] == "shimcoins" else f"{it['price']:,}".replace(",", " ")
    kb = InlineKeyboardBuilder()
    await btn(kb, "🗑 Удалить", f"shopa_kill:{iid}")
    await btn(kb, "Назад", "shopa_list", "back")
    kb.adjust(1)
    await ui.edit(c.message,
        f"🛒 <b>{it['name']}</b>\n{it.get('description') or ''}\n"
        f"Цена: {price_txt} {_CUR_NAMES.get(it['currency'],'')}",
        reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("shopa_kill:"))
async def cb_kill(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    iid = int(c.data.split(":")[1])
    await shop.delete_item(iid)
    await c.answer("Товар удалён.")
    c.data = "shopa_list"
    await cb_list(c)
