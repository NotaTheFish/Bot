"""
Особые предложения (акции обмена Шимкоинов на грибы/коины).

Админская часть: создание (адресно из карточки юзера), список всех акций,
удаление. Клиентская покупка — в handlers/offers_buy.py (следующий этап).

Создание акции — пошаговый FSM:
  1. валюта: грибы / коины / обе
  2. цена в Шимкоинах за 1млн грибов (если грибы)
  3. цена в Шимкоинах за 100млн коинов (если коины)
  4. лимит на всю акцию (сколько всего можно купить) или «без лимита»
  5. срок действия (1д / 24ч / безлим)
"""
import contextlib
import logging
from datetime import datetime, timedelta, timezone

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from config import SUPER_ADMINS
from services import settings, ui
from services.amount_parse import parse_amount

router = Router()
log = logging.getLogger("offers")


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class OfferNew(StatesGroup):
    price_mush = State()
    price_coin = State()
    limit = State()
    expiry = State()


def _parse_expiry(text: str):
    """'1д'/'24ч'/'30м'/'безлим' -> datetime | None. None если бессрочно/ошибка."""
    t = (text or "").strip().lower()
    if t in ("безлим", "бессрочно", "-", "0"):
        return None
    import re
    m = re.fullmatch(r"(\d+)\s*([дчмdhm])", t)
    if not m:
        return "err"
    n = int(m.group(1))
    unit = m.group(2)
    now = datetime.now(timezone.utc)
    if unit in ("д", "d"):
        return now + timedelta(days=n)
    if unit in ("ч", "h"):
        return now + timedelta(hours=n)
    return now + timedelta(minutes=n)


# ---------------- создание акции (адресно) ----------------
@router.callback_query(F.data.startswith("off_new:"))
async def cb_offer_new(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    await state.set_state(None)
    await state.update_data(offer={"tg_id": tg_id})
    await ui.edit(c.message,
        f"🏷 <b>Новая акция</b> для <code>{tg_id}</code>\n\n"
        f"Какую валюту можно будет купить за Шимкоины?",
        reply_markup=await kb.offer_currency(tg_id))
    await c.answer()


@router.callback_query(F.data.startswith("off_cur:"))
async def cb_offer_currency(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    _, tg_id_s, which = c.data.split(":")  # which = mush|coin|both
    data = await state.get_data()
    offer = data.get("offer") or {"tg_id": int(tg_id_s)}
    offer["which"] = which
    await state.update_data(offer=offer)
    # запрашиваем цены по порядку
    if which in ("mush", "both"):
        await state.set_state(OfferNew.price_mush)
        await ui.edit(c.message,
            "🍄 Цена <b>грибов</b>\n\n"
            "Сколько Шимкоинов стоит <b>1 000 000 грибов</b>?\n"
            "Напиши число (например <code>5</code>).")
    else:  # только коины
        await state.set_state(OfferNew.price_coin)
        await ui.edit(c.message,
            "🪙 Цена <b>коинов</b>\n\n"
            "Сколько Шимкоинов стоит <b>100 000 000 коинов</b>?\n"
            "Напиши число (например <code>3</code>).")
    await c.answer()


@router.message(OfferNew.price_mush)
async def offer_price_mush(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    price = parse_amount(msg.text or "")
    if price is None or price <= 0:
        return await ui.answer(msg, "Нужно положительное число Шимкоинов.")
    data = await state.get_data()
    offer = data["offer"]
    offer["price_mush"] = price
    await state.update_data(offer=offer)
    if offer["which"] == "both":
        await state.set_state(OfferNew.price_coin)
        await ui.answer(msg,
            "🪙 Цена <b>коинов</b>\n\n"
            "Сколько Шимкоинов стоит <b>100 000 000 коинов</b>?\n"
            "Напиши число.")
    else:
        await _ask_limit(msg, state)


@router.message(OfferNew.price_coin)
async def offer_price_coin(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    price = parse_amount(msg.text or "")
    if price is None or price <= 0:
        return await ui.answer(msg, "Нужно положительное число Шимкоинов.")
    data = await state.get_data()
    offer = data["offer"]
    offer["price_coin"] = price
    await state.update_data(offer=offer)
    await _ask_limit(msg, state)


async def _ask_limit(msg, state: FSMContext):
    await state.set_state(OfferNew.limit)
    data = await state.get_data()
    which = data["offer"]["which"]
    hint = ""
    if which in ("mush", "both"):
        hint += "грибов (например <code>20м</code>)"
    if which == "both":
        hint += " и коинов через пробел"
    elif which == "coin":
        hint += "коинов (например <code>500м</code>)"
    await ui.answer(msg,
        f"📦 <b>Лимит акции</b>\n\n"
        f"Сколько ВСЕГО можно будет купить {hint}?\n"
        f"Или напиши <code>безлим</code>.\n\n"
        + ("Для двух валют — два числа через пробел: <code>20м 500м</code> "
           "(грибы коины)." if which == "both" else ""))


@router.message(OfferNew.limit)
async def offer_limit(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    data = await state.get_data()
    offer = data["offer"]
    which = offer["which"]
    txt = (msg.text or "").strip().lower()
    lim_mush = lim_coin = None
    if txt not in ("безлим", "бессрочно", "0", "-"):
        parts = txt.split()
        if which == "both":
            if len(parts) != 2:
                return await ui.answer(msg, "Нужно два числа: грибы коины. Или <code>безлим</code>.")
            lim_mush = parse_amount(parts[0])
            lim_coin = parse_amount(parts[1])
            if lim_mush is None or lim_coin is None:
                return await ui.answer(msg, "Не понял числа. Пример: <code>20м 500м</code>.")
        elif which == "mush":
            lim_mush = parse_amount(parts[0])
            if lim_mush is None:
                return await ui.answer(msg, "Не понял число.")
        else:
            lim_coin = parse_amount(parts[0])
            if lim_coin is None:
                return await ui.answer(msg, "Не понял число.")
    offer["limit_mush"] = lim_mush
    offer["limit_coin"] = lim_coin
    await state.update_data(offer=offer)
    await state.set_state(OfferNew.expiry)
    await ui.answer(msg,
        "⏳ <b>Срок действия</b>\n\n"
        "Например <code>1д</code>, <code>3д</code>, <code>24ч</code>, <code>30м</code> "
        "или <code>безлим</code>.")


@router.message(OfferNew.expiry)
async def offer_expiry(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    exp = _parse_expiry(msg.text or "")
    if exp == "err":
        return await ui.answer(msg, "Не понял срок. Пример: <code>1д</code>, <code>24ч</code>.")
    data = await state.get_data()
    offer = data["offer"]
    which = offer["which"]
    pm = offer.get("price_mush") if which in ("mush", "both") else None
    pc = offer.get("price_coin") if which in ("coin", "both") else None
    oid = await db.offer_create(
        offer["tg_id"], pm, pc, offer.get("limit_mush"), offer.get("limit_coin"),
        exp, msg.from_user.id)
    await db.audit(msg.from_user.id, "offer_create", {"id": oid, "target": offer["tg_id"]})
    await state.clear()

    # уведомим игрока, что ему доступна акция
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, offer["tg_id"],
            "🏷 <b>Тебе доступно особое предложение!</b>\n"
            "Загляни в профиль → «Особые предложения».")

    when = "бессрочно" if exp is None else exp.astimezone().strftime("%d.%m %H:%M")
    lines = [f"✅ <b>Акция создана</b> для <code>{offer['tg_id']}</code>\n"]
    if pm:
        lm = "без лимита" if offer.get("limit_mush") is None else fmt(offer["limit_mush"])
        lines.append(f"🍄 {pm} Шимк. за 1млн грибов · лимит {lm}")
    if pc:
        lc = "без лимита" if offer.get("limit_coin") is None else fmt(offer["limit_coin"])
        lines.append(f"🪙 {pc} Шимк. за 100млн коинов · лимит {lc}")
    lines.append(f"⏳ Срок: {when}")
    await ui.answer(msg, "\n".join(lines), reply_markup=await kb.offers_back())


# ---------------- список акций / удаление ----------------
@router.callback_query(F.data == "off_menu")
async def cb_offers_menu(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    offers = await db.offers_all()
    if not offers:
        return await ui.edit(c.message,
            "🏷 <b>Акции</b>\n\nПока нет ни одной. Создать можно из карточки игрока "
            "(Найти юзера → Предложить акцию).",
            reply_markup=await kb.offers_back())
    await ui.edit(c.message, "🏷 <b>Акции</b>\n\nВыбери, чтобы посмотреть или удалить:",
                  reply_markup=await kb.offers_list(offers))
    await c.answer()


@router.callback_query(F.data.startswith("off_view:"))
async def cb_offer_view(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    oid = int(c.data.split(":")[1])
    o = await db.offer_get(oid)
    if not o:
        return await c.answer("Акция не найдена.", show_alert=True)
    live = await db.offer_is_live(o)
    when = "бессрочно" if o["expires_at"] is None else o["expires_at"].astimezone().strftime("%d.%m %H:%M")
    lines = [f"🏷 <b>Акция #{o['id']}</b> {'🟢 активна' if live else '🔴 неактивна'}\n",
             f"Игрок: <code>{o['tg_id']}</code>"]
    if o["price_mush"]:
        lm = "∞" if o["limit_mush"] is None else fmt(o["limit_mush"])
        lines.append(f"🍄 {o['price_mush']} Шимк./1млн · продано {fmt(o['sold_mush'])}/{lm}")
    if o["price_coin"]:
        lc = "∞" if o["limit_coin"] is None else fmt(o["limit_coin"])
        lines.append(f"🪙 {o['price_coin']} Шимк./100млн · продано {fmt(o['sold_coin'])}/{lc}")
    lines.append(f"⏳ Срок: {when}")
    await ui.edit(c.message, "\n".join(lines), reply_markup=await kb.offer_card(oid))
    await c.answer()


@router.callback_query(F.data.startswith("off_del:"))
async def cb_offer_del(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    oid = int(c.data.split(":")[1])
    await db.offer_delete(oid)
    await db.audit(c.from_user.id, "offer_delete", {"id": oid})
    await c.answer("Акция удалена.")
    offers = await db.offers_all()
    if not offers:
        return await ui.edit(c.message, "🏷 <b>Акции</b>\n\nБольше нет.",
                             reply_markup=await kb.offers_back())
    await ui.edit(c.message, "🏷 <b>Акции</b>\n\nВыбери:",
                  reply_markup=await kb.offers_list(offers))
