"""
«Подарить»: передача валюты/токенов/предметов другому игроку по его публичному ID.
Безопасная сделка: выбор → количество → ID получателя → подтверждение.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, settings, transfer, profile, inventory as inv
from services.ui import btn
from services.amount_parse import parse_amount, shk_parse, shk_fmt

router = Router()

_CUR_E = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠",
          "revive": "revive", "max": "max", "partials": "partials"}
_CUR_NAME = {"mushrooms": "Грибы", "coins": "Коины", "shimcoins": "Шимкоины",
             "revive": "Revive", "max": "Max", "partials": "Partials"}


class GiftFSM(StatesGroup):
    amount = State()
    target = State()


def _fmt(amount: int, cur: str) -> str:
    if cur == "shimcoins":
        return f"{shk_fmt(amount)} 💠"
    e = _CUR_E.get(cur, "")
    return f"{amount:,}".replace(",", " ") + f" {e}"


@router.callback_query(F.data == "gift_open")
async def cb_gift(c: CallbackQuery):
    b = await db.balances(c.from_user.id)
    kb = InlineKeyboardBuilder()
    lines = ["🎁 <b>Подарить</b>\n\nЧто передать другому игроку? (без комиссии)"]
    # валюты/токены с ненулевым балансом
    for cur in transfer.CURRENCIES:
        if b.get(cur, 0) > 0:
            await btn(kb, f"{_CUR_NAME[cur]}: {_fmt(b[cur], cur)}", f"gift_cur:{cur}")
    # предметы инвентаря
    items = await inv.inventory(c.from_user.id)
    if items:
        await btn(kb, "📦 Предмет из инвентаря", "gift_items")
    await btn(kb, "Назад", "profile", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("gift_cur:"))
async def cb_gift_cur(c: CallbackQuery, state: FSMContext):
    cur = c.data.split(":")[1]
    await state.update_data(gift={"kind": "currency", "cur": cur})
    await state.set_state(GiftFSM.amount)
    await ui.edit(c.message,
        f"Сколько {_CUR_NAME[cur]} подарить? Введи число:" +
        ("\n<i>(шимкоины: 5 = 5.00)</i>" if cur == "shimcoins" else ""), reply_markup=None)
    await c.answer()


@router.message(GiftFSM.amount)
async def msg_gift_amount(msg: Message, state: FSMContext):
    data = await state.get_data(); g = data.get("gift", {})
    cur = g.get("cur")
    amount = shk_parse(msg.text or "") if cur == "shimcoins" else parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    b = await db.balances(msg.from_user.id)
    if b.get(cur, 0) < amount:
        return await ui.reply(msg, f"Недостаточно: у тебя {_fmt(b.get(cur,0), cur)}. Ещё раз:")
    g["amount"] = amount
    await state.update_data(gift=g)
    await state.set_state(GiftFSM.target)
    await ui.reply(msg, "Введи <b>ID</b> получателя (4-5 символов):")


@router.callback_query(F.data == "gift_items")
async def cb_gift_items(c: CallbackQuery):
    items = await inv.inventory(c.from_user.id)
    kb = InlineKeyboardBuilder()
    if not items:
        await btn(kb, "Назад", "gift_open", "back")
        kb.adjust(1)
        return await ui.edit(c.message, "📦 Нет предметов для подарка.", reply_markup=kb.as_markup())
    for it in items:
        p = it["payload"]; t = it["item_type"]
        if t == "luck":
            lbl = f"🍀 Удача ×{p.get('mult',2):g} на {p.get('minutes',15)}м"
        elif t == "discount":
            lbl = f"🏷 Скидка {p.get('percent',10)}%"
        elif t == "shield":
            lbl = f"🛡 Щит {'на '+str(p.get('minutes',60))+'м' if p.get('kind')=='time' else '×'+str(p.get('uses',1))}"
        else:
            lbl = t
        await btn(kb, lbl, f"gift_item:{it['id']}")
    await btn(kb, "Назад", "gift_open", "back")
    kb.adjust(1)
    await ui.edit(c.message, "📦 Какой предмет подарить?", reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("gift_item:"))
async def cb_gift_item(c: CallbackQuery, state: FSMContext):
    inv_id = int(c.data.split(":")[1])
    await state.update_data(gift={"kind": "item", "inv_id": inv_id})
    await state.set_state(GiftFSM.target)
    await ui.edit(c.message, "Введи <b>ID</b> получателя (4-5 символов):", reply_markup=None)
    await c.answer()


@router.message(GiftFSM.target)
async def msg_gift_target(msg: Message, state: FSMContext):
    data = await state.get_data(); g = data.get("gift", {})
    target_id = await profile.resolve_public_id((msg.text or "").strip())
    if not target_id:
        return await ui.reply(msg, "Игрок с таким ID не найден. Проверь и введи ещё раз:")
    if target_id == msg.from_user.id:
        await state.set_state(None)
        return await ui.reply(msg, "Нельзя подарить самому себе 🙂")
    g["target"] = target_id
    await state.update_data(gift=g)
    await state.set_state(None)
    # подтверждение
    tname = await profile.display_name(target_id, link=False)
    if g["kind"] == "currency":
        what = _fmt(g["amount"], g["cur"])
    else:
        it = await db.pool().fetchrow("SELECT * FROM rb_inventory WHERE id=$1", g["inv_id"])
        what = "предмет" if it else "?"
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Подтвердить", "gift_confirm")
    await btn(kb, "❌ Отмена", "gift_cancel")
    kb.adjust(2)
    await ui.reply(msg,
        f"🎁 <b>Проверь сделку:</b>\n\nПередать: <b>{what}</b>\n"
        f"Получатель: {tname} (ID <code>{(msg.text or '').strip()}</code>)\n\n"
        f"Всё верно?", reply_markup=kb.as_markup())


@router.callback_query(F.data == "gift_cancel")
async def cb_gift_cancel(c: CallbackQuery, state: FSMContext):
    await state.update_data(gift=None)
    await c.answer("Отменено.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Подарок отменён.", reply_markup=None)


@router.callback_query(F.data == "gift_confirm")
async def cb_gift_confirm(c: CallbackQuery, state: FSMContext):
    data = await state.get_data(); g = data.get("gift", {})
    await state.update_data(gift=None)
    if not g or not g.get("target"):
        return await c.answer("Данные потеряны, начни заново.", show_alert=True)
    target = g["target"]
    if g["kind"] == "currency":
        ok, err = await transfer.transfer_currency(c.from_user.id, target, g["cur"], g["amount"])
        if not ok:
            return await c.answer(f"⚠️ {err}", show_alert=True)
        what = _fmt(g["amount"], g["cur"])
    else:
        ok, err, it = await transfer.transfer_item(c.from_user.id, target, g["inv_id"])
        if not ok:
            return await c.answer(f"⚠️ {err}", show_alert=True)
        what = "предмет"
    fromname = await profile.display_name(c.from_user.id, link=False)
    await c.answer("Подарок отправлен!")
    with contextlib.suppress(Exception):
        await c.message.edit_text(f"✅ Подарок отправлен: <b>{what}</b>", reply_markup=None)
    with contextlib.suppress(Exception):
        await ui.send(c.bot, target, f"🎁 {fromname} подарил тебе: <b>{what}</b>!")
