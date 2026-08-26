"""
Покупка токенов в банке. Поток:
  🎫 Токены -> выбор токена -> выбор оплаты (шимкоины/грибы) -> количество -> подтверждение.

Два режима ввода количества (как в банке):
  "500"   -> купить 500 токенов, посчитать стоимость (округл. ВВЕРХ, в пользу казны)
  "300$"  -> потратить 300 шимкоинов/грибов, посчитать сколько токенов (округл. ВНИЗ)
Опт/розница выбирается по количеству получаемых токенов (>=1000 опт).

Токены целые. Оплата шимкоинами — из баланса в ЦЕНТАХ. Грибами — в грибах.
"""
import contextlib

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from keyboards import btn
from services import settings, tokens, ui
from services.amount_parse import parse_amount, shk_parse, shk_fmt

router = Router()


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


class TokBuy(StatesGroup):
    qty = State()


@router.callback_query(F.data == "tokbuy")
async def cb_tokbuy(c: CallbackQuery):
    k = InlineKeyboardBuilder()
    for code, name in tokens.TOKENS.items():
        e = await settings.emoji(code)
        await btn(k, f"{e} {name}", f"tokbuy:{code}")
    await btn(k, "Назад", "bank", "back")
    k.adjust(1)
    await ui.edit(c.message,
        "🎫 <b>Токены</b>\n\nВыбери, что купить:",
        reply_markup=k.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("tokbuy:"))
async def cb_tokbuy_pick(c: CallbackQuery):
    code = c.data.split(":")[1]
    if code not in tokens.TOKENS:
        return await c.answer("Ошибка.", show_alert=True)
    # какие способы оплаты доступны (цена задана хоть для одного tier)
    k = InlineKeyboardBuilder()
    shk_ok = (await tokens.get_price(code, "shk", "retail") > 0
              or await tokens.get_price(code, "shk", "whole") > 0)
    mush_ok = (await tokens.get_price(code, "mush", "retail") > 0
               or await tokens.get_price(code, "mush", "whole") > 0)
    if shk_ok:
        await btn(k, "💠 Платить шимкоинами", f"tokpay:{code}:shk")
    if mush_ok:
        await btn(k, "🍄 Платить грибами", f"tokpay:{code}:mush")
    await btn(k, "Назад", "tokbuy", "back")
    k.adjust(1)
    if not shk_ok and not mush_ok:
        return await c.answer("Цена на этот токен ещё не задана.", show_alert=True)
    e = await settings.emoji(code)
    await ui.edit(c.message,
        f"{e} <b>{tokens.TOKENS[code]}</b>\n\nЧем платишь?",
        reply_markup=k.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("tokpay:"))
async def cb_tokpay(c: CallbackQuery, state: FSMContext):
    _, code, pay = c.data.split(":")
    if code not in tokens.TOKENS or pay not in ("shk", "mush"):
        return await c.answer("Ошибка.", show_alert=True)
    await state.set_state(TokBuy.qty)
    await state.update_data(tokbuy={"code": code, "pay": pay})
    e = await settings.emoji(code)
    pr = await tokens.get_price(code, pay, "retail")
    pw = await tokens.get_price(code, pay, "whole")
    pay_e = "💠" if pay == "shk" else "🍄"
    pr_s = (shk_fmt(pr) if pay == "shk" else fmt(pr)) if pr > 0 else "—"
    pw_s = (shk_fmt(pw) if pay == "shk" else fmt(pw)) if pw > 0 else "—"
    step_hint = ""
    if pay == "shk":
        step_hint = (f"\n💠 В розницу (до {tokens.WHOLESALE_FROM}) — кратно "
                     f"{tokens.RETAIL_SHK_STEP} шт (50, 100, 950…). От {tokens.WHOLESALE_FROM} — любое.")
    await ui.edit(c.message,
        f"{e} <b>{tokens.TOKENS[code]}</b> · платишь {pay_e}\n\n"
        f"Цена за 1 шт: розница {pr_s} · опт (от {tokens.WHOLESALE_FROM}) {pw_s}\n\n"
        f"Сколько купить? Напиши число (<code>500</code>, <code>1000</code>).\n"
        f"Или сумму с <b>$</b> — потратить столько {pay_e} "
        f"(<code>300$</code>).{step_hint}",
        reply_markup=await _cancel_kb())
    await c.answer()


async def _cancel_kb():
    k = InlineKeyboardBuilder()
    await btn(k, "Отмена", "bank", "back")
    k.adjust(1)
    return k.as_markup()


async def _confirm_kb():
    k = InlineKeyboardBuilder()
    await btn(k, "Купить", "tokbuy_go")
    await btn(k, "Отмена", "bank", "back")
    k.adjust(1)
    return k.as_markup()


@router.message(TokBuy.qty, ~F.text.startswith("/"), F.text != "☰ Меню")
async def msg_tokbuy_qty(msg: Message, state: FSMContext):
    data = await state.get_data()
    tb = data.get("tokbuy")
    if not tb:
        return await state.clear()
    code, pay = tb["code"], tb["pay"]
    raw = (msg.text or "").strip()
    has_dollar = "$" in raw
    b = await db.balances(msg.from_user.id)
    bal_pay = b["shimcoins"] if pay == "shk" else b["mushrooms"]

    if has_dollar:
        # потратить N pay-валюты -> сколько токенов
        budget = shk_parse(raw) if pay == "shk" else parse_amount(raw.replace("$", "").strip())
        if budget is None or budget <= 0:
            return await ui.answer(msg, "Нужно положительное число.")
        qty, cost, err = await tokens.quote_buy_reverse(code, pay, budget)
        if err:
            return await ui.answer(msg, f"⚠️ {err}")
    else:
        qty = parse_amount(raw)
        if qty is None or qty <= 0:
            return await ui.answer(msg, "Нужно положительное число.")
        cost, err = await tokens.quote_buy(code, pay, qty)
        if err:
            return await ui.answer(msg, f"⚠️ {err}")

    if cost > bal_pay:
        pay_e = "💠" if pay == "shk" else "🍄"
        have = shk_fmt(bal_pay) if pay == "shk" else fmt(bal_pay)
        need = shk_fmt(cost) if pay == "shk" else fmt(cost)
        return await ui.answer(msg, f"Недостаточно. Нужно {need} {pay_e}, у тебя {have} {pay_e}.")

    await state.update_data(tokbuy={**tb, "qty": qty, "cost": cost})
    e = await settings.emoji(code)
    pay_e = "💠" if pay == "shk" else "🍄"
    cost_s = shk_fmt(cost) if pay == "shk" else fmt(cost)
    tier = "опт" if qty >= tokens.WHOLESALE_FROM else "розница"
    await ui.answer(msg,
        f"🧾 <b>Проверь покупку</b>\n\n"
        f"Получаешь: <b>{fmt(qty)}</b> {e} {tokens.TOKENS[code]}\n"
        f"Платишь: <b>{cost_s}</b> {pay_e}\n"
        f"Тариф: {tier}\n\n"
        f"Подтверждаешь?",
        reply_markup=await _confirm_kb())


@router.callback_query(F.data == "tokbuy_go")
async def cb_tokbuy_go(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    tb = data.get("tokbuy")
    if not tb or "qty" not in tb:
        await state.clear()
        return await c.answer("Сессия истекла, начни заново.", show_alert=True)
    code, pay, qty, cost = tb["code"], tb["pay"], tb["qty"], tb["cost"]
    await state.clear()

    pay_cur = "shimcoins" if pay == "shk" else "mushrooms"
    uid = c.from_user.id

    # атомарно: списать оплату, начислить токены. Проверка баланса внутри транзакции.
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                bal = await conn.fetchval(
                    "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2 FOR UPDATE",
                    uid, pay_cur) or 0
                if bal < cost:
                    return await c.answer("Недостаточно средств.", show_alert=True)
                idem = f"tokbuy:{uid}:{code}:{c.message.message_id}"
                await db.apply(conn, uid, pay_cur, -cost, "token_buy", idem + ":pay")
                await db.apply(conn, uid, code, qty, "token_buy", idem + ":get")
    except Exception:
        return await c.answer("Не удалось купить, попробуй ещё раз.", show_alert=True)

    e = await settings.emoji(code)
    pay_e = "💠" if pay == "shk" else "🍄"
    cost_s = shk_fmt(cost) if pay == "shk" else fmt(cost)
    b = await db.balances(uid)
    await ui.edit(c.message,
        f"✅ <b>Куплено!</b>\n\n"
        f"+{fmt(qty)} {e} {tokens.TOKENS[code]}\n"
        f"−{cost_s} {pay_e}\n\n"
        f"Теперь у тебя: {fmt(b[code])} {e}",
        reply_markup=await _cancel_kb())
    await c.answer("Готово!")
