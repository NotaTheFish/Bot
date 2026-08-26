"""
Админка цен токенов. 3 токена × 4 цены (шимкоины розница/опт, грибы розница/опт).
Цена — за 1 токен. Шимкоины в ЦЕНТАХ (как весь ШК-баланс), грибы в грибах.
"""
import contextlib

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from config import SUPER_ADMINS
from keyboards import btn
from services import settings, tokens, ui
from services.amount_parse import parse_amount, shk_parse, shk_fmt

router = Router()


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class TokPrice(StatesGroup):
    value = State()


_PAY_TIER = [
    ("shk", "retail", "💠 розница (<1000)"),
    ("shk", "whole", "💠 опт (≥1000)"),
    ("mush", "retail", "🍄 розница (<1000)"),
    ("mush", "whole", "🍄 опт (≥1000)"),
]


def _price_show(pay: str, val: int) -> str:
    if val <= 0:
        return "не задана"
    return f"{shk_fmt(val)} 💠" if pay == "shk" else f"{fmt(val)} 🍄"


@router.callback_query(F.data == "tok_admin")
async def cb_tok_admin(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    k = InlineKeyboardBuilder()
    for code, name in tokens.TOKENS.items():
        e = await settings.emoji(code)
        await btn(k, f"{e} {name}", f"tokadm:{code}")
    await btn(k, "Админка", "admin", "back")
    k.adjust(1)
    await ui.edit(c.message,
        "🎫 <b>Цены токенов</b>\n\n"
        "Выбери токен, чтобы задать цены (за 1 штуку).\n"
        "Отдельно за шимкоины и грибы, отдельно розница (&lt;1000) и опт (≥1000).",
        reply_markup=k.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("tokadm:"))
async def cb_tok_one(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    code = c.data.split(":")[1]
    if code not in tokens.TOKENS:
        return await c.answer("Ошибка.", show_alert=True)
    await _show_token_prices(c, code)


async def _show_token_prices(c, code: str):
    name = tokens.TOKENS[code]
    e = await settings.emoji(code)
    lines = [f"{e} <b>{name} — цены за 1 штуку</b>\n"]
    k = InlineKeyboardBuilder()
    for pay, tier, label in _PAY_TIER:
        val = await tokens.get_price(code, pay, tier)
        lines.append(f"{label}: <b>{_price_show(pay, val)}</b>")
        await btn(k, f"Задать: {label}", f"tokset:{code}:{pay}:{tier}")
    await btn(k, "Назад", "tok_admin", "back")
    k.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=k.as_markup())
    if hasattr(c, "answer"):
        with contextlib.suppress(Exception):
            await c.answer()


@router.callback_query(F.data.startswith("tokset:"))
async def cb_tok_set(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    _, code, pay, tier = c.data.split(":")
    await state.set_state(TokPrice.value)
    await state.update_data(tok={"code": code, "pay": pay, "tier": tier})
    unit = "шимкоинах (можно дробно: 5, 4.50)" if pay == "shk" else "грибах (100000, 100к, 1м)"
    await ui.edit(c.message,
        f"Цена за <b>1 {tokens.TOKENS[code]}</b> в {unit}?\n\n"
        f"Напиши число.",
        reply_markup=None)
    await c.answer()


@router.message(TokPrice.value)
async def msg_tok_value(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    data = await state.get_data()
    tok = data.get("tok")
    if not tok:
        return await state.clear()
    pay = tok["pay"]
    # шимкоины -> центы (дробно), грибы -> целое
    if pay == "shk":
        val = shk_parse(msg.text or "")
    else:
        val = parse_amount(msg.text or "")
    if val is None or val <= 0:
        return await ui.answer(msg, "Нужно положительное число.")
    await tokens.set_price(tok["code"], pay, tok["tier"], val, msg.from_user.id)
    await state.clear()
    e = await settings.emoji(tok["code"])
    await ui.answer(msg,
        f"✅ Цена задана: {tokens.TOKENS[tok['code']]} — "
        f"{_price_show(pay, val)} за штуку "
        f"({'опт' if tok['tier']=='whole' else 'розница'}).")
