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
    ("shk", "retail", "💠 розница"),
    ("shk", "whole", "💠 опт"),
    ("mush", "retail", "🍄 розница"),
    ("mush", "whole", "🍄 опт"),
]


def _price_show(pay: str, val: int) -> str:
    if val <= 0:
        return "не задана"
    # ШК — цена за партию из 50, грибы — за 1 штуку
    return f"{shk_fmt(val)} 💠 / 50шт" if pay == "shk" else f"{fmt(val)} 🍄 / шт"


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
    lines = [f"{e} <b>{name} — цены</b>\n"
             f"<i>Шимкоины — за партию 50 шт · Грибы — за 1 шт</i>\n"
             f"<i>Розница — до 1000 шт · Опт — от 1000 шт</i>\n"]
    k = InlineKeyboardBuilder()
    for pay, tier, label in _PAY_TIER:
        val = await tokens.get_price(code, pay, tier)
        lines.append(f"{label}: <b>{_price_show(pay, val)}</b>")
        await btn(k, f"{label} — задать", f"tokset:{code}:{pay}:{tier}")
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
    tier_s = "опт (≥1000)" if tier == "whole" else "розница (&lt;1000)"
    if pay == "shk":
        prompt = (f"Цена за <b>партию 50 шт</b> {tokens.TOKENS[code]} "
                  f"в <b>шимкоинах</b> ({tier_s})?\n\n"
                  f"Можно дробно: <code>0.25</code>, <code>5</code>, <code>4.50</code>.\n"
                  f"Напиши число.")
    else:
        prompt = (f"Цена за <b>1 шт</b> {tokens.TOKENS[code]} "
                  f"в <b>грибах</b> ({tier_s})?\n\n"
                  f"<code>100000</code>, <code>100к</code>, <code>1м</code>.\n"
                  f"Напиши число.")
    await ui.edit(c.message, prompt, reply_markup=None)
    await c.answer()


@router.message(TokPrice.value, ~F.text.startswith("/"), F.text != "☰ Меню")
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
    code = tok["code"]
    await tokens.set_price(code, pay, tok["tier"], val, msg.from_user.id)
    await state.clear()

    # сразу показываем обновлённое меню цен этого токена новым сообщением —
    # чтобы не нажимать заново «меню -> админка -> цены».
    name = tokens.TOKENS[code]
    e = await settings.emoji(code)
    tier_s = "опт" if tok["tier"] == "whole" else "розница"
    head = (f"✅ Задано: {_price_show(pay, val)} ({tier_s}).\n\n"
            f"{e} <b>{name} — цены за 1 штуку</b>\n")
    lines = [head]
    k = InlineKeyboardBuilder()
    for p, tier, label in _PAY_TIER:
        v = await tokens.get_price(code, p, tier)
        lines.append(f"{label}: <b>{_price_show(p, v)}</b>")
        await btn(k, f"{label} — задать", f"tokset:{code}:{p}:{tier}")
    await btn(k, "К токенам", "tok_admin", "back")
    k.adjust(1)
    await ui.answer(msg, "\n".join(lines), reply_markup=k.as_markup())
