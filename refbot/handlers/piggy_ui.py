"""
Копилки: UI. Создать (имя+валюта), пополнить, снять, разбить.
Имя валидируется как ник (без ссылок/инъекций).
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, settings, piggy, profile
from services.ui import btn
from services.amount_parse import parse_amount, shk_fmt, shk_parse

router = Router()

_CUR_E = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}


class PiggyFSM(StatesGroup):
    name = State()
    amount_in = State()
    amount_out = State()


def _fmt_amt(amount: int, cur: str) -> str:
    if cur == "shimcoins":
        return f"{shk_fmt(amount)} 💠"
    return f"{amount:,}".replace(",", " ") + f" {_CUR_E.get(cur,'')}"


@router.callback_query(F.data == "piggy_open")
async def cb_open(c: CallbackQuery):
    items = await piggy.list_piggy(c.from_user.id)
    kb = InlineKeyboardBuilder()
    lines = ["🐷 <b>Копилки</b>\n"]
    if not items:
        lines.append("<i>Пока нет копилок. Создай первую!</i>")
    for p in items:
        lines.append(f"🐷 <b>{p['name']}</b>: {_fmt_amt(p['amount'], p['currency'])}")
        await btn(kb, f"{p['name']} ({_fmt_amt(p['amount'], p['currency'])})", f"piggy_view:{p['id']}")
    lines.append(f"\nКопилок: {len(items)}/{piggy.MAX_PIGGY}")
    if len(items) < piggy.MAX_PIGGY:
        await btn(kb, "➕ Новая копилка", "piggy_new")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data == "piggy_new")
async def cb_new(c: CallbackQuery, state: FSMContext):
    if await piggy.count_piggy(c.from_user.id) >= piggy.MAX_PIGGY:
        return await c.answer("Достигнут лимит копилок.", show_alert=True)
    await state.set_state(PiggyFSM.name)
    await ui.edit(c.message,
        "🐷 <b>Новая копилка</b>\n\nПридумай название (до 32 символов, буквы/цифры/пробел, "
        "без ссылок и спецсимволов):", reply_markup=None)
    await c.answer()


@router.message(PiggyFSM.name)
async def msg_name(msg: Message, state: FSMContext):
    name, err = profile.validate_nick(msg.text or "")   # те же правила, что для ника
    if err:
        return await ui.reply(msg, f"⚠️ {err}\nПопробуй другое название:")
    await state.update_data(piggy_name=name)
    await state.set_state(None)
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибы", "piggycur:mushrooms")
    await btn(kb, "🪙 Коины", "piggycur:coins")
    await btn(kb, "💠 Шимкоины", "piggycur:shimcoins")
    kb.adjust(3)
    await ui.reply(msg, f"Копилка «<b>{name}</b>». Какую валюту хранить?",
                   reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("piggycur:"))
async def cb_cur(c: CallbackQuery, state: FSMContext):
    cur = c.data.split(":")[1]
    data = await state.get_data()
    name = data.get("piggy_name")
    await state.set_state(None)
    if not name:
        return await c.answer("Название потеряно, начни заново.", show_alert=True)
    pid, err = await piggy.create_piggy(c.from_user.id, name, cur)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Копилка создана!")
    c.data = f"piggy_view:{pid}"
    await cb_view(c)


@router.callback_query(F.data.startswith("piggy_view:"))
async def cb_view(c: CallbackQuery):
    pid = int(c.data.split(":")[1])
    p = await piggy.get_piggy(pid, c.from_user.id)
    if not p:
        return await c.answer("Копилка не найдена.", show_alert=True)
    b = await db.balances(c.from_user.id)
    kb = InlineKeyboardBuilder()
    await btn(kb, "➕ Пополнить", f"piggy_in:{pid}")
    await btn(kb, "➖ Снять", f"piggy_out:{pid}")
    await btn(kb, "🔨 Разбить", f"piggy_smash:{pid}")
    await btn(kb, "Назад", "piggy_open", "back")
    kb.adjust(2, 1, 1)
    await ui.edit(c.message,
        f"🐷 <b>{p['name']}</b>\n\n"
        f"В копилке: <b>{_fmt_amt(p['amount'], p['currency'])}</b>\n"
        f"На балансе: {_fmt_amt(b.get(p['currency'], 0), p['currency'])}",
        reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("piggy_in:"))
async def cb_in(c: CallbackQuery, state: FSMContext):
    pid = int(c.data.split(":")[1])
    await state.set_state(PiggyFSM.amount_in)
    await state.update_data(piggy_id=pid)
    await ui.edit(c.message, "Сколько положить в копилку? Введи число:", reply_markup=None)
    await c.answer()


@router.callback_query(F.data.startswith("piggy_out:"))
async def cb_out(c: CallbackQuery, state: FSMContext):
    pid = int(c.data.split(":")[1])
    await state.set_state(PiggyFSM.amount_out)
    await state.update_data(piggy_id=pid)
    await ui.edit(c.message, "Сколько снять из копилки? Введи число:", reply_markup=None)
    await c.answer()


def _parse_by_cur(text: str, cur: str):
    return shk_parse(text) if cur == "shimcoins" else parse_amount(text)


@router.message(PiggyFSM.amount_in)
async def msg_in(msg: Message, state: FSMContext):
    data = await state.get_data()
    pid = data.get("piggy_id")
    p = await piggy.get_piggy(pid, msg.from_user.id)
    if not p:
        await state.set_state(None)
        return await ui.reply(msg, "Копилка не найдена.")
    amount = _parse_by_cur(msg.text or "", p["currency"])
    if amount is None or amount <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    ok, err = await piggy.deposit(msg.from_user.id, pid, amount)
    await state.set_state(None)
    if not ok:
        return await ui.reply(msg, f"⚠️ {err}")
    kb = InlineKeyboardBuilder()
    await btn(kb, "🐷 К копилке", f"piggy_view:{pid}", "back")
    await ui.reply(msg, f"✅ Положено {_fmt_amt(amount, p['currency'])} в «{p['name']}».",
                   reply_markup=kb.as_markup())


@router.message(PiggyFSM.amount_out)
async def msg_out(msg: Message, state: FSMContext):
    data = await state.get_data()
    pid = data.get("piggy_id")
    p = await piggy.get_piggy(pid, msg.from_user.id)
    if not p:
        await state.set_state(None)
        return await ui.reply(msg, "Копилка не найдена.")
    amount = _parse_by_cur(msg.text or "", p["currency"])
    if amount is None or amount <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    ok, err = await piggy.withdraw(msg.from_user.id, pid, amount)
    await state.set_state(None)
    if not ok:
        return await ui.reply(msg, f"⚠️ {err}")
    kb = InlineKeyboardBuilder()
    await btn(kb, "🐷 К копилке", f"piggy_view:{pid}", "back")
    await ui.reply(msg, f"✅ Снято {_fmt_amt(amount, p['currency'])} из «{p['name']}».",
                   reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("piggy_smash:"))
async def cb_smash(c: CallbackQuery):
    parts = c.data.split(":")
    pid = int(parts[1])
    confirmed = len(parts) > 2 and parts[2] == "yes"
    p = await piggy.get_piggy(pid, c.from_user.id)
    if not p:
        return await c.answer("Копилка не найдена.", show_alert=True)
    if not confirmed:
        kb = InlineKeyboardBuilder()
        await btn(kb, "🔨 Да, разбить", f"piggy_smash:{pid}:yes")
        await btn(kb, "Отмена", f"piggy_view:{pid}", "back")
        kb.adjust(1)
        return await ui.edit(c.message,
            f"🔨 <b>Разбить «{p['name']}»?</b>\n\n"
            f"Все {_fmt_amt(p['amount'], p['currency'])} вернутся на баланс, "
            f"копилка удалится.", reply_markup=kb.as_markup())
    res, err = await piggy.smash(c.from_user.id, pid)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Копилка разбита!")
    c.data = "piggy_open"
    await cb_open(c)
