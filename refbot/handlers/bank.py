"""
Банк: игрок обменивает валюты, админ управляет курсами/комиссией/стопом.

Игрок: 🏦 Банк -> выбор направления -> ввод суммы -> предпросмотр -> обмен.
Админ: 🏦 Управление банком -> курсы, комиссия, стоп-тумблер.
"""
import contextlib
import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
import keyboards as kb
from config import SUPER_ADMINS, BANK_MUSH_UNIT, BANK_COIN_UNIT, BANK_EXCH_DAILY_LIMIT
from services import bank, settings, ui
from services.amount_parse import parse_amount
from keyboards import btn

router = Router()
log = logging.getLogger("bank_h")


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))




class Exch(StatesGroup):
    amount = State()


# ==================== ИГРОК ====================
@router.callback_query(F.data == "bank")
async def cb_bank(c: CallbackQuery):
    sx = await settings.ctx()
    pm = await bank.price_mush()
    pc = await bank.price_coin()
    fee = await bank.fee_pct()
    stopped = await bank.is_stopped()
    left = await bank.exch_left(c.from_user.id)
    b = await db.balances(c.from_user.id)
    lines = [
        "🏦 <b>Банк</b>\n",
        f"Курс: <b>{pm:g}</b> 💠 за 1 млн 🍄 · <b>{pc:g}</b> 💠 за 10 млн 🪙",
        f"Комиссия грибы↔коины: <b>{fee:g}%</b> · обмен шимкоинов без комиссии",
        f"Твой баланс: {fmt(b['mushrooms'])} 🍄 · {fmt(b['coins'])} 🪙 · {fmt(b['shimcoins'])} 💠\n",
    ]
    if stopped:
        lines.append("🛑 Обмен грибы↔коины сейчас <b>остановлен</b>.")
    else:
        lines.append(f"Обменов грибы↔коины сегодня осталось: <b>{left}</b> из {BANK_EXCH_DAILY_LIMIT}")
    lines.append("\nВыбери обмен:")
    await ui.edit(c.message, "\n".join(lines), reply_markup=await _bank_menu(stopped))
    await c.answer()


async def _bank_menu(stopped: bool):
    """Шаг 1: что ПОЛУЧИТЬ. Один эмодзи на кнопку."""
    k = InlineKeyboardBuilder()
    await btn(k, "🍄 Получить грибы", "bankget:mushrooms")
    await btn(k, "🪙 Получить коины", "bankget:coins")
    await btn(k, "Меню", "menu", "back")
    k.adjust(1)
    return k.as_markup()


# что можно отдать за каждую цель (src). Порядок: сначала шимкоины (без лимита/комиссии).
_PAY_FOR = {
    "mushrooms": [("shimcoins", "💠 Платить шимкоинами"), ("coins", "🪙 Платить коинами")],
    "coins": [("shimcoins", "💠 Платить шимкоинами"), ("mushrooms", "🍄 Платить грибами")],
}


@router.callback_query(F.data.startswith("bankget:"))
async def cb_bank_get(c: CallbackQuery, state: FSMContext):
    dst = c.data.split(":")[1]
    if dst not in ("mushrooms", "coins"):
        return await c.answer("Ошибка.", show_alert=True)
    stopped = await bank.is_stopped()
    k = InlineKeyboardBuilder()
    dst_name = "грибы" if dst == "mushrooms" else "коины"
    for src, label in _PAY_FOR[dst]:
        # обмен грибы<->коины заблокирован при стопе; шимкоины — всегда доступны
        if {src, dst} == {"mushrooms", "coins"} and stopped:
            continue
        await btn(k, label, f"exch2:{src}:{dst}")
    await btn(k, "Назад", "bank", "back")
    k.adjust(1)
    kb_markup = k.as_markup()
    note = ""
    if stopped and dst in ("mushrooms", "coins"):
        note = "\n\n🛑 Обмен грибы↔коины остановлен — доступны только шимкоины."
    await ui.edit(c.message,
        f"🏦 <b>Получить {dst_name}</b>\n\nЧем платишь?{note}",
        reply_markup=kb_markup)
    await c.answer()


@router.callback_query(F.data.startswith("exch2:"))
async def cb_exch2_pick(c: CallbackQuery, state: FSMContext):
    try:
        _, src, dst = c.data.split(":")
    except ValueError:
        return await c.answer("Ошибка.", show_alert=True)
    if {src, dst} == {"mushrooms", "coins"} and await bank.is_stopped():
        return await c.answer("🛑 Обмен грибы↔коины сейчас остановлен.", show_alert=True)
    await state.set_state(Exch.amount)
    await state.update_data(exch={"src": src, "dst": dst})
    b = await db.balances(c.from_user.id)
    src_name = {"mushrooms": "грибов", "coins": "коинов", "shimcoins": "шимкоинов"}[src]
    fee_note = ""
    if {src, dst} != {"mushrooms", "coins"}:
        fee_note = "\nОбмен шимкоинов — без комиссии."
    await ui.edit(c.message,
        f"🏦 Платишь <b>{src_name}</b>\n\n"
        f"У тебя: <b>{fmt(b[src])}</b> {src_name}{fee_note}\n\n"
        f"Сколько {src_name} обменять? Напиши число (<code>100к</code>, <code>1м</code>).",
        reply_markup=await _exch_cancel())
    await c.answer()


async def _exch_cancel():
    k = InlineKeyboardBuilder()
    await btn(k, "Отмена", "bank", "back")
    k.adjust(1)
    return k.as_markup()


@router.message(Exch.amount)
async def exch_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    ex = data.get("exch")
    if not ex:
        return await state.clear()
    amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.answer(msg, "Нужно положительное число.")
    src, dst = ex["src"], ex["dst"]
    # баланс
    b = await db.balances(msg.from_user.id)
    if amount > b[src]:
        return await ui.answer(msg, f"Недостаточно. У тебя {fmt(b[src])}.")
    got, shk_after, err = await bank.quote(src, dst, amount)
    if err:
        return await ui.answer(msg, f"⚠️ {err}")
    # предпросмотр
    await state.update_data(exch={**ex, "amount": amount, "got": got})
    src_e = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[src]
    dst_e = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[dst]
    is_gm = {src, dst} == {"mushrooms", "coins"}
    if is_gm:
        fee = await bank.fee_pct()
        fee_line = f"Комиссия банка: {fee:g}%\n"
    else:
        fee_line = "Обмен шимкоинов — без комиссии.\n"
    await ui.answer(msg,
        f"🧾 <b>Проверь обмен</b>\n\n"
        f"Отдаёшь: <b>{fmt(amount)}</b> {src_e}\n"
        f"Получаешь: <b>{fmt(got)}</b> {dst_e}\n"
        f"{fee_line}\n"
        f"Подтверждаешь?",
        reply_markup=await _exch_confirm())


async def _exch_confirm():
    k = InlineKeyboardBuilder()
    await btn(k, "✅ Обменять", "exch_go")
    await btn(k, "Отмена", "bank", "back")
    k.adjust(1)
    return k.as_markup()


@router.callback_query(F.data == "exch_go")
async def cb_exch_go(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    ex = data.get("exch")
    if not ex or "amount" not in ex:
        await state.clear()
        return await c.answer("Обмен устарел, начни заново.", show_alert=True)
    src, dst, amount = ex["src"], ex["dst"], ex["amount"]
    await state.clear()
    try:
        got, err = await bank.exchange(c.from_user.id, src, dst, amount)
    except bank.BankError as e:
        return await c.answer(str(e), show_alert=True)
    if err:
        return await c.answer(err, show_alert=True)
    src_e = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[src]
    dst_e = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[dst]
    await ui.edit(c.message,
        f"✅ <b>Обмен выполнен!</b>\n\n"
        f"−{fmt(amount)} {src_e}\n"
        f"+{fmt(got)} {dst_e}",
        reply_markup=await _back_bank())
    await c.answer("Готово!")


async def _back_bank():
    k = InlineKeyboardBuilder()
    await btn(k, "🏦 В банк", "bank")
    await btn(k, "Меню", "menu", "back")
    k.adjust(1)
    return k.as_markup()


# ==================== АДМИН ====================
class BankAdm(StatesGroup):
    price_mush = State()
    price_coin = State()
    fee = State()


@router.callback_query(F.data == "bank_admin")
async def cb_bank_admin(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await _show_bank_admin(c)


async def _show_bank_admin(c):
    pm = await bank.price_mush()
    pc = await bank.price_coin()
    fee = await bank.fee_pct()
    stopped = await bank.is_stopped()
    await ui.edit(c.message,
        f"🏦 <b>Управление банком</b>\n\n"
        f"Курс грибов: <b>{pm:g}</b> 💠 за 1 млн 🍄\n"
        f"Курс коинов: <b>{pc:g}</b> 💠 за 10 млн 🪙\n"
        f"Комиссия: <b>{fee:g}%</b>\n"
        f"Обмен грибы↔коины: {'🛑 ОСТАНОВЛЕН' if stopped else '🟢 работает'}",
        reply_markup=await _bank_admin_kb(stopped))
    if hasattr(c, "answer"):
        with contextlib.suppress(Exception):
            await c.answer()


async def _bank_admin_kb(stopped: bool):
    k = InlineKeyboardBuilder()
    await btn(k, "🍄 Задать курс грибов", "bankset:mush")
    await btn(k, "🪙 Задать курс коинов", "bankset:coin")
    await btn(k, "💸 Задать комиссию", "bankset:fee")
    await btn(k, "🟢 Включить обмен" if stopped else "🛑 Остановить обмен", "bank_stop")
    await btn(k, "Админка", "admin", "back")
    k.adjust(2, 1, 1, 1)
    return k.as_markup()


@router.callback_query(F.data == "bank_stop")
async def cb_bank_stop(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    now_stopped = await bank.toggle_stop(c.from_user.id)
    await c.answer("🛑 Обмен остановлен" if now_stopped else "🟢 Обмен включён")
    await _show_bank_admin(c)


@router.callback_query(F.data.startswith("bankset:"))
async def cb_bank_set(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    what = c.data.split(":")[1]
    if what == "mush":
        await state.set_state(BankAdm.price_mush)
        await ui.edit(c.message, "🍄 Сколько 💠 шимкоинов стоит <b>1 млн грибов</b>?\n"
                      "Напиши число (можно дробное, например <code>5</code> или <code>4.5</code>).")
    elif what == "coin":
        await state.set_state(BankAdm.price_coin)
        await ui.edit(c.message, "🪙 Сколько 💠 шимкоинов стоит <b>10 млн коинов</b>?\n"
                      "Напиши число (например <code>0.5</code>).")
    else:
        await state.set_state(BankAdm.fee)
        await ui.edit(c.message, "💸 Комиссия банка в процентах?\n"
                      "Напиши число (например <code>5</code>).")
    await c.answer()


def _parse_float(text: str):
    try:
        v = float((text or "").strip().replace(",", "."))
        return v if v >= 0 else None
    except ValueError:
        return None


@router.message(BankAdm.price_mush)
async def set_pm(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    v = _parse_float(msg.text or "")
    if v is None or v <= 0:
        return await ui.answer(msg, "Нужно положительное число.")
    await bank.set_price_mush(v, msg.from_user.id)
    await state.clear()
    await ui.answer(msg, f"✅ Курс грибов: {v:g} 💠 за 1 млн 🍄",
                    reply_markup=await _bank_admin_back())


@router.message(BankAdm.price_coin)
async def set_pc(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    v = _parse_float(msg.text or "")
    if v is None or v <= 0:
        return await ui.answer(msg, "Нужно положительное число.")
    await bank.set_price_coin(v, msg.from_user.id)
    await state.clear()
    await ui.answer(msg, f"✅ Курс коинов: {v:g} 💠 за 10 млн 🪙",
                    reply_markup=await _bank_admin_back())


@router.message(BankAdm.fee)
async def set_fee(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    v = _parse_float(msg.text or "")
    if v is None or v > 90:
        return await ui.answer(msg, "Комиссия — число от 0 до 90.")
    await bank.set_fee(v, msg.from_user.id)
    await state.clear()
    await ui.answer(msg, f"✅ Комиссия: {v:g}%", reply_markup=await _bank_admin_back())


async def _bank_admin_back():
    k = InlineKeyboardBuilder()
    await btn(k, "🏦 Управление банком", "bank_admin")
    await btn(k, "Админка", "admin", "back")
    k.adjust(1)
    return k.as_markup()
