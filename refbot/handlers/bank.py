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
from services.amount_parse import parse_amount, shk_parse, shk_fmt
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
        f"Твой баланс: {fmt(b['mushrooms'])} 🍄 · {fmt(b['coins'])} 🪙 · {shk_fmt(b['shimcoins'])} 💠\n",
    ]
    if stopped:
        lines.append("🛑 Обмен грибы↔коины сейчас <b>остановлен</b>.")
    else:
        lines.append(f"Обменов грибы↔коины сегодня осталось: <b>{left}</b> из {BANK_EXCH_DAILY_LIMIT}")
    # пометка о выключенных товарах
    off = []
    if not await bank.item_enabled("mushrooms"):
        off.append("грибы 🍄")
    if not await bank.item_enabled("coins"):
        off.append("коины 🪙")
    if off:
        lines.append(f"⛔️ Сейчас недоступны: {', '.join(off)}")
    lines.append("\nВыбери обмен:")
    await ui.edit(c.message, "\n".join(lines), reply_markup=await _bank_menu(stopped))
    await c.answer()


async def _bank_menu(stopped: bool):
    """Шаг 1: что ПОЛУЧИТЬ. Один эмодзи на кнопку. Выключенные товары скрыты."""
    k = InlineKeyboardBuilder()
    if await bank.item_enabled("mushrooms"):
        await btn(k, "🍄 Получить грибы", "bankget:mushrooms")
    if await bank.item_enabled("coins"):
        await btn(k, "🪙 Получить коины", "bankget:coins")
    await btn(k, "🎫 Токены", "tokbuy")
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
    if not await bank.item_enabled(dst):
        return await c.answer("Этот товар сейчас недоступен.", show_alert=True)
    stopped = await bank.is_stopped()
    k = InlineKeyboardBuilder()
    dst_name = "грибы" if dst == "mushrooms" else "коины"
    for src, label in _PAY_FOR[dst]:
        # обмен грибы<->коины заблокирован при стопе; шимкоины — всегда доступны
        if {src, dst} == {"mushrooms", "coins"} and stopped:
            continue
        # нельзя платить выключенным товаром (грибы/коины)
        if src in ("mushrooms", "coins") and not await bank.item_enabled(src):
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
    dst_name = {"mushrooms": "грибов", "coins": "коинов"}.get(dst, "")
    if src == "shimcoins":
        # два режима ввода для покупки за шимкоины
        await ui.edit(c.message,
            f"🏦 Платишь <b>шимкоинами</b> 💠 — без комиссии\n"
            f"У тебя: <b>{shk_fmt(b['shimcoins'])}</b> 💠\n\n"
            f"Два способа указать сумму:\n"
            f"• <code>25$</code> — <b>отдать</b> 25 шимкоинов (получишь сколько выйдет)\n"
            f"• <code>25м</code> — <b>получить</b> 25 млн {dst_name} (спишется нужное число 💠)\n\n"
            f"Знак <b>$</b> = считать в шимкоинах. Без него — сколько {dst_name} хочешь получить.",
            reply_markup=await _exch_cancel())
    else:
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
    src, dst = ex["src"], ex["dst"]
    raw = (msg.text or "").strip()

    # Для оплаты шимкоинами два режима ввода:
    #   "25$" / "$25" / "25 $"  -> отдать 25 ШК (прямой расчёт), ввод в ЦЕНТАХ
    #   "25м"  (без $)          -> хочу получить 25м dst, посчитать сколько ШК (обратный)
    # Для грибы<->коины знак $ не нужен — всегда прямой расчёт по отдаваемой валюте.
    has_dollar = "$" in raw
    buy_mode = (src == "shimcoins") and not has_dollar

    b = await db.balances(msg.from_user.id)

    if buy_mode:
        # обратный расчёт: raw = желаемое кол-во dst (грибы/коины, целое)
        amount = parse_amount(raw)
        if amount is None or amount <= 0:
            return await ui.answer(msg, "Нужно положительное число.")
        need_cents, got, err = await bank.quote_reverse(dst, amount)
        if err:
            return await ui.answer(msg, f"⚠️ {err}")
        if need_cents > b["shimcoins"]:
            return await ui.answer(msg,
                f"Нужно <b>{shk_fmt(need_cents)}</b> 💠, а у тебя {shk_fmt(b['shimcoins'])} 💠.")
        spend, real_got = need_cents, got
    elif src == "shimcoins":
        # прямой расчёт, платишь шимкоинами: ввод в ШК ($), парсим в ЦЕНТЫ
        spend_cents = shk_parse(raw)
        if spend_cents is None or spend_cents <= 0:
            return await ui.answer(msg, "Нужно положительное число.")
        if spend_cents > b["shimcoins"]:
            return await ui.answer(msg, f"Недостаточно. У тебя {shk_fmt(b['shimcoins'])} 💠.")
        got, cents_after, err = await bank.quote(src, dst, spend_cents)
        if err:
            return await ui.answer(msg, f"⚠️ {err}")
        spend, real_got = spend_cents, got
    else:
        # прямой расчёт, отдаёшь грибы/коины (целые)
        amount = parse_amount(raw)
        if amount is None or amount <= 0:
            return await ui.answer(msg, "Нужно положительное число.")
        if amount > b[src]:
            return await ui.answer(msg, f"Недостаточно. У тебя {fmt(b[src])}.")
        got, cents_after, err = await bank.quote(src, dst, amount)
        if err:
            return await ui.answer(msg, f"⚠️ {err}")
        spend, real_got = amount, got

    # предпросмотр
    await state.update_data(exch={**ex, "amount": spend, "got": real_got})
    src_e = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[src]
    dst_e = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[dst]
    # ШК показываем с копейками (shk_fmt), грибы/коины — обычным fmt
    spend_s = shk_fmt(spend) if src == "shimcoins" else fmt(spend)
    got_s = shk_fmt(real_got) if dst == "shimcoins" else fmt(real_got)
    is_gm = {src, dst} == {"mushrooms", "coins"}
    if is_gm:
        fee = await bank.fee_pct()
        fee_line = f"Комиссия банка: {fee:g}%\n"
    else:
        fee_line = "Обмен шимкоинов — без комиссии.\n"
    await ui.answer(msg,
        f"🧾 <b>Проверь обмен</b>\n\n"
        f"Отдаёшь: <b>{spend_s}</b> {src_e}\n"
        f"Получаешь: <b>{got_s}</b> {dst_e}\n"
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
    spend_s = shk_fmt(amount) if src == "shimcoins" else fmt(amount)
    got_s = shk_fmt(got) if dst == "shimcoins" else fmt(got)
    await ui.edit(c.message,
        f"✅ <b>Обмен выполнен!</b>\n\n"
        f"−{spend_s} {src_e}\n"
        f"+{got_s} {dst_e}",
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
    mush_on = await bank.item_enabled("mushrooms")
    coin_on = await bank.item_enabled("coins")
    await ui.edit(c.message,
        f"🏦 <b>Управление банком</b>\n\n"
        f"Курс грибов: <b>{pm:g}</b> 💠 за 1 млн 🍄\n"
        f"Курс коинов: <b>{pc:g}</b> 💠 за 10 млн 🪙\n"
        f"Комиссия: <b>{fee:g}%</b>\n"
        f"Обмен грибы↔коины: {'🛑 ОСТАНОВЛЕН' if stopped else '🟢 работает'}\n"
        f"Грибы в банке: {'🟢 вкл' if mush_on else '🔴 выкл'}\n"
        f"Коины в банке: {'🟢 вкл' if coin_on else '🔴 выкл'}",
        reply_markup=await _bank_admin_kb(stopped, mush_on, coin_on))
    if hasattr(c, "answer"):
        with contextlib.suppress(Exception):
            await c.answer()


async def _bank_admin_kb(stopped: bool, mush_on: bool = True, coin_on: bool = True):
    k = InlineKeyboardBuilder()
    await btn(k, "🍄 Задать курс грибов", "bankset:mush")
    await btn(k, "🪙 Задать курс коинов", "bankset:coin")
    await btn(k, "💸 Задать комиссию", "bankset:fee")
    await btn(k, "🟢 Включить обмен" if stopped else "🛑 Остановить обмен", "bank_stop")
    # тумблеры товаров: выключенный товар пропадает из всех операций банка
    await btn(k, f"🍄 Грибы: {'выключить' if mush_on else 'включить'}", "bankitem:mushrooms")
    await btn(k, f"🪙 Коины: {'выключить' if coin_on else 'включить'}", "bankitem:coins")
    await btn(k, "Админка", "admin", "back")
    k.adjust(2, 1, 1, 2, 1)
    return k.as_markup()


@router.callback_query(F.data.startswith("bankitem:"))
async def cb_bank_item(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    item = c.data.split(":")[1]
    if item not in ("mushrooms", "coins"):
        return await c.answer("Ошибка.", show_alert=True)
    now_on = await bank.toggle_item(item, c.from_user.id)
    name = "Грибы" if item == "mushrooms" else "Коины"
    await c.answer(f"{name}: {'включены' if now_on else 'выключены'}")
    await _show_bank_admin(c)


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
