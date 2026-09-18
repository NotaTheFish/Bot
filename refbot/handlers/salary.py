"""
Зарплаты: назначение/остановка через карточку пользователя + воркер выплат.
"""
import asyncio
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, settings, salary, profile
from services.ui import btn
from services.amount_parse import parse_amount, shk_parse, shk_fmt
from config import SUPER_ADMINS

router = Router()

_CUR = {"mushrooms": "🍄 грибы", "coins": "🪙 коины", "shimcoins": "💠 шимкоины"}


async def _can(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class SalaryFSM(StatesGroup):
    amount = State()
    day = State()


def _fmt(amount: int, cur: str) -> str:
    if cur == "shimcoins":
        return f"{shk_fmt(amount)} 💠"
    return f"{amount:,}".replace(",", " ") + (" 🍄" if cur == "mushrooms" else " 🪙")


@router.callback_query(F.data.startswith("a_salary:"))
async def cb_salary(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    cur_sal = await salary.get_salary(tg_id)
    kb = InlineKeyboardBuilder()
    lines = [f"💼 <b>Зарплата</b> для <code>{tg_id}</code>", ""]
    if cur_sal and cur_sal["active"]:
        lines.append(f"Сейчас: <b>{_fmt(cur_sal['amount'], cur_sal['currency'])}</b> "
                     f"каждое {cur_sal['pay_day']} число.")
        await btn(kb, "✏️ Изменить", f"a_sal_set:{tg_id}")
        await btn(kb, "🛑 Остановить", f"a_sal_stop:{tg_id}")
    else:
        lines.append("Зарплата не назначена.")
        await btn(kb, "➕ Назначить зарплату", f"a_sal_set:{tg_id}")
    await btn(kb, "Назад", f"a_findback:{tg_id}", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("a_sal_set:"))
async def cb_sal_set(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    await state.update_data(sal={"tg_id": tg_id})
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибы", "a_sal_cur:mushrooms")
    await btn(kb, "🪙 Коины", "a_sal_cur:coins")
    await btn(kb, "💠 Шимкоины", "a_sal_cur:shimcoins")
    kb.adjust(3)
    await ui.edit(c.message, "💼 В какой валюте зарплата?", reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("a_sal_cur:"))
async def cb_sal_cur(c: CallbackQuery, state: FSMContext):
    cur = c.data.split(":")[1]
    data = await state.get_data(); sal = data.get("sal", {})
    sal["currency"] = cur
    await state.update_data(sal=sal)
    await state.set_state(SalaryFSM.amount)
    await ui.edit(c.message,
        f"Сумма зарплаты в {_CUR[cur]} — введи число:" +
        ("\n<i>(шимкоины: 5 = 5.00)</i>" if cur == "shimcoins" else ""), reply_markup=None)
    await c.answer()


@router.message(SalaryFSM.amount)
async def msg_sal_amount(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    data = await state.get_data(); sal = data.get("sal", {})
    cur = sal.get("currency")
    amount = shk_parse(msg.text or "") if cur == "shimcoins" else parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.reply(msg, "Нужно положительное число. Ещё раз:")
    sal["amount"] = amount
    await state.update_data(sal=sal)
    await state.set_state(SalaryFSM.day)
    await ui.reply(msg, "Какого числа каждый месяц платить? Введи день (1-28):")


@router.message(SalaryFSM.day)
async def msg_sal_day(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    day = parse_amount(msg.text or "")
    if day is None or day < 1 or day > 28:
        return await ui.reply(msg, "Введи день от 1 до 28:")
    data = await state.get_data(); sal = data.get("sal", {})
    await state.set_state(None)
    await salary.set_salary(sal["tg_id"], sal["amount"], sal["currency"], int(day),
                            msg.from_user.id)
    pname = await profile.display_name(sal["tg_id"], link=False)
    kb = InlineKeyboardBuilder()
    await btn(kb, "К пользователю", f"a_findback:{sal['tg_id']}", "back")
    await ui.reply(msg,
        f"✅ Зарплата назначена {pname}: <b>{_fmt(sal['amount'], sal['currency'])}</b> "
        f"каждое {int(day)} число.", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("a_sal_stop:"))
async def cb_sal_stop(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Только админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    await salary.stop_salary(tg_id)
    await c.answer("Зарплата остановлена.")
    c.data = f"a_salary:{tg_id}"
    await cb_salary(c)


async def salary_worker(bot):
    """Раз в час проверяет и платит зарплаты, у кого сегодня день выплаты."""
    import logging
    log = logging.getLogger("refbot")
    log.info("salary worker запущен")
    while True:
        try:
            for p in await salary.pay_due():
                sx = await settings.ctx()
                e = sx["e_" + p["currency"]]
                amt = shk_fmt(p["amount"]) if p["currency"] == "shimcoins" \
                    else f"{p['amount']:,}".replace(",", " ")
                with contextlib.suppress(Exception):
                    await ui.send(bot, p["tg_id"],
                        f"💼 Вам начислена зарплата в размере {amt} {e}")
        except Exception as e:
            log.warning("salary worker: %s", e)
        await asyncio.sleep(3600)   # раз в час
