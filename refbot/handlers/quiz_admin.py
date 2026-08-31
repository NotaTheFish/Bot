"""
Викторина: админка добавления вопросов. Вопросы от админа дают ×2 балла (points=2).
Команда !добавитьвопрос — пошаговый ввод. !вопросы — статистика/удаление.
"""
import json
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui
from config import SUPER_ADMINS

router = Router()


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class AddQ(StatesGroup):
    question = State()
    options = State()
    correct = State()


@router.message(F.text.lower() == "!добавитьвопрос")
async def cmd_add_q(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return
    if msg.chat.type != "private":
        return await ui.reply(msg, "Добавляй вопросы в личке бота.")
    await state.set_state(AddQ.question)
    await ui.reply(msg, "🧠 <b>Новый вопрос</b>\n\nНапиши текст вопроса:")


@router.message(AddQ.question)
async def add_q_text(msg: Message, state: FSMContext):
    q = (msg.text or "").strip()
    if len(q) < 5:
        return await ui.reply(msg, "Слишком короткий вопрос. Введи ещё раз:")
    await state.update_data(question=q)
    await state.set_state(AddQ.options)
    await ui.reply(msg,
        "Теперь 4 варианта ответа, каждый с новой строки:\n\n"
        "<code>Париж\nЛондон\nМадрид\nРим</code>")


@router.message(AddQ.options)
async def add_q_options(msg: Message, state: FSMContext):
    opts = [o.strip() for o in (msg.text or "").split("\n") if o.strip()]
    if len(opts) != 4:
        return await ui.reply(msg, "Нужно ровно 4 варианта, каждый с новой строки. Ещё раз:")
    await state.update_data(options=opts)
    await state.set_state(AddQ.correct)
    kb = InlineKeyboardBuilder()
    for i, o in enumerate(opts):
        kb.button(text=f"{'ABCD'[i]}) {o[:20]}", callback_data=f"addq_ok:{i}")
    kb.adjust(1)
    await ui.reply(msg, "Какой вариант правильный?", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("addq_ok:"))
async def add_q_correct(c: CallbackQuery, state: FSMContext):
    correct = int(c.data.split(":")[1])
    data = await state.get_data()
    await state.set_state(None)
    q = data.get("question")
    opts = data.get("options")
    if not q or not opts:
        return await c.answer("Данные потеряны, начни заново.", show_alert=True)
    # вопросы админа дают ×2 балла
    await db.pool().execute(
        "INSERT INTO rb_quiz (question, options, correct, added_by, points) "
        "VALUES ($1, $2::jsonb, $3, $4, 2)",
        q, json.dumps(opts, ensure_ascii=False), correct, c.from_user.id)
    await c.answer("Вопрос добавлен!")
    with contextlib.suppress(Exception):
        await c.message.edit_text(
            f"✅ <b>Вопрос добавлен</b> (даёт ×2 балла)\n\n{q}\n\n"
            f"Правильный: {'ABCD'[correct]}) {opts[correct]}", reply_markup=None)


@router.message(F.text.lower() == "!вопросы")
async def cmd_q_stats(msg: Message):
    if not await _is_admin(msg.from_user.id):
        return
    total = await db.pool().fetchval("SELECT count(*) FROM rb_quiz WHERE active")
    admin_q = await db.pool().fetchval("SELECT count(*) FROM rb_quiz WHERE active AND points>=2")
    await ui.reply(msg,
        f"🧠 <b>База викторины</b>\n\n"
        f"Всего активных: <b>{total}</b>\n"
        f"От админов (×2 балла): <b>{admin_q}</b>\n\n"
        f"Добавить: <code>!добавитьвопрос</code>")
