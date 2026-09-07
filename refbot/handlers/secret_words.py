"""
Секретные слова достижений. Подключается ПОСЛЕДНИМ роутером, чтобы не перехватывать
ввод других хендлеров (FSM, команды). Проверяет текст в ЛС на совпадение с секретным
словом достижения. Если совпало — засчитывает; иначе молча пропускает.
"""
from aiogram import F, Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from services import ui, achievements as ach
from services.ui import btn

router = Router()


@router.message(F.chat.type == "private", F.text & ~F.text.startswith("/") & ~F.text.startswith("!"))
async def catch_secret_word(msg: Message, state: FSMContext):
    # если игрок в каком-то диалоге (FSM) — не вмешиваемся
    if await state.get_state() is not None:
        return
    ach_row = await ach.try_secret_word(msg.from_user.id, msg.text or "")
    if not ach_row:
        return   # не секретное слово
    kb = InlineKeyboardBuilder()
    await btn(kb, "🎁 Забрать награду", f"ach_claim:{ach_row['id']}:hidden:0")
    await btn(kb, "🏆 К достижениям", "ach_open")
    kb.adjust(1)
    await ui.send(msg.bot, msg.chat.id,
        f"🔓 <b>Секретное достижение открыто!</b>\n\n"
        f"«{ach_row['title']}»\n\nЗабери награду 👇",
        reply_markup=kb.as_markup())
