from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database
from keyboards import kb_templates
from constants import TEMPLATES

router = Router()


class ClientSetupSG(StatesGroup):
    choose_template = State()


async def start_client_setup(message: Message, state: FSMContext):
    await state.set_state(ClientSetupSG.choose_template)
    await message.answer(
        "🎨 <b>Клиентский шаблон</b>\n\n"
        "Выбери стиль карточки для своих отзывов. "
        "Этот шаблон будет использоваться когда ты пишешь "
        "<code>@RevShimbot текст отзыва</code> в чате с продавцом.",
        reply_markup=kb_templates()
    )


@router.callback_query(ClientSetupSG.choose_template, F.data.startswith("tpl:"))
async def cb_client_template(call: CallbackQuery, state: FSMContext, db: Database):
    tid = call.data.split(":")[1]
    await call.answer(f"✅ Выбран {tid}")
    await db.save_client_template(
        call.from_user.id,
        call.from_user.username,
        tid
    )
    await state.clear()
    tpl_name = TEMPLATES.get(tid, tid)
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(
        f"✅ <b>Клиентский шаблон сохранён!</b>\n\n"
        f"Стиль: <b>{tpl_name}</b>\n\n"
        f"Теперь когда ты пишешь в чате с продавцом:\n"
        f"<code>@RevShimbot текст отзыва</code>\n\n"
        f"Бот автоматически создаст карточку в твоём стиле."
    )
