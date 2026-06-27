from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message
from config import ADMIN_IDS
from db.queries import get_setting
from keyboards.user_kb import main_menu_kb
from keyboards.admin_kb import admin_menu_kb

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id

    if user_id in ADMIN_IDS:
        channel_done = await get_setting("channel_setup_done")
        kb = admin_menu_kb(channel_setup_done=(channel_done == "true"))
        await message.answer(
            "👑 <b>Добро пожаловать, админ!</b>\n\n"
            "Управляй заявками через кнопки ниже.",
            reply_markup=kb,
            parse_mode="HTML",
        )
    else:
        await message.answer(
            "👋 <b>Привет!</b>\n\n"
            "Здесь ты можешь предложить новость или вакансию для нашего канала.\n"
            "Нажми кнопку ниже, чтобы начать.",
            reply_markup=main_menu_kb(),
            parse_mode="HTML",
        )
