from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext

from db import Database
from keyboards import kb_seller_menu
from utils.helpers import get_ref_link

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, db: Database, bot, state: FSMContext, command=None):
    await state.clear()
    args = message.text.split()
    deep_link = args[1] if len(args) > 1 else None

    # Покупатель пришёл по реферальной ссылке
    if deep_link and deep_link.startswith("seller_"):
        try:
            seller_id = int(deep_link.replace("seller_", ""))
        except ValueError:
            await message.answer("❌ Неверная ссылка.")
            return

        seller = await db.get_seller(seller_id)
        if not seller:
            await message.answer("❌ Магазин не найден или продавец ещё не настроил шаблон.")
            return

        # Сохраняем контекст покупателя и запускаем flow отзыва
        await state.set_data({"seller_id": seller_id, "mode": "buyer"})
        from handlers.review import start_review_flow
        await start_review_flow(message, seller, state, db)
        return

    # Продавец или новый пользователь
    seller = await db.get_seller(message.from_user.id)
    name = message.from_user.first_name or "друг"

    if seller:
        await message.answer(
            f"👋 С возвращением, <b>{name}</b>!\n\n"
            f"Твой магазин: <b>{seller['shop_name']}</b>\n"
            f"Шаблон: <b>{seller['template_id']}</b>",
            reply_markup=kb_seller_menu()
        )
    else:
        await message.answer(
            f"👋 Привет, <b>{name}</b>!\n\n"
            f"Я <b>ReviewBot</b> — помогаю создавать красивые карточки отзывов для Telegram-магазинов.\n\n"
            f"Как это работает:\n"
            f"1. Ты настраиваешь шаблон своего магазина\n"
            f"2. Получаешь реферальную ссылку\n"
            f"3. Покупатели открывают её, пишут отзыв и получают красивую карточку\n\n"
            f"Начнём настройку?"
        )
        from handlers.setup import cmd_setup
        await cmd_setup(message, db, state)


@router.message(Command("menu"))
async def cmd_menu(message: Message, db: Database):
    seller = await db.get_seller(message.from_user.id)
    if not seller:
        await message.answer("Сначала настрой шаблон — /setup")
        return
    await message.answer(
        f"🏪 <b>{seller['shop_name']}</b> — меню продавца",
        reply_markup=kb_seller_menu()
    )


@router.callback_query(F.data == "menu:mylink")
async def cb_mylink(call: CallbackQuery, db: Database, config):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    link = get_ref_link(config.BOT_USERNAME, call.from_user.id)
    await call.message.answer(
        f"🔗 Твоя реферальная ссылка:\n\n"
        f"<code>{link}</code>\n\n"
        f"Поделись ею с покупателями — они откроют её и оставят красивый отзыв одним нажатием."
    )
    await call.answer()


@router.callback_query(F.data == "menu:stats")
async def cb_stats(call: CallbackQuery, db: Database):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    stats = await db.get_seller_stats(call.from_user.id)
    await call.message.answer(
        f"📊 <b>Статистика магазина {seller['shop_name']}</b>\n\n"
        f"Всего отзывов: <b>{stats['total']}</b>\n"
        f"Средняя оценка: <b>{stats['avg_stars'] or '—'} ★</b>"
    )
    await call.answer()


@router.callback_query(F.data == "menu:edit")
async def cb_edit(call: CallbackQuery, db: Database, state: FSMContext):
    await call.answer()
    from handlers.setup import cmd_setup
    await cmd_setup(call.message, db, state)


@router.callback_query(F.data == "cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.answer("❌ Отменено.")
    await call.answer()
