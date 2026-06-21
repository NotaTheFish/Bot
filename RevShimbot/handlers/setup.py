from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database
from keyboards import (
    kb_templates, kb_stars_mode, kb_stars_value,
    kb_item_mode, kb_allow_template_choice, kb_setup_done
)
from utils.helpers import get_ref_link, get_inline_button_str

router = Router()


class SetupSG(StatesGroup):
    shop_name = State()
    template = State()
    stars_mode = State()
    stars_value = State()
    item_mode = State()
    item_value = State()
    template_choice = State()


async def cmd_setup(message: Message, db: Database, state: FSMContext):
    seller = await db.get_seller(message.from_user.id)
    if seller:
        await state.update_data(
            shop_name=seller["shop_name"],
            template_id=seller["template_id"],
            stars_mode=seller["stars_mode"],
            stars_value=seller["stars_value"],
            item_mode=seller["item_mode"],
            item_value=seller["item_value"],
            allow_template_choice=seller["allow_template_choice"],
        )
    await state.set_state(SetupSG.shop_name)
    await message.answer(
        "🏪 <b>Шаг 1 из 5 — Название магазина</b>\n\n"
        "Введи название своего магазина или канала.\n"
        "<i>Например: ShimShop, CoS Trades, @sonaria_market</i>"
    )


@router.message(SetupSG.shop_name)
async def step_shop_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 64:
        await message.answer("❌ Слишком длинное название, максимум 64 символа.")
        return
    await state.update_data(shop_name=name)
    await state.set_state(SetupSG.template)
    data = await state.get_data()
    await message.answer(
        "🎨 <b>Шаг 2 из 5 — Выбери шаблон карточки</b>\n\n"
        "Покупатели увидят именно этот стиль при написании отзыва.",
        reply_markup=kb_templates(data.get("template_id"))
    )


@router.callback_query(SetupSG.template, F.data.startswith("tpl:"))
async def step_template(call: CallbackQuery, state: FSMContext):
    tid = call.data.split(":")[1]
    await state.update_data(template_id=tid)
    await call.answer(f"Выбран шаблон ✅")
    data = await state.get_data()
    await state.set_state(SetupSG.stars_mode)
    await call.message.answer(
        "⭐️ <b>Шаг 3 из 5 — Звёзды (рейтинг)</b>\n\n"
        "Как будет работать оценка в отзыве?",
        reply_markup=kb_stars_mode(data.get("stars_mode"))
    )


@router.callback_query(SetupSG.stars_mode, F.data.startswith("stars_mode:"))
async def step_stars_mode(call: CallbackQuery, state: FSMContext):
    mode = call.data.split(":")[1]
    await state.update_data(stars_mode=mode)
    await call.answer()
    if mode == "fixed":
        await state.set_state(SetupSG.stars_value)
        await call.message.answer(
            "⭐️ Сколько звёзд зафиксировать?",
            reply_markup=kb_stars_value()
        )
    else:
        await go_to_item_mode(call.message, state)


@router.callback_query(SetupSG.stars_value, F.data.startswith("stars_val:"))
async def step_stars_value(call: CallbackQuery, state: FSMContext):
    val = int(call.data.split(":")[1])
    await state.update_data(stars_value=val)
    await call.answer()
    await go_to_item_mode(call.message, state)


async def go_to_item_mode(message: Message, state: FSMContext):
    await state.set_state(SetupSG.item_mode)
    data = await state.get_data()
    await message.answer(
        "📦 <b>Шаг 4 из 5 — Поле «Что купил»</b>\n\n"
        "Как покупатель будет указывать товар?",
        reply_markup=kb_item_mode(data.get("item_mode"))
    )


@router.callback_query(SetupSG.item_mode, F.data.startswith("item_mode:"))
async def step_item_mode(call: CallbackQuery, state: FSMContext):
    mode = call.data.split(":")[1]
    await state.update_data(item_mode=mode)
    await call.answer()
    if mode == "fixed":
        await state.set_state(SetupSG.item_value)
        await call.message.answer(
            "📦 Введи фиксированное значение для поля «Что купил».\n"
            "<i>Например: Грибы, Токены, Коины, Существа, Услуги</i>"
        )
    elif mode == "hint":
        await state.set_state(SetupSG.item_value)
        await call.message.answer(
            "📦 Введи подсказку для покупателя.\n"
            "<i>Например: Что именно купил? (Грибы, Токены, Существа...)</i>"
        )
    else:
        await state.update_data(item_value="")
        await go_to_template_choice(call.message, state)


@router.message(SetupSG.item_value)
async def step_item_value(message: Message, state: FSMContext):
    val = message.text.strip()
    if len(val) > 128:
        await message.answer("❌ Слишком длинно, максимум 128 символов.")
        return
    await state.update_data(item_value=val)
    await go_to_template_choice(message, state)


async def go_to_template_choice(message: Message, state: FSMContext):
    await state.set_state(SetupSG.template_choice)
    data = await state.get_data()
    await message.answer(
        "🎨 <b>Шаг 5 из 5 — Выбор шаблона покупателем</b>\n\n"
        "Разрешить покупателю самому выбрать стиль карточки?",
        reply_markup=kb_allow_template_choice(data.get("allow_template_choice", False))
    )


@router.callback_query(SetupSG.template_choice, F.data.startswith("tpl_choice:"))
async def step_template_choice(call: CallbackQuery, state: FSMContext, db: Database, config):
    allow = call.data.split(":")[1] == "yes"
    await state.update_data(allow_template_choice=allow)
    await call.answer()
    data = await state.get_data()

    # Сохраняем в БД
    username = call.from_user.username
    await db.upsert_seller(call.from_user.id, username, data["shop_name"])
    await db.update_seller(
        call.from_user.id,
        template_id=data.get("template_id", "classic_gold"),
        stars_mode=data.get("stars_mode", "buyer_choice"),
        stars_value=data.get("stars_value", 5),
        item_mode=data.get("item_mode", "free"),
        item_value=data.get("item_value", ""),
        allow_template_choice=allow,
    )
    await state.clear()

    link = get_ref_link(config.BOT_USERNAME, call.from_user.id)
    inline_btn = get_inline_button_str(config.BOT_USERNAME, call.from_user.id)
    from keyboards import kb_seller_menu
    await call.message.answer(
        f"✅ <b>Шаблон сохранён!</b>\n\n"
        f"🏪 Магазин: <b>{data['shop_name']}</b>\n\n"
        f"🔗 <b>Реферальная ссылка</b> (поделись с покупателями):\n"
        f"<code>{link}</code>\n\n"
        f"⭐️ <b>Inline-кнопка для закрепа</b>\n"
        f"Вставь в описание канала или закреп — покупатель нажмёт и сразу напишет отзыв:\n\n"
        f"Текст кнопки: <code>⭐️ Оставить отзыв</code>\n"
        f"Ссылка кнопки: <code>{inline_btn}</code>\n\n"
        f"<i>В @BotFather или редакторе поста выбери «Inline кнопка» и вставь эту ссылку.</i>",
        reply_markup=kb_seller_menu()
    )
