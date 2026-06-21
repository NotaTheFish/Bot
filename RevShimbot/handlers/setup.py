from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database
from keyboards import (
    kb_templates, kb_stars_mode, kb_stars_value,
    kb_item_mode, kb_allow_template_choice, kb_setup_done, kb_seller_menu
)
from utils.helpers import get_ref_link

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
            edit_mode=False,
        )
    else:
        await state.update_data(edit_mode=False)
    await state.set_state(SetupSG.shop_name)
    await message.answer(
        "🏪 <b>Шаг 1 из 5 — Название магазина</b>\n\n"
        "Введи название своего магазина или канала.\n"
        "<i>Например: ShimShop, CoS Trades, @sonaria_market</i>"
    )


async def _save_and_finish(user_id: int, username, state: FSMContext,
                           db: Database, config, message: Message):
    """Сохраняет шаблон в БД и отправляет финальное сообщение."""
    data = await state.get_data()
    await db.upsert_seller(user_id, username, data.get("shop_name", ""))
    await db.update_seller(
        user_id,
        template_id=data.get("template_id", "classic_gold"),
        stars_mode=data.get("stars_mode", "buyer_choice"),
        stars_value=data.get("stars_value", 5),
        item_mode=data.get("item_mode", "free"),
        item_value=data.get("item_value", ""),
        allow_template_choice=data.get("allow_template_choice", False),
    )
    await state.clear()
    link = get_ref_link(config.BOT_USERNAME, user_id)
    await message.answer(
        f"✅ <b>Шаблон сохранён!</b>\n\n"
        f"🏪 Магазин: <b>{data.get('shop_name', '')}</b>\n\n"
        f"🔗 Реф-ссылка:\n<code>{link}</code>\n\n"
        f"💬 Фраза для покупателей (отзыв прямо в чате):\n"
        f"<code>@{config.BOT_USERNAME} {user_id} текст отзыва</code>",
        reply_markup=kb_seller_menu()
    )


# ── Шаг 1: название ───────────────────────────────────────────────────────

@router.message(SetupSG.shop_name)
async def step_shop_name(message: Message, state: FSMContext, db: Database, config):
    name = message.text.strip()
    if len(name) > 64:
        await message.answer("❌ Слишком длинное, максимум 64 символа.")
        return
    await state.update_data(shop_name=name)
    data = await state.get_data()

    if data.get("edit_mode"):
        await _save_and_finish(message.from_user.id, message.from_user.username,
                               state, db, config, message)
        return

    await state.set_state(SetupSG.template)
    await message.answer(
        "🎨 <b>Шаг 2 из 5 — Стиль карточки</b>",
        reply_markup=kb_templates(data.get("template_id"))
    )


# ── Шаг 2: шаблон ─────────────────────────────────────────────────────────

@router.callback_query(SetupSG.template, F.data.startswith("tpl:"))
async def step_template(call: CallbackQuery, state: FSMContext, db: Database, config):
    tid = call.data.split(":")[1]
    await state.update_data(template_id=tid)
    await call.answer("✅")
    await call.message.edit_reply_markup(reply_markup=None)
    data = await state.get_data()

    if data.get("edit_mode"):
        await _save_and_finish(call.from_user.id, call.from_user.username,
                               state, db, config, call.message)
        return

    await state.set_state(SetupSG.stars_mode)
    await call.message.answer(
        "⭐️ <b>Шаг 3 из 5 — Звёзды</b>",
        reply_markup=kb_stars_mode(data.get("stars_mode"))
    )


# ── Шаг 3: режим звёзд ────────────────────────────────────────────────────

@router.callback_query(SetupSG.stars_mode, F.data.startswith("stars_mode:"))
async def step_stars_mode(call: CallbackQuery, state: FSMContext, db: Database, config):
    mode = call.data.split(":")[1]
    await state.update_data(stars_mode=mode)
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    data = await state.get_data()

    if mode == "fixed":
        await state.set_state(SetupSG.stars_value)
        await call.message.answer("⭐️ Сколько звёзд зафиксировать?",
                                  reply_markup=kb_stars_value())
        return

    if data.get("edit_mode"):
        await _save_and_finish(call.from_user.id, call.from_user.username,
                               state, db, config, call.message)
        return

    await go_to_item_mode(call.message, state)


@router.callback_query(SetupSG.stars_value, F.data.startswith("stars_val:"))
async def step_stars_value(call: CallbackQuery, state: FSMContext, db: Database, config):
    val = int(call.data.split(":")[1])
    await state.update_data(stars_value=val)
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    data = await state.get_data()

    if data.get("edit_mode"):
        await _save_and_finish(call.from_user.id, call.from_user.username,
                               state, db, config, call.message)
        return

    await go_to_item_mode(call.message, state)


# ── Шаг 4: поле «что купил» ───────────────────────────────────────────────

async def go_to_item_mode(message: Message, state: FSMContext):
    await state.set_state(SetupSG.item_mode)
    data = await state.get_data()
    await message.answer(
        "📦 <b>Шаг 4 из 5 — Поле «Что купил»</b>",
        reply_markup=kb_item_mode(data.get("item_mode"))
    )


@router.callback_query(SetupSG.item_mode, F.data.startswith("item_mode:"))
async def step_item_mode(call: CallbackQuery, state: FSMContext, db: Database, config):
    mode = call.data.split(":")[1]
    await state.update_data(item_mode=mode)
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    data = await state.get_data()

    if mode in ("fixed", "hint"):
        await state.set_state(SetupSG.item_value)
        hint = "Введи фиксированное значение\n<i>Например: Грибы, Токены, Коины</i>" \
            if mode == "fixed" else \
            "Введи подсказку для покупателя\n<i>Например: Что именно купил? (Грибы, Токены...)</i>"
        await call.message.answer(f"📦 {hint}")
        return

    # free mode
    await state.update_data(item_value="")
    if data.get("edit_mode"):
        await _save_and_finish(call.from_user.id, call.from_user.username,
                               state, db, config, call.message)
        return
    await go_to_template_choice(call.message, state)


@router.message(SetupSG.item_value)
async def step_item_value(message: Message, state: FSMContext, db: Database, config):
    val = message.text.strip()
    if len(val) > 128:
        await message.answer("❌ Слишком длинно, максимум 128 символов.")
        return
    await state.update_data(item_value=val)
    data = await state.get_data()

    if data.get("edit_mode"):
        await _save_and_finish(message.from_user.id, message.from_user.username,
                               state, db, config, message)
        return

    await go_to_template_choice(message, state)


# ── Шаг 5: выбор шаблона покупателем ─────────────────────────────────────

async def go_to_template_choice(message: Message, state: FSMContext):
    await state.set_state(SetupSG.template_choice)
    data = await state.get_data()
    await message.answer(
        "🎨 <b>Шаг 5 из 5 — Выбор стиля покупателем</b>\n\n"
        "Разрешить покупателю самому выбрать стиль карточки?",
        reply_markup=kb_allow_template_choice(data.get("allow_template_choice", False))
    )


@router.callback_query(SetupSG.template_choice, F.data.startswith("tpl_choice:"))
async def step_template_choice(call: CallbackQuery, state: FSMContext, db: Database, config):
    allow = call.data.split(":")[1] == "yes"
    await state.update_data(allow_template_choice=allow)
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    await _save_and_finish(call.from_user.id, call.from_user.username,
                           state, db, config, call.message)
