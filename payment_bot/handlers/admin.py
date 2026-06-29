import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext

from payment_bot.config import settings
from payment_bot.db.queries import (
    create_admin_key, log_event,
)
from payment_bot.db.queries_users import (
    get_admin_by_telegram_id, get_admin_balance,
    get_all_sub_admins_total_volume, update_admin_aggregator,
)
from payment_bot.keyboards.kb import (
    aggregator_choice_keyboard, cancel_keyboard,
    main_admin_menu, sub_admin_menu,
)
from payment_bot.utils.states import AggregatorSetupFSM
from payment_bot.utils.tokens import generate_admin_key_token, admin_key_expires_at
from payment_bot.utils.crypto import encrypt
from payment_bot.services.aggregators import CURRENCY_MAP
import json

logger = logging.getLogger(__name__)
router = Router()


# ─── BALANCE ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "balance:my")
async def cb_balance_my(call: CallbackQuery):
    tg_id = call.from_user.id
    admin = await get_admin_by_telegram_id(tg_id)
    if not admin:
        await call.answer("Ошибка.")
        return

    balance = await get_admin_balance(admin["id"])
    provider = admin["aggregator"] or "не настроен"

    await call.message.edit_text(
        f"💵 <b>Ваш баланс</b>\n\n"
        f"Накоплено: <b>{balance:.2f} USD</b>\n"
        f"Агрегатор: {provider}\n\n"
        f"<i>Для вывода откройте ваш агрегатор и запросите выплату вручную.</i>",
        reply_markup=main_admin_menu() if tg_id == settings.MAIN_ADMIN_ID else sub_admin_menu(),
        parse_mode="HTML",
    )
    await call.answer()


@router.callback_query(F.data == "balance:subadmins")
async def cb_balance_subadmins(call: CallbackQuery):
    if call.from_user.id != settings.MAIN_ADMIN_ID:
        await call.answer("Недоступно.")
        return

    rows = await get_all_sub_admins_total_volume()
    if not rows:
        await call.message.edit_text(
            "📊 Суб-администраторов пока нет.",
            reply_markup=main_admin_menu(),
        )
        await call.answer()
        return

    total = sum(float(r["total_usd"]) for r in rows)
    lines = [f"👤 #{r['telegram_id']}: {float(r['total_usd']):.2f} USD" for r in rows]

    await call.message.edit_text(
        f"📊 <b>Оборот суб-администраторов</b>\n\n"
        + "\n".join(lines)
        + f"\n\n<b>Итого: {total:.2f} USD</b>",
        reply_markup=main_admin_menu(),
        parse_mode="HTML",
    )
    await call.answer()


# ─── SUB-ADMIN KEY GENERATION ─────────────────────────────────────────────────

@router.callback_query(F.data == "admin_key:create")
async def cb_admin_key_create(call: CallbackQuery):
    if call.from_user.id != settings.MAIN_ADMIN_ID:
        await call.answer("Недоступно.")
        return

    admin = await get_admin_by_telegram_id(call.from_user.id)
    if not admin:
        await call.answer("Ошибка.")
        return

    token = generate_admin_key_token()
    expires = admin_key_expires_at(hours=24)

    await create_admin_key(
        created_by_admin_id=admin["id"],
        key_token=token,
        expires_at=expires,
    )

    await log_event("admin_key_created", {"token": token}, admin_id=admin["id"])

    bot_username = call.bot.username or "YOUR_BOT"
    link = f"https://t.me/{bot_username}?start={token}"

    await call.message.edit_text(
        f"🔑 <b>Ключ суб-администратора создан</b>\n\n"
        f"Одноразовая ссылка (действует 24 часа):\n"
        f"<code>{link}</code>\n\n"
        f"Отправьте эту ссылку новому суб-администратору. "
        f"После активации он сможет создавать сделки.",
        reply_markup=main_admin_menu(),
        parse_mode="HTML",
    )
    await call.answer()


# ─── AGGREGATOR SETUP ─────────────────────────────────────────────────────────

@router.callback_query(F.data == "aggregator:setup")
async def cb_aggregator_setup(call: CallbackQuery):
    admin = await get_admin_by_telegram_id(call.from_user.id)
    current = admin["aggregator"] if admin else None
    currencies = CURRENCY_MAP.get(current, []) if current else []

    text = "⚙️ <b>Настройка платёжного агрегатора</b>\n\n"
    if current:
        text += f"Текущий агрегатор: <b>{current}</b>\n"
        text += f"Поддерживаемые валюты: {', '.join(currencies)}\n\n"
    text += "Выберите агрегатор:"

    await call.message.edit_text(
        text,
        reply_markup=aggregator_choice_keyboard(),
        parse_mode="HTML",
    )
    await call.answer()


@router.callback_query(F.data.startswith("aggregator:choose:"))
async def cb_aggregator_choose(call: CallbackQuery, state: FSMContext):
    provider = call.data.split(":")[-1]
    await state.update_data(provider=provider)
    await state.set_state(AggregatorSetupFSM.waiting_api_key)

    instructions = {
        "lava": (
            "🟢 <b>Lava.ru</b>\n\n"
            "1. Войдите в lava.ru → Настройки → API\n"
            "2. Скопируйте API-ключ\n\n"
            "Введите API-ключ:"
        ),
        "payok": (
            "🔵 <b>Payok.io</b>\n\n"
            "1. Войдите в payok.io → API\n"
            "2. Скопируйте API-ключ\n\n"
            "Введите API-ключ:"
        ),
        "freekassa": (
            "🟠 <b>Freekassa</b>\n\n"
            "1. Войдите в freekassa.ru → Мои магазины\n"
            "2. Скопируйте API-ключ\n\n"
            "Введите API-ключ:"
        ),
    }

    await call.message.edit_text(
        instructions.get(provider, "Введите API-ключ:"),
        reply_markup=cancel_keyboard(),
        parse_mode="HTML",
    )
    await call.answer()


@router.message(AggregatorSetupFSM.waiting_api_key)
async def fsm_aggregator_api_key(message: Message, state: FSMContext):
    api_key = message.text.strip()
    if len(api_key) < 10:
        await message.answer("❌ Похоже, ключ слишком короткий. Попробуйте ещё раз:")
        return

    data = await state.get_data()
    provider = data["provider"]

    if provider == "lava":
        # Lava doesn't need shop_id separately — finish here
        await _save_aggregator(message, state, provider, api_key, {})
        return

    await state.update_data(api_key=api_key)
    await state.set_state(AggregatorSetupFSM.waiting_shop_id)

    shop_id_label = {
        "payok": "Введите ID магазина (Shop ID) из настроек Payok:",
        "freekassa": "Введите ID магазина (merchant ID) из настроек Freekassa:",
    }
    await message.answer(shop_id_label.get(provider, "Введите ID магазина:"))


@router.message(AggregatorSetupFSM.waiting_shop_id)
async def fsm_aggregator_shop_id(message: Message, state: FSMContext):
    shop_id = message.text.strip()
    data = await state.get_data()
    provider = data["provider"]

    if provider == "payok":
        await state.update_data(shop_id=shop_id)
        await state.set_state(AggregatorSetupFSM.waiting_secret)
        await message.answer("Введите секретный ключ (Secret) из настроек Payok:")
        return

    if provider == "freekassa":
        await state.update_data(shop_id=shop_id)
        await state.set_state(AggregatorSetupFSM.waiting_secret)
        await message.answer("Введите Секрет 1 (Secret Word 1) из настроек Freekassa:")
        return


@router.message(AggregatorSetupFSM.waiting_secret)
async def fsm_aggregator_secret(message: Message, state: FSMContext):
    secret = message.text.strip()
    data = await state.get_data()
    provider = data["provider"]
    api_key = data["api_key"]
    extra = {"shop_id": data.get("shop_id", ""), "secret": secret}

    await _save_aggregator(message, state, provider, api_key, extra)


async def _save_aggregator(message: Message, state: FSMContext, provider: str, api_key: str, extra: dict):
    tg_id = message.from_user.id
    admin = await get_admin_by_telegram_id(tg_id)
    if not admin:
        await message.answer("❌ Ошибка: администратор не найден.")
        await state.clear()
        return

    encrypted_key = encrypt(api_key)

    await update_admin_aggregator(
        admin_id=admin["id"],
        aggregator=provider,
        aggregator_key=encrypted_key,
        aggregator_data=extra,
    )

    await log_event(
        "aggregator_configured",
        {"provider": provider},
        admin_id=admin["id"],
    )

    currencies = CURRENCY_MAP.get(provider, [])
    is_main = tg_id == settings.MAIN_ADMIN_ID

    await message.answer(
        f"✅ <b>Агрегатор {provider} настроен!</b>\n\n"
        f"Доступные валюты: {', '.join(currencies)}\n\n"
        f"Теперь вы можете создавать сделки.",
        reply_markup=main_admin_menu() if is_main else sub_admin_menu(),
        parse_mode="HTML",
    )
    await state.clear()
