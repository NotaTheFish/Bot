import logging
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import CommandStart, CommandObject

from payment_bot.config import settings
from payment_bot.db.queries_users import (
    get_or_create_user, get_admin_by_telegram_id,
    create_main_admin, create_sub_admin,
)
from payment_bot.db.queries import (
    get_deal_by_token, activate_deal_token, get_admin_key,
    consume_admin_key, log_event,
)
from payment_bot.keyboards.kb import (
    main_admin_menu, sub_admin_menu, currency_keyboard,
)
from payment_bot.services.aggregators import CURRENCY_MAP
from payment_bot.utils.crypto import decrypt

logger = logging.getLogger(__name__)
router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    tg_id = message.from_user.id
    arg = command.args or ""

    # ── Bootstrap: register main admin on first /start ────────────────────────
    if tg_id == settings.MAIN_ADMIN_ID:
        admin = await get_admin_by_telegram_id(tg_id)
        if not admin:
            await create_main_admin(tg_id)
            logger.info(f"Main admin registered: {tg_id}")
        await _show_admin_panel(message, is_main=True)
        return

    # ── Sub-admin activation key ───────────────────────────────────────────────
    if arg.startswith("adm_"):
        key = await get_admin_key(arg)
        if not key:
            await message.answer("❌ Ключ не найден.")
            return
        if key["used_by"] is not None:
            await message.answer("❌ Этот ключ уже был использован.")
            return
        from datetime import datetime
        if key["expires_at"] < datetime.utcnow():
            await message.answer("❌ Срок действия ключа истёк. Запросите новый у главного админа.")
            return

        user = await get_or_create_user(tg_id)
        await create_sub_admin(tg_id)
        await consume_admin_key(arg, user["id"])
        await log_event("sub_admin_activated", {"telegram_id": tg_id, "key": arg})
        await message.answer(
            "✅ Вы активированы как суб-администратор!\n\n"
            "Для начала работы настройте платёжный агрегатор.",
        )
        await _show_admin_panel(message, is_main=False)
        return

    # ── Deal invite token ──────────────────────────────────────────────────────
    if arg.startswith("deal_"):
        await _handle_deal_token(message, arg)
        return

    # ── Known sub-admin re-entering bot ───────────────────────────────────────
    admin = await get_admin_by_telegram_id(tg_id)
    if admin:
        await _show_admin_panel(message, is_main=admin["is_main"])
        return

    # ── Anything else: silent ignore (handled by middleware already) ───────────
    return


async def _show_admin_panel(message: Message, is_main: bool):
    if is_main:
        await message.answer(
            "👑 <b>Панель главного администратора</b>",
            reply_markup=main_admin_menu(),
            parse_mode="HTML",
        )
    else:
        await message.answer(
            "🛠 <b>Панель администратора</b>",
            reply_markup=sub_admin_menu(),
            parse_mode="HTML",
        )


async def _handle_deal_token(message: Message, token: str):
    from datetime import datetime

    deal = await get_deal_by_token(token)
    if not deal:
        await message.answer("❌ Сделка не найдена.")
        return

    if deal["status"] == "EXPIRED":
        await message.answer("⏰ Срок действия ссылки истёк. Обратитесь к продавцу.")
        return

    if deal["status"] != "CREATED":
        await message.answer("❌ Эта ссылка уже была использована.")
        return

    if deal["expires_at"] < datetime.utcnow():
        await message.answer("⏰ Срок действия ссылки истёк. Обратитесь к продавцу.")
        return

    # Register/get client
    tg_id = message.from_user.id
    user = await get_or_create_user(tg_id)
    await activate_deal_token(deal["id"], user["id"])

    await log_event(
        "deal_token_activated",
        {"telegram_id": tg_id, "deal_id": deal["id"]},
        deal_id=deal["id"],
    )

    # Get admin's aggregator to know which currencies to offer
    from payment_bot.db.queries_users import get_admin_by_telegram_id
    from payment_bot.db.pool import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        admin = await conn.fetchrow(
            "SELECT * FROM pt_admins WHERE id = $1", deal["admin_id"]
        )

    provider = admin["aggregator"] if admin else None
    currencies = CURRENCY_MAP.get(provider, ["RUB", "UAH", "EUR", "USD"]) if provider else ["RUB", "UAH", "EUR", "USD"]

    await message.answer(
        f"💳 <b>Оплата сделки</b>\n\n"
        f"Сумма: <b>{deal['base_price_usd']:.2f} USD</b>\n\n"
        f"Выберите вашу валюту:",
        reply_markup=currency_keyboard(currencies, deal_id=deal["id"]),
        parse_mode="HTML",
    )
