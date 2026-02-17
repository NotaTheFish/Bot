from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import asyncpg
from aiogram import Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from .accounting import add_transaction, list_transactions_by_period, to_excel_rows
from .config import Settings
from .excel_export import build_transactions_report
from .reviews import ReviewsService
from .taboo import safe_send_document, safe_send_message

router = Router(name="admin")

NO_ACCESS_TEXT = "Нет доступа"

START_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика отзывов")],
        [KeyboardButton(text="🧾 Добавить чек")],
        [KeyboardButton(text="📤 Выгрузить Excel")],
        [KeyboardButton(text="🔄 Обновить описание")],
    ],
    resize_keyboard=True,
)

STATS_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Сегодня", callback_data="stats:day"),
            InlineKeyboardButton(text="7 дней", callback_data="stats:week"),
            InlineKeyboardButton(text="30 дней", callback_data="stats:month"),
        ]
    ]
)

EXPORT_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Сегодня", callback_data="export:day"),
            InlineKeyboardButton(text="7 дней", callback_data="export:week"),
        ],
        [
            InlineKeyboardButton(text="30 дней", callback_data="export:month"),
            InlineKeyboardButton(text="Всё время", callback_data="export:all"),
        ],
    ]
)


class AddCheckFSM(StatesGroup):
    item = State()
    qty = State()
    unit_price = State()
    currency = State()
    pay_method = State()
    note = State()
    receipt = State()




def register_admin_handlers(dispatcher: Dispatcher) -> None:
    dispatcher.include_router(router)


def _is_admin(user_id: Optional[int], settings: Settings) -> bool:
    return user_id is not None and int(user_id) in set(settings.ACCOUNTANT_ADMIN_IDS)


async def _check_access(event: Message | CallbackQuery, settings: Settings) -> bool:
    user = event.from_user
    if _is_admin(user.id if user else None, settings):
        return True

    if isinstance(event, Message):
        await safe_send_message(event.bot, event.chat.id, NO_ACCESS_TEXT)
    else:
        if event.message:
            await safe_send_message(event.message.bot, event.message.chat.id, NO_ACCESS_TEXT)
        await event.answer()
    return False

@router.message(CommandStart())
async def handle_start(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите действие:", reply_markup=START_KEYBOARD)


@router.message(F.text == "📊 Статистика отзывов")
async def ask_stats_period(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите период:", reply_markup=STATS_KEYBOARD)


@router.callback_query(F.data.startswith("stats:"))
async def show_stats(callback: CallbackQuery, settings: Settings, reviews_service: ReviewsService) -> None:
    if not await _check_access(callback, settings):
        return

    period = callback.data.split(":", maxsplit=1)[1]
    period_to_label = {"day": "Сегодня", "week": "7 дней", "month": "30 дней"}
    if period not in period_to_label:
        await callback.answer("Неверный период", show_alert=True)
        return

    stats = await reviews_service.get_stats_reviews(period)

    await safe_send_message(
        callback.message.bot,
        callback.message.chat.id,
        f"📊 Статистика ({period_to_label[period]})\n"
        f"Добавлено: {stats['added']}\n"
        f"Удалено: {stats['deleted']}\n"
        f"Активных: {stats['active']}",
    )
    await callback.answer()




@router.message(F.text == "🔄 Обновить описание")
@router.message(Command("refresh_about"))
async def refresh_about(message: Message, settings: Settings, reviews_service: ReviewsService) -> None:
    if not await _check_access(message, settings):
        return

    count = await reviews_service.count_active(settings.REVIEWS_CHANNEL_ID)
    await reviews_service.update_channel_about(
        message.bot,
        settings.REVIEWS_CHANNEL_ID,
        settings.ABOUT_TEMPLATE,
        settings.ABOUT_DATE_FORMAT,
    )
    await safe_send_message(message.bot, message.chat.id, f"Описание обновлено. Активных отзывов: {count}")


@router.message(F.text == "🧾 Добавить чек")
async def start_add_check(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await state.clear()
    await state.set_state(AddCheckFSM.item)
    await safe_send_message(message.bot, message.chat.id, "Введите item (или '-' для пустого):")


@router.message(AddCheckFSM.item)
async def add_check_item(message: Message, state: FSMContext) -> None:
    await state.update_data(item=None if message.text == "-" else (message.text or "").strip())
    await state.set_state(AddCheckFSM.qty)
    await safe_send_message(message.bot, message.chat.id, "Введите qty (число):")


@router.message(AddCheckFSM.qty)
async def add_check_qty(message: Message, state: FSMContext) -> None:
    try:
        qty = Decimal((message.text or "").replace(",", ".").strip())
    except InvalidOperation:
        await safe_send_message(message.bot, message.chat.id, "qty должен быть числом. Попробуйте ещё раз.")
        return

    await state.update_data(qty=str(qty))
    await state.set_state(AddCheckFSM.unit_price)
    await safe_send_message(message.bot, message.chat.id, "Введите unit_price (число):")


@router.message(AddCheckFSM.unit_price)
async def add_check_unit_price(message: Message, state: FSMContext) -> None:
    try:
        unit_price = Decimal((message.text or "").replace(",", ".").strip())
    except InvalidOperation:
        await safe_send_message(message.bot, message.chat.id, "unit_price должен быть числом. Попробуйте ещё раз.")
        return

    await state.update_data(unit_price=str(unit_price))
    await state.set_state(AddCheckFSM.currency)
    await safe_send_message(message.bot, message.chat.id, "Введите currency (или '-' для пустого, по умолчанию RUB):")


@router.message(AddCheckFSM.currency)
async def add_check_currency(message: Message, state: FSMContext) -> None:
    currency_raw = (message.text or "").strip()
    currency = "RUB" if currency_raw == "-" or not currency_raw else currency_raw.upper()
    await state.update_data(currency=currency)
    await state.set_state(AddCheckFSM.pay_method)
    await safe_send_message(message.bot, message.chat.id, "Введите pay_method (или '-' для пустого):")


@router.message(AddCheckFSM.pay_method)
async def add_check_pay_method(message: Message, state: FSMContext) -> None:
    await state.update_data(pay_method=None if message.text == "-" else (message.text or "").strip())
    await state.set_state(AddCheckFSM.note)
    await safe_send_message(message.bot, message.chat.id, "Введите note (или '-' для пустого):")


@router.message(AddCheckFSM.note)
async def add_check_note(message: Message, state: FSMContext) -> None:
    await state.update_data(note=None if message.text == "-" else (message.text or "").strip())
    await state.set_state(AddCheckFSM.receipt)
    await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ чека (или '-' для пустого):")


@router.message(AddCheckFSM.receipt)
async def add_check_receipt(message: Message, state: FSMContext, pool: asyncpg.Pool, settings: Settings) -> None:
    if not await _check_access(message, settings):
        await state.clear()
        return

    data = await state.get_data()
    receipt_file_id: Optional[str] = None

    if (message.text or "").strip() != "-":
        if message.photo:
            receipt_file_id = message.photo[-1].file_id
        elif message.document:
            receipt_file_id = message.document.file_id
        else:
            await safe_send_message(message.bot, message.chat.id, "Нужно отправить фото/документ или '-'.")
            return

    qty = Decimal(data["qty"])
    unit_price = Decimal(data["unit_price"])
    total = qty * unit_price
    amount_kopecks = int((total * 100).to_integral_value())

    await add_transaction(
        pool,
        admin_id=int(message.from_user.id),
        amount_kopecks=amount_kopecks,
        currency=data.get("currency") or "RUB",
        note=data.get("note"),
        item=data.get("item"),
        qty=qty,
        unit_price=unit_price,
        total=total,
        pay_method=data.get("pay_method"),
        receipt_file_id=receipt_file_id,
    )

    await state.clear()
    await safe_send_message(
        message.bot,
        message.chat.id,
        f"Чек сохранён.\nitem: {data.get('item') or '-'}\nqty: {qty}\n"
        f"unit_price: {unit_price}\ntotal: {total}",
    )


@router.message(F.text == "📤 Выгрузить Excel")
async def ask_export_period(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите период выгрузки:", reply_markup=EXPORT_KEYBOARD)


@router.callback_query(F.data.startswith("export:"))
async def export_excel(callback: CallbackQuery, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return

    period = callback.data.split(":", maxsplit=1)[1]
    label_by_period = {
        "day": "Сегодня",
        "week": "7 дней",
        "month": "30 дней",
        "all": "Всё время",
    }
    if period not in label_by_period:
        await callback.answer("Неверный период", show_alert=True)
        return

    period_for_filter = {"day": "day", "week": "7days", "month": "30days", "all": "all"}[period]
    rows = await list_transactions_by_period(pool, period=period_for_filter)

    report_bytes = build_transactions_report(to_excel_rows(rows))
    filename = f"transactions_{period}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
    document = BufferedInputFile(report_bytes, filename=filename)

    if callback.message:
        await safe_send_document(
            callback.message.bot,
            callback.message.chat.id,
            document,
            caption=f"Выгрузка: {label_by_period[period]}",
        )

    await callback.answer("Файл отправлен")