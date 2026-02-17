from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import asyncpg
from aiogram import Dispatcher, F, Router
from aiogram.filters import CommandStart
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

from .config import Settings
from .excel_export import build_transactions_report
from .reviews import ReviewsService

router = Router(name="admin")

NO_ACCESS_TEXT = "Нет доступа"

START_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика отзывов")],
        [KeyboardButton(text="🧾 Добавить чек")],
        [KeyboardButton(text="📤 Выгрузить Excel")],
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


@dataclass
class DateRange:
    start: Optional[datetime]
    end: datetime

def register_admin_handlers(dispatcher: Dispatcher) -> None:
    dispatcher.include_router(router)


def _is_admin(user_id: Optional[int], settings: Settings) -> bool:
    return user_id is not None and int(user_id) in set(settings.ACCOUNTANT_ADMIN_IDS)


async def _check_access(event: Message | CallbackQuery, settings: Settings) -> bool:
    user = event.from_user
    if _is_admin(user.id if user else None, settings):
        return True

    if isinstance(event, Message):
        await event.answer(NO_ACCESS_TEXT)
    else:
        if event.message:
            await event.message.answer(NO_ACCESS_TEXT)
        await event.answer()
    return False


def _period_range(period: str) -> DateRange:
    now = datetime.now(timezone.utc)
    if period == "all":
        return DateRange(start=None, end=now)
    if period == "day":
        return DateRange(start=now - timedelta(days=1), end=now)
    if period == "week":
        return DateRange(start=now - timedelta(days=7), end=now)
    if period == "month":
        return DateRange(start=now - timedelta(days=30), end=now)
    raise ValueError("unsupported period")


async def _collect_reviews_stats(pool: asyncpg.Pool, settings: Settings, period: str) -> tuple[int, int]:
    date_range = _period_range(period)
    async with pool.acquire() as conn:
        if date_range.start is None:
            deleted = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM reviews
                WHERE channel_id = $1
                  AND deleted_at IS NOT NULL
                """,
                int(settings.REVIEWS_CHANNEL_ID),
            )
        else:
            deleted = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM reviews
                WHERE channel_id = $1
                  AND deleted_at IS NOT NULL
                  AND deleted_at >= $2
                """,
                int(settings.REVIEWS_CHANNEL_ID),
                date_range.start,
            )

        active = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM reviews
            WHERE channel_id = $1
              AND deleted_at IS NULL
            """,
            int(settings.REVIEWS_CHANNEL_ID),
        )
    return int(deleted or 0), int(active or 0)


async def safe_send_document(message: Message, document: BufferedInputFile, caption: str) -> None:
    try:
        await message.bot.send_document(chat_id=message.chat.id, document=document, caption=caption)
    except Exception:
        await message.answer("Не удалось отправить файл. Попробуйте позже.")


@router.message(CommandStart())
async def handle_start(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await message.answer("Выберите действие:", reply_markup=START_KEYBOARD)


@router.message(F.text == "📊 Статистика отзывов")
async def ask_stats_period(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await message.answer("Выберите период:", reply_markup=STATS_KEYBOARD)


@router.callback_query(F.data.startswith("stats:"))
async def show_stats(callback: CallbackQuery, settings: Settings, reviews_service: ReviewsService, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return

    period = callback.data.split(":", maxsplit=1)[1]
    period_to_label = {"day": "Сегодня", "week": "7 дней", "month": "30 дней"}
    if period not in period_to_label:
        await callback.answer("Неверный период", show_alert=True)
        return

    added = await reviews_service.get_stats_reviews(period)
    deleted, active = await _collect_reviews_stats(pool, settings, period)

    await callback.message.answer(
        f"📊 Статистика ({period_to_label[period]})\n"
        f"Добавлено: {added}\n"
        f"Удалено: {deleted}\n"
        f"Активных: {active}"
    )
    await callback.answer()


@router.message(F.text == "🧾 Добавить чек")
async def start_add_check(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await state.clear()
    await state.set_state(AddCheckFSM.item)
    await message.answer("Введите item (или '-' для пустого):")


@router.message(AddCheckFSM.item)
async def add_check_item(message: Message, state: FSMContext) -> None:
    await state.update_data(item=None if message.text == "-" else (message.text or "").strip())
    await state.set_state(AddCheckFSM.qty)
    await message.answer("Введите qty (число):")


@router.message(AddCheckFSM.qty)
async def add_check_qty(message: Message, state: FSMContext) -> None:
    try:
        qty = Decimal((message.text or "").replace(",", ".").strip())
    except InvalidOperation:
        await message.answer("qty должен быть числом. Попробуйте ещё раз.")
        return

    await state.update_data(qty=str(qty))
    await state.set_state(AddCheckFSM.unit_price)
    await message.answer("Введите unit_price (число):")


@router.message(AddCheckFSM.unit_price)
async def add_check_unit_price(message: Message, state: FSMContext) -> None:
    try:
        unit_price = Decimal((message.text or "").replace(",", ".").strip())
    except InvalidOperation:
        await message.answer("unit_price должен быть числом. Попробуйте ещё раз.")
        return

    await state.update_data(unit_price=str(unit_price))
    await state.set_state(AddCheckFSM.currency)
    await message.answer("Введите currency (или '-' для пустого, по умолчанию RUB):")


@router.message(AddCheckFSM.currency)
async def add_check_currency(message: Message, state: FSMContext) -> None:
    currency_raw = (message.text or "").strip()
    currency = "RUB" if currency_raw == "-" or not currency_raw else currency_raw.upper()
    await state.update_data(currency=currency)
    await state.set_state(AddCheckFSM.pay_method)
    await message.answer("Введите pay_method (или '-' для пустого):")


@router.message(AddCheckFSM.pay_method)
async def add_check_pay_method(message: Message, state: FSMContext) -> None:
    await state.update_data(pay_method=None if message.text == "-" else (message.text or "").strip())
    await state.set_state(AddCheckFSM.note)
    await message.answer("Введите note (или '-' для пустого):")


@router.message(AddCheckFSM.note)
async def add_check_note(message: Message, state: FSMContext) -> None:
    await state.update_data(note=None if message.text == "-" else (message.text or "").strip())
    await state.set_state(AddCheckFSM.receipt)
    await message.answer("Отправьте фото/документ чека (или '-' для пустого):")


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
            await message.answer("Нужно отправить фото/документ или '-'.")
            return

    qty = Decimal(data["qty"])
    unit_price = Decimal(data["unit_price"])
    total = qty * unit_price
    amount_kopecks = int((total * 100).to_integral_value())

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transactions (
                admin_id,
                amount_kopecks,
                currency,
                note,
                item,
                qty,
                unit_price,
                total,
                pay_method,
                receipt_file_id
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            """,
            int(message.from_user.id),
            amount_kopecks,
            data.get("currency") or "RUB",
            data.get("note"),
            data.get("item"),
            str(qty),
            str(unit_price),
            str(total),
            data.get("pay_method"),
            receipt_file_id,
        )

    await state.clear()
    await message.answer(
        f"Чек сохранён.\nitem: {data.get('item') or '-'}\nqty: {qty}\n"
        f"unit_price: {unit_price}\ntotal: {total}"
    )


@router.message(F.text == "📤 Выгрузить Excel")
async def ask_export_period(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await message.answer("Выберите период выгрузки:", reply_markup=EXPORT_KEYBOARD)


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

    date_range = _period_range(period)
    async with pool.acquire() as conn:
        if date_range.start is None:
            rows = await conn.fetch("SELECT * FROM transactions ORDER BY created_at DESC, id DESC")
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM transactions
                WHERE created_at >= $1
                ORDER BY created_at DESC, id DESC
                """,
                date_range.start,
            )

    report_bytes, filename = build_transactions_report(list(rows), period=label_by_period[period])
    document = BufferedInputFile(report_bytes, filename=filename)

    if callback.message:
        await safe_send_document(callback.message, document, caption=f"Выгрузка: {label_by_period[period]}")

    await callback.answer("Файл отправлен")