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

from .accounting import add_receipt_with_items, list_transactions_by_period, to_excel_rows
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
    currency = State()
    pay_method = State()
    note = State()
    items_menu = State()
    item_category = State()
    item_name = State()
    item_qty = State()
    item_unit_price = State()
    item_note = State()
    item_delete = State()
    item_edit_select = State()
    item_edit_field = State()
    item_edit_value = State()
    receipt = State()
    confirm = State()


ITEM_CATEGORIES = ("VID", "TOKENS", "MUSHROOMS", "OTHER")

NAV_BACK_CANCEL = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отменить")]],
    resize_keyboard=True,
)
NAV_BACK_CANCEL_SKIP = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⏭ Пропустить")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отменить")],
    ],
    resize_keyboard=True,
)
ITEMS_MENU_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Добавить позицию")],
        [KeyboardButton(text="✏️ Изменить позицию"), KeyboardButton(text="🗑 Удалить позицию")],
        [KeyboardButton(text="➡️ К файлу")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отменить")],
    ],
    resize_keyboard=True,
)
CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="VID"), KeyboardButton(text="TOKENS")],
        [KeyboardButton(text="MUSHROOMS"), KeyboardButton(text="OTHER")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отменить")],
    ],
    resize_keyboard=True,
)
CONFIRM_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="✅ Сохранить")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отменить")],
    ],
    resize_keyboard=True,
)
EDIT_FIELD_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Категория"), KeyboardButton(text="Название")],
        [KeyboardButton(text="Количество"), KeyboardButton(text="Цена")],
        [KeyboardButton(text="Комментарий")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="❌ Отменить")],
    ],
    resize_keyboard=True,
)


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


def _is_cancel(text: str) -> bool:
    return text == "❌ Отменить"


def _is_back(text: str) -> bool:
    return text == "⬅️ Назад"


def _is_skip(text: str) -> bool:
    return text == "⏭ Пропустить"


def _parse_decimal(raw: str) -> Optional[Decimal]:
    try:
        return Decimal((raw or "").replace(",", ".").strip())
    except InvalidOperation:
        return None


def _calc_line(category: str, qty: Decimal, unit_price: Decimal) -> tuple[str, Decimal]:
    if category == "MUSHROOMS":
        return "per_1000", (qty / Decimal("1000")) * unit_price
    return "unit", qty * unit_price


def _items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Позиции пока не добавлены."
    lines = ["Позиции:"]
    for idx, item in enumerate(items, start=1):
        lines.append(
            f"{idx}. [{item['category']}] {item['item_name']} — qty: {item['qty']}, "
            f"unit_price: {item['unit_price']}, total: {item['line_total']} ({item['unit_basis']})"
        )
    return "\n".join(lines)


async def _cancel_add_check(message: Message, state: FSMContext) -> None:
    await state.clear()
    await safe_send_message(message.bot, message.chat.id, "Добавление чека отменено.", reply_markup=START_KEYBOARD)


async def _show_items_menu(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    items = data.get("items", [])
    await state.set_state(AddCheckFSM.items_menu)
    await safe_send_message(
        message.bot,
        message.chat.id,
        f"{_items_text(items)}\n\nВыберите действие с позициями:",
        reply_markup=ITEMS_MENU_KEYBOARD,
    )


async def _show_summary(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    items = data.get("items", [])
    total = sum((Decimal(item["line_total"]) for item in items), Decimal("0"))
    await state.set_state(AddCheckFSM.confirm)
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Проверьте чек перед сохранением:\n"
        f"Валюта: {data.get('currency', 'RUB')}\n"
        f"Способ оплаты: {data.get('pay_method') or '-'}\n"
        f"Комментарий: {data.get('note') or '-'}\n"
        f"Файл чека: {'есть' if data.get('receipt_file_id') else 'нет'}\n"
        f"Сумма по позициям: {total}\n\n"
        f"{_items_text(items)}",
        reply_markup=CONFIRM_KEYBOARD,
    )


@router.message(F.text == "🧾 Добавить чек")
async def start_add_check(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await state.clear()
    await state.update_data(items=[])
    await state.set_state(AddCheckFSM.currency)
    await safe_send_message(message.bot, message.chat.id, "Выберите валюту (например RUB, USD):", reply_markup=NAV_BACK_CANCEL)


@router.message(AddCheckFSM.currency)
async def add_check_currency(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await safe_send_message(message.bot, message.chat.id, "Это первый шаг.", reply_markup=NAV_BACK_CANCEL)
        return

    await state.update_data(currency=(text or "RUB").upper())
    await state.set_state(AddCheckFSM.pay_method)
    await safe_send_message(message.bot, message.chat.id, "Введите способ оплаты:", reply_markup=NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.pay_method)
async def add_check_pay_method(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.currency)
        await safe_send_message(message.bot, message.chat.id, "Выберите валюту:", reply_markup=NAV_BACK_CANCEL)
        return

    pay_method = None if _is_skip(text) or not text else text
    await state.update_data(pay_method=pay_method)
    await state.set_state(AddCheckFSM.note)
    await safe_send_message(message.bot, message.chat.id, "Введите комментарий к чеку:", reply_markup=NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.note)
async def add_check_note(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.pay_method)
        await safe_send_message(message.bot, message.chat.id, "Введите способ оплаты:", reply_markup=NAV_BACK_CANCEL_SKIP)
        return

    note = None if _is_skip(text) or not text else text
    await state.update_data(note=note)
    await _show_items_menu(message, state)


@router.message(AddCheckFSM.items_menu)
async def add_check_items_menu(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.note)
        await safe_send_message(message.bot, message.chat.id, "Введите комментарий к чеку:", reply_markup=NAV_BACK_CANCEL_SKIP)
        return
    if text == "➕ Добавить позицию":
        await state.update_data(item_draft={}, edit_index=None)
        await state.set_state(AddCheckFSM.item_category)
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию:", reply_markup=CATEGORY_KEYBOARD)
        return
    if text == "🗑 Удалить позицию":
        await state.set_state(AddCheckFSM.item_delete)
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции для удаления:", reply_markup=NAV_BACK_CANCEL)
        return
    if text == "✏️ Изменить позицию":
        await state.set_state(AddCheckFSM.item_edit_select)
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции для изменения:", reply_markup=NAV_BACK_CANCEL)
        return
    if text == "➡️ К файлу":
        await state.set_state(AddCheckFSM.receipt)
        await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ чека:", reply_markup=NAV_BACK_CANCEL_SKIP)
        return

    await safe_send_message(message.bot, message.chat.id, "Выберите действие кнопками.", reply_markup=ITEMS_MENU_KEYBOARD)


@router.message(AddCheckFSM.item_category)
async def add_check_item_category(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip().upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _show_items_menu(message, state)
        return
    if text not in ITEM_CATEGORIES:
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию из кнопок.", reply_markup=CATEGORY_KEYBOARD)
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    item_draft["category"] = text
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_name)
    await safe_send_message(message.bot, message.chat.id, "Введите название позиции:", reply_markup=NAV_BACK_CANCEL)


@router.message(AddCheckFSM.item_name)
async def add_check_item_name(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_category)
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию:", reply_markup=CATEGORY_KEYBOARD)
        return
    if not text:
        await safe_send_message(message.bot, message.chat.id, "Название не должно быть пустым.")
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    item_draft["item_name"] = text
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_qty)
    await safe_send_message(message.bot, message.chat.id, "Введите количество:", reply_markup=NAV_BACK_CANCEL)


@router.message(AddCheckFSM.item_qty)
async def add_check_item_qty(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_name)
        await safe_send_message(message.bot, message.chat.id, "Введите название позиции:", reply_markup=NAV_BACK_CANCEL)
        return
    qty = _parse_decimal(text)
    if qty is None:
        await safe_send_message(message.bot, message.chat.id, "Количество должно быть числом.")
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    item_draft["qty"] = str(qty)
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_unit_price)
    await safe_send_message(message.bot, message.chat.id, "Введите цену за единицу:", reply_markup=NAV_BACK_CANCEL)


@router.message(AddCheckFSM.item_unit_price)
async def add_check_item_unit_price(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_qty)
        await safe_send_message(message.bot, message.chat.id, "Введите количество:", reply_markup=NAV_BACK_CANCEL)
        return
    unit_price = _parse_decimal(text)
    if unit_price is None:
        await safe_send_message(message.bot, message.chat.id, "Цена должна быть числом.")
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    category = item_draft.get("category", "OTHER")
    qty = Decimal(item_draft["qty"])
    unit_basis, line_total = _calc_line(category, qty, unit_price)

    item_draft["unit_price"] = str(unit_price)
    item_draft["unit_basis"] = unit_basis
    item_draft["line_total"] = str(line_total)
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_note)
    await safe_send_message(message.bot, message.chat.id, "Комментарий к позиции:", reply_markup=NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.item_note)
async def add_check_item_note(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_unit_price)
        await safe_send_message(message.bot, message.chat.id, "Введите цену за единицу:", reply_markup=NAV_BACK_CANCEL)
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    item_draft["note"] = None if _is_skip(text) or not text else text
    items = data.get("items", [])

    edit_index = data.get("edit_index")
    if edit_index is None:
        items.append(item_draft)
    else:
        items[edit_index] = item_draft

    await state.update_data(items=items, item_draft=None, edit_index=None)
    await _show_items_menu(message, state)


@router.message(AddCheckFSM.item_delete)
async def add_check_item_delete(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _show_items_menu(message, state)
        return

    if not text.isdigit():
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции цифрой.")
        return

    data = await state.get_data()
    items = data.get("items", [])
    idx = int(text) - 1
    if idx < 0 or idx >= len(items):
        await safe_send_message(message.bot, message.chat.id, "Такой позиции нет.")
        return

    items.pop(idx)
    await state.update_data(items=items)
    await _show_items_menu(message, state)


@router.message(AddCheckFSM.item_edit_select)
async def add_check_item_edit_select(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _show_items_menu(message, state)
        return
    if not text.isdigit():
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции цифрой.")
        return

    data = await state.get_data()
    items = data.get("items", [])
    idx = int(text) - 1
    if idx < 0 or idx >= len(items):
        await safe_send_message(message.bot, message.chat.id, "Такой позиции нет.")
        return

    await state.update_data(edit_index=idx)
    await state.set_state(AddCheckFSM.item_edit_field)
    await safe_send_message(message.bot, message.chat.id, "Что изменить?", reply_markup=EDIT_FIELD_KEYBOARD)


@router.message(AddCheckFSM.item_edit_field)
async def add_check_item_edit_field(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_edit_select)
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции для изменения:", reply_markup=NAV_BACK_CANCEL)
        return

    field_map = {
        "Категория": "category",
        "Название": "item_name",
        "Количество": "qty",
        "Цена": "unit_price",
        "Комментарий": "note",
    }
    field = field_map.get(text)
    if field is None:
        await safe_send_message(message.bot, message.chat.id, "Выберите поле кнопками.", reply_markup=EDIT_FIELD_KEYBOARD)
        return

    await state.update_data(edit_field=field)
    await state.set_state(AddCheckFSM.item_edit_value)
    kb = CATEGORY_KEYBOARD if field == "category" else (NAV_BACK_CANCEL_SKIP if field == "note" else NAV_BACK_CANCEL)
    await safe_send_message(message.bot, message.chat.id, "Введите новое значение:", reply_markup=kb)


@router.message(AddCheckFSM.item_edit_value)
async def add_check_item_edit_value(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_edit_field)
        await safe_send_message(message.bot, message.chat.id, "Что изменить?", reply_markup=EDIT_FIELD_KEYBOARD)
        return

    data = await state.get_data()
    idx = data.get("edit_index")
    field = data.get("edit_field")
    items = data.get("items", [])
    if idx is None or field is None or idx < 0 or idx >= len(items):
        await _show_items_menu(message, state)
        return

    item = items[idx]
    if field == "category":
        val = text.upper()
        if val not in ITEM_CATEGORIES:
            await safe_send_message(message.bot, message.chat.id, "Выберите категорию из кнопок.", reply_markup=CATEGORY_KEYBOARD)
            return
        item[field] = val
    elif field in {"qty", "unit_price"}:
        dec = _parse_decimal(text)
        if dec is None:
            await safe_send_message(message.bot, message.chat.id, "Нужно число.")
            return
        item[field] = str(dec)
    elif field == "note":
        item[field] = None if _is_skip(text) or not text else text
    else:
        if not text:
            await safe_send_message(message.bot, message.chat.id, "Значение не должно быть пустым.")
            return
        item[field] = text

    qty = Decimal(item["qty"])
    unit_price = Decimal(item["unit_price"])
    unit_basis, line_total = _calc_line(item["category"], qty, unit_price)
    item["unit_basis"] = unit_basis
    item["line_total"] = str(line_total)

    items[idx] = item
    await state.update_data(items=items, edit_index=None, edit_field=None)
    await _show_items_menu(message, state)


@router.message(AddCheckFSM.receipt)
async def add_check_receipt(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _show_items_menu(message, state)
        return

    receipt_file_id: Optional[str] = None
    receipt_file_type: Optional[str] = None
    if not _is_skip(text):
        if message.photo:
            receipt_file_id = message.photo[-1].file_id
            receipt_file_type = "photo"
        elif message.document:
            receipt_file_id = message.document.file_id
            receipt_file_type = "document"
        else:
            await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ или нажмите «⏭ Пропустить».")
            return

    await state.update_data(receipt_file_id=receipt_file_id, receipt_file_type=receipt_file_type)
    await _show_summary(message, state)


@router.message(AddCheckFSM.confirm)
async def add_check_confirm(
    message: Message,
    state: FSMContext,
    pool: asyncpg.Pool,
    settings: Settings,
) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.receipt)
        await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ чека:", reply_markup=NAV_BACK_CANCEL_SKIP)
        return
    if text != "✅ Сохранить":
        await safe_send_message(message.bot, message.chat.id, "Подтвердите кнопкой «✅ Сохранить».", reply_markup=CONFIRM_KEYBOARD)
        return
    if not await _check_access(message, settings):
        await state.clear()
        return

    data = await state.get_data()
    items = data.get("items", [])
    if not items:
        await safe_send_message(message.bot, message.chat.id, "Добавьте хотя бы одну позицию перед сохранением.")
        await _show_items_menu(message, state)
        return

    saved = await add_receipt_with_items(
        pool,
        admin_id=int(message.from_user.id),
        currency=data.get("currency") or "RUB",
        pay_method=data.get("pay_method"),
        note=data.get("note"),
        receipt_file_id=data.get("receipt_file_id"),
        receipt_file_type=data.get("receipt_file_type"),
        items=items,
    )
    await state.clear()
    await safe_send_message(
        message.bot,
        message.chat.id,
        f"Чек сохранён (ID: {saved['receipt']['id']}). Позиций: {len(saved['items'])}.",
        reply_markup=START_KEYBOARD,
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