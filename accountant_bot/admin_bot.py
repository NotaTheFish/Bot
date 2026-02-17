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

from .accounting import add_receipt_with_items
from .config import Settings
from .db import cancel_receipt, get_receipt_with_items, list_receipts_by_period, refund_receipt
from .excel_export import build_transactions_report
from .reviews import ReviewsService
from .taboo import safe_send_document, safe_send_message

router = Router(name="admin")

NO_ACCESS_TEXT = "Нет доступа"

START_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика отзывов")],
        [KeyboardButton(text="🧾 Добавить чек")],
[KeyboardButton(text="🔍 Найти чек"), KeyboardButton(text="🧾 Последние чеки")],
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


class ReceiptLookupFSM(StatesGroup):
    wait_receipt_id = State()


ITEM_CATEGORIES = ("VID", "TOKENS", "MUSHROOMS", "OTHER")

BTN_BACK = "Назад"
BTN_CANCEL = "Отменить"
BTN_SKIP = "Пропустить"
BTN_SAVE = "Сохранить"
BTN_FIX = "Исправить"

NAV_BACK_CANCEL = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)]],
    resize_keyboard=True,
)
NAV_BACK_CANCEL_SKIP = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_SKIP)],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
ITEMS_MENU_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Добавить позицию")],
        [KeyboardButton(text=f"✏️ {BTN_FIX} позицию"), KeyboardButton(text="🗑 Удалить позицию")],
        [KeyboardButton(text="➡️ К файлу")],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="VID"), KeyboardButton(text="TOKENS")],
        [KeyboardButton(text="MUSHROOMS"), KeyboardButton(text="OTHER")],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
CONFIRM_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_SAVE)],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
EDIT_FIELD_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Категория"), KeyboardButton(text="Название товара")],
        [KeyboardButton(text="Количество"), KeyboardButton(text="Цена")],
        [KeyboardButton(text="Комментарий")],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
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
    return text in {BTN_CANCEL, "❌ Отменить"}


def _is_back(text: str) -> bool:
    return text in {BTN_BACK, "⬅️ Назад"}


def _is_skip(text: str) -> bool:
    return text in {BTN_SKIP, "⏭ Пропустить"}




def _unit_price_prompt(category: str) -> str:
    return "Цена за 1000 грибов" if category == "MUSHROOMS" else "Цена за 1"

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
            f"{idx}. [{item['category']}] {item['item_name']} — Количество: {item['qty']}, "
            f"{_unit_price_prompt(item['category'])}: {item['unit_price']}, Итог: {item['line_total']}"
        )
    return "\n".join(lines)


def _receipt_actions_keyboard(receipt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="❌ Отменить чек", callback_data=f"receipt:cancel:{receipt_id}"),
                InlineKeyboardButton(text="↩️ Возврат", callback_data=f"receipt:refund:{receipt_id}"),
            ]
        ]
    )


def _receipt_list_keyboard(rows: list[asyncpg.Record]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for row in rows:
        created_at = row.get("created_at")
        date_label = created_at.strftime("%d.%m") if created_at else "--.--"
        total = row.get("total") or Decimal("0")
        currency = row.get("currency") or "RUB"
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"#{row['id']} {date_label} {total} {currency}",
                    callback_data=f"receipt:open:{row['id']}",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _receipt_details_text(receipt: asyncpg.Record, items: list[asyncpg.Record]) -> str:
    total = sum((Decimal(str(item.get("line_total") or "0")) for item in items), Decimal("0"))
    lines = [
        f"🧾 Чек #{receipt['id']}",
        f"Дата: {receipt['created_at'].strftime('%d.%m.%Y %H:%M') if receipt.get('created_at') else '-'}",
        f"Статус: {receipt.get('status') or 'created'}",
        f"Валюта: {receipt.get('currency') or 'RUB'}",
        f"Способ оплаты: {receipt.get('pay_method') or '-'}",
        f"Комментарий: {receipt.get('note') or '-'}",
        f"Сумма: {total} {receipt.get('currency') or 'RUB'}",
        "",
        "Позиции:",
    ]
    if not items:
        lines.append("— Нет позиций")
    else:
        for idx, item in enumerate(items, start=1):
            lines.append(
                f"{idx}. [{item.get('category') or 'OTHER'}] {item.get('item_name') or '-'} — "
                f"Количество: {item.get('qty') or '0'}, "
                f"{_unit_price_prompt(item.get('category') or 'OTHER')}: {item.get('unit_price') or '0'}, "
                f"Итог: {item.get('line_total') or '0'}"
            )
            if item.get("note"):
                lines.append(f"   💬 {item['note']}")
    return "\n".join(lines)


async def _send_receipt_details(message: Message, pool: asyncpg.Pool, receipt_id: int) -> None:
    payload = await get_receipt_with_items(pool, receipt_id)
    if payload is None:
        await safe_send_message(message.bot, message.chat.id, "Чек не найден.")
        return

    receipt = payload["receipt"]
    items = payload["items"]
    await safe_send_message(
        message.bot,
        message.chat.id,
        _receipt_details_text(receipt, items),
        reply_markup=_receipt_actions_keyboard(int(receipt["id"])),
    )

    file_id = receipt.get("receipt_file_id")
    file_type = receipt.get("receipt_file_type")
    if file_id and file_type == "photo":
        await message.bot.send_photo(chat_id=message.chat.id, photo=file_id)
    elif file_id and file_type == "document":
        await message.bot.send_document(chat_id=message.chat.id, document=file_id)


async def _fetch_recent_receipts(pool: asyncpg.Pool, limit: int = 10) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                r.id,
                r.created_at,
                r.currency,
                COALESCE(SUM(ri.line_total), 0) AS total
            FROM receipts r
            LEFT JOIN receipt_items ri ON ri.receipt_id = r.id
            GROUP BY r.id, r.created_at, r.currency
            ORDER BY r.created_at DESC, r.id DESC
            LIMIT $1
            """,
            int(limit),
        )
    return list(rows)


@router.message(F.text == "🔍 Найти чек")
async def start_receipt_lookup(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await state.clear()
    await state.set_state(ReceiptLookupFSM.wait_receipt_id)
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Введите ID чека:",
        reply_markup=NAV_BACK_CANCEL,
    )


@router.message(ReceiptLookupFSM.wait_receipt_id)
async def process_receipt_lookup(message: Message, state: FSMContext, pool: asyncpg.Pool) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text) or _is_back(text):
        await state.clear()
        await safe_send_message(message.bot, message.chat.id, "Поиск чека завершён.", reply_markup=START_KEYBOARD)
        return
    if not text.isdigit():
        await safe_send_message(message.bot, message.chat.id, "ID должен быть числом.", reply_markup=NAV_BACK_CANCEL)
        return

    await _send_receipt_details(message, pool, int(text))
    await state.clear()
    await safe_send_message(message.bot, message.chat.id, "Выберите действие:", reply_markup=START_KEYBOARD)


@router.message(F.text == "🧾 Последние чеки")
async def show_recent_receipts(message: Message, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(message, settings):
        return
    rows = await _fetch_recent_receipts(pool, limit=10)
    if not rows:
        await safe_send_message(message.bot, message.chat.id, "Чеки пока отсутствуют.")
        return
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Последние чеки:",
        reply_markup=_receipt_list_keyboard(rows),
    )


@router.callback_query(F.data.startswith("receipt:open:"))
async def open_receipt_from_list(callback: CallbackQuery, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return

    receipt_id_raw = callback.data.split(":")[-1]
    if not receipt_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    await _send_receipt_details(callback.message, pool, int(receipt_id_raw))
    await callback.answer()


@router.callback_query(F.data.startswith("receipt:cancel:"))
async def cancel_receipt_action(callback: CallbackQuery, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return

    receipt_id_raw = callback.data.split(":")[-1]
    if not receipt_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return

    row = await cancel_receipt(pool, receipt_id=int(receipt_id_raw))
    if row is None:
        await callback.answer("Чек не найден", show_alert=True)
        return

    await safe_send_message(callback.message.bot, callback.message.chat.id, f"Чек #{receipt_id_raw} отменён.")
    await _send_receipt_details(callback.message, pool, int(receipt_id_raw))
    await callback.answer("Статус обновлён")


@router.callback_query(F.data.startswith("receipt:refund:"))
async def refund_receipt_action(callback: CallbackQuery, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return

    receipt_id_raw = callback.data.split(":")[-1]
    if not receipt_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return

    row = await refund_receipt(pool, receipt_id=int(receipt_id_raw))
    if row is None:
        await callback.answer("Чек не найден", show_alert=True)
        return

    await safe_send_message(callback.message.bot, callback.message.chat.id, f"Возврат по чеку #{receipt_id_raw} выполнен.")
    await _send_receipt_details(callback.message, pool, int(receipt_id_raw))
    await callback.answer("Статус обновлён")


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
        f"Итог по позициям: {total}\n\n"
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
    await safe_send_message(message.bot, message.chat.id, "Валюта (например RUB, USD):", reply_markup=NAV_BACK_CANCEL)


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
    await safe_send_message(message.bot, message.chat.id, "Способ оплаты:", reply_markup=NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.pay_method)
async def add_check_pay_method(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.currency)
        await safe_send_message(message.bot, message.chat.id, "Валюта:", reply_markup=NAV_BACK_CANCEL)
        return

    pay_method = None if _is_skip(text) or not text else text
    await state.update_data(pay_method=pay_method)
    await state.set_state(AddCheckFSM.note)
    await safe_send_message(message.bot, message.chat.id, "Комментарий к чеку:", reply_markup=NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.note)
async def add_check_note(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.pay_method)
        await safe_send_message(message.bot, message.chat.id, "Способ оплаты:", reply_markup=NAV_BACK_CANCEL_SKIP)
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
        await safe_send_message(message.bot, message.chat.id, "Комментарий к чеку:", reply_markup=NAV_BACK_CANCEL_SKIP)
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
    if text in {f"✏️ {BTN_FIX} позицию", "✏️ Изменить позицию"}:
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
    await safe_send_message(message.bot, message.chat.id, "Название товара:", reply_markup=NAV_BACK_CANCEL)


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
    await safe_send_message(message.bot, message.chat.id, "Количество:", reply_markup=NAV_BACK_CANCEL)


@router.message(AddCheckFSM.item_qty)
async def add_check_item_qty(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_name)
        await safe_send_message(message.bot, message.chat.id, "Название товара:", reply_markup=NAV_BACK_CANCEL)
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
    await safe_send_message(
        message.bot,
        message.chat.id,
        f"{_unit_price_prompt(item_draft.get('category', 'OTHER'))}:",
        reply_markup=NAV_BACK_CANCEL,
    )


@router.message(AddCheckFSM.item_unit_price)
async def add_check_item_unit_price(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_qty)
        await safe_send_message(message.bot, message.chat.id, "Количество:", reply_markup=NAV_BACK_CANCEL)
        return
    unit_price = _parse_decimal(text)
    if unit_price is None:
        await safe_send_message(
            message.bot,
            message.chat.id,
            f"Поле «{_unit_price_prompt(item_draft.get('category', 'OTHER'))}» должно быть числом.",
        )
        return

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
        data = await state.get_data()
        item_draft = data.get("item_draft", {})
        await state.set_state(AddCheckFSM.item_unit_price)
        await safe_send_message(
            message.bot,
            message.chat.id,
            f"{_unit_price_prompt(item_draft.get('category', 'OTHER'))}:",
            reply_markup=NAV_BACK_CANCEL,
        )
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
        "Название товара": "item_name",
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
            label = "Количество" if field == "qty" else _unit_price_prompt(item.get("category", "OTHER"))
            await safe_send_message(message.bot, message.chat.id, f"Поле «{label}» должно быть числом.")
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
            await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ или нажмите «Пропустить».")
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
    if text != BTN_SAVE:
        await safe_send_message(message.bot, message.chat.id, "Подтвердите сохранение кнопкой «Сохранить».", reply_markup=CONFIRM_KEYBOARD)
        return
    if not await _check_access(message, settings):
        await state.clear()
        return

    data = await state.get_data()
    items = data.get("items", [])
    if not items:
        await safe_send_message(message.bot, message.chat.id, "Добавьте хотя бы одну позицию в чек перед сохранением.")
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
        f"Чек сохранён (ID: {saved['receipt']['id']}). Позиции: {len(saved['items'])}, итог: {sum((Decimal(item['line_total']) for item in saved['items']), Decimal('0'))}.",
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
    receipts = await list_receipts_by_period(pool, period=period_for_filter)

    export_rows: list[dict[str, Any]] = []
    for receipt in receipts:
        receipt_data = dict(receipt)
        admin_id = int(receipt_data.get("admin_id") or 0)
        admin_signature = f"id: {admin_id}"
        try:
            chat = await callback.bot.get_chat(admin_id)
            if chat.username:
                admin_signature = f"@{chat.username} (id: {admin_id})"
            else:
                full_name = " ".join(part for part in [chat.first_name, chat.last_name] if part).strip()
                admin_signature = f"{full_name or 'Unknown'} (id: {admin_id})"
        except Exception:
            admin_signature = f"id: {admin_id}"

        payload = await get_receipt_with_items(pool, int(receipt_data["id"]))
        items = [] if payload is None else [dict(item) for item in payload["items"]]

        export_rows.append(
            {
                "receipt_id": receipt_data.get("id"),
                "created_at": receipt_data.get("created_at"),
                "admin": admin_signature,
                "currency": receipt_data.get("currency"),
                "pay_method": receipt_data.get("pay_method"),
                "total_sum": sum((Decimal(str(item.get("line_total") or "0")) for item in items), Decimal("0")),
                "note": receipt_data.get("note"),
                "receipt_file_id": receipt_data.get("receipt_file_id"),
                "status": receipt_data.get("status"),
                "items": items,
            }
        )

    report_bytes = build_transactions_report(export_rows)
    filename = f"transactions_{period}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    document = BufferedInputFile(report_bytes, filename=filename)

    if callback.message:
        await safe_send_document(
            callback.message.bot,
            callback.message.chat.id,
            document,
            caption=f"Выгрузка: {label_by_period[period]}",
        )

    await callback.answer("Файл отправлен")