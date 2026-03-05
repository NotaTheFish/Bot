from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional
from zoneinfo import ZoneInfo

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
from .category_labels import CATEGORY_LABELS
from .config import Settings
from .db import (
    add_product,
    cancel_receipt,
    count_products,
    count_search_products,
    delete_product,
    get_product,
    get_receipt_with_items,
    list_products,
    list_search_products,
    list_receipts_by_period,
    refund_receipt,
    search_products,
)
from .excel_export import build_transactions_report
from .reviews import ReviewsService
from .taboo import safe_send_document, safe_send_message
from .time_ranges import period_to_utc_range

router = Router(name="admin")

NO_ACCESS_TEXT = "Нет доступа"
PRODUCT_CATEGORIES = ("VID", "TOKENS", "OTHER", "DRAGONS", "POTIONS", "COINS")
PRODUCTS_PAGE_SIZE = 8

BTN_REVIEWS = "⭐ Отзывы"
BTN_REVIEWS_LEGACY = "📊 Статистика отзывов"
BTN_RECEIPTS_MENU = "🧾 Управление чеками"
BTN_ADD_RECEIPT = "➕ Создать чек"
BTN_ADD_RECEIPT_LEGACY = "🧾 Добавить чек"
BTN_EXPORT_EXCEL = "📤 Excel"
BTN_EXPORT_EXCEL_LEGACY = "📤 Выгрузить Excel"
BTN_FIND_RECEIPT = "🔎 Найти чек"
BTN_FIND_RECEIPT_LEGACY = "🔍 Найти чек"
BTN_RECENT_RECEIPTS = "🕘 Последние чеки"
BTN_PRODUCTS = "🧩 Управление товарами"
BTN_PRODUCTS_ADD = "➕ Добавить товар"
BTN_PRODUCTS_DELETE = "➖ Удалить товар"

START_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_REVIEWS), KeyboardButton(text=BTN_RECEIPTS_MENU)],
        [KeyboardButton(text=BTN_EXPORT_EXCEL), KeyboardButton(text=BTN_PRODUCTS)],
    ],
    resize_keyboard=True,
)

RECEIPTS_MENU_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_ADD_RECEIPT)],
        [KeyboardButton(text=BTN_FIND_RECEIPT)],
        [KeyboardButton(text=BTN_RECENT_RECEIPTS)],
        [KeyboardButton(text="⬅️ Назад")],
    ],
    resize_keyboard=True,
)

STATS_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Сегодня", callback_data="stats:day"),
            InlineKeyboardButton(text="📆 7 дней", callback_data="stats:week"),
            InlineKeyboardButton(text="🗓 30 дней", callback_data="stats:month"),
        ]
    ]
)

EXPORT_GAME_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="COS", callback_data="export:game:COS"),
            InlineKeyboardButton(text="DA", callback_data="export:game:DA"),
        ],
    ]
)


def export_period_keyboard(game_code: str) -> InlineKeyboardMarkup:
    normalized_game = (game_code or "").strip().upper()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Сегодня", callback_data=f"export:{normalized_game}:day"),
                InlineKeyboardButton(text="7 дней", callback_data=f"export:{normalized_game}:week"),
            ],
            [
                InlineKeyboardButton(text="30 дней", callback_data=f"export:{normalized_game}:month"),
                InlineKeyboardButton(text="Всё время", callback_data=f"export:{normalized_game}:all"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="export:back")],
        ]
    )


class AddCheckFSM(StatesGroup):
    game = State()
    currency = State()
    currency_custom = State()
    pay_method = State()
    pay_method_custom = State()
    note = State()
    items_menu = State()
    item_category = State()
    item_name = State()
    item_name_add_new = State()
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


class ReceiptMenuFSM(StatesGroup):
    menu = State()


class ProductCatalogFSM(StatesGroup):
    choose_game = State()
    menu = State()
    add_category = State()
    add_name = State()
    delete_category = State()
    delete_list = State()


GAME_CODES = ("COS", "DA")
GAME_ITEM_CATEGORIES: dict[str, tuple[str, ...]] = {
    "COS": ("VID", "TOKENS", "MUSHROOMS", "OTHER"),
    "DA": ("DRAGONS", "POTIONS", "COINS", "OTHER"),
}
CATEGORY_CODES_BY_LABEL = {label: code for code, label in CATEGORY_LABELS.items()}
PRODUCT_CATEGORY_BUTTONS_BY_GAME = {
    "COS": {
        "Виды": "VID",
        "Токены": "TOKENS",
        "Другое": "OTHER",
    },
    "DA": {
        "DRAGONS": "DRAGONS",
        "POTIONS": "POTIONS",
        "COINS": "COINS",
        "OTHER": "OTHER",
    },
}


def category_label(code: str) -> str:
    normalized_code = (code or "").strip()
    if normalized_code in CATEGORY_LABELS:
        return CATEGORY_LABELS[normalized_code]
    return normalized_code or "OTHER"

BTN_BACK = "Назад"
BTN_CANCEL = "Отменить"
BTN_SKIP = "Пропустить"
BTN_SAVE = "Сохранить"
BTN_FIX = "Исправить"

ADD_CHECK_NAV_PREFIX = "add_check:nav"

NAV_BACK = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=BTN_BACK)]],
    resize_keyboard=True,
)

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
def _items_menu_keyboard(items_count: int) -> ReplyKeyboardMarkup:
    if items_count == 0:
        keyboard = [
            [KeyboardButton(text="➕ Добавить позицию")],
            [KeyboardButton(text="❌ Отмена")],
        ]
    else:
        keyboard = [
            [KeyboardButton(text="➕ Добавить позицию")],
            [KeyboardButton(text="🗑 Удалить позицию")],
            [KeyboardButton(text="✏️ Изменить позицию")],
            [KeyboardButton(text="✅ К чеку")],
            [KeyboardButton(text="❌ Отмена")],
        ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=CATEGORY_LABELS["VID"]), KeyboardButton(text=CATEGORY_LABELS["TOKENS"])],
        [KeyboardButton(text=CATEGORY_LABELS["MUSHROOMS"]), KeyboardButton(text=CATEGORY_LABELS["OTHER"])],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
DA_CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="DRAGONS"), KeyboardButton(text="POTIONS")],
        [KeyboardButton(text="COINS"), KeyboardButton(text=CATEGORY_LABELS["OTHER"])],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
GAME_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="COS", callback_data="add_check:game:COS"),
            InlineKeyboardButton(text="DA", callback_data="add_check:game:DA"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"{ADD_CHECK_NAV_PREFIX}:cancel")],
    ]
)
PRODUCT_CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Виды"), KeyboardButton(text="Токены")],
        [KeyboardButton(text="Другое")],
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
    ],
    resize_keyboard=True,
)
PRODUCT_ADD_CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Виды"), KeyboardButton(text="Токены")],
        [KeyboardButton(text="Другое")],
        [KeyboardButton(text=BTN_BACK)],
    ],
    resize_keyboard=True,
)
PRODUCT_DELETE_CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Виды"), KeyboardButton(text="Токены")],
        [KeyboardButton(text="Другое")],
        [KeyboardButton(text=BTN_BACK)],
    ],
    resize_keyboard=True,
)
PRODUCT_GAME_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🍄 COS"), KeyboardButton(text="🐉 DA")],
        [KeyboardButton(text="⬅️ Назад")],
    ],
    resize_keyboard=True,
)
PRODUCTS_MENU_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_PRODUCTS_ADD)],
        [KeyboardButton(text=BTN_PRODUCTS_DELETE)],
        [KeyboardButton(text="⬅️ Назад")],
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

INLINE_NAV_BACK_CANCEL = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{ADD_CHECK_NAV_PREFIX}:back"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"{ADD_CHECK_NAV_PREFIX}:cancel"),
        ]
    ]
)

INLINE_NAV_BACK_CANCEL_SKIP = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"{ADD_CHECK_NAV_PREFIX}:skip")],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{ADD_CHECK_NAV_PREFIX}:back"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"{ADD_CHECK_NAV_PREFIX}:cancel"),
        ],
    ]
)

CURRENCY_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="₽ RUB", callback_data="add_check:currency:RUB"),
            InlineKeyboardButton(text="₴ UAH", callback_data="add_check:currency:UAH"),
        ],
        [
            InlineKeyboardButton(text="$ USD", callback_data="add_check:currency:USD"),
            InlineKeyboardButton(text="€ EUR", callback_data="add_check:currency:EUR"),
        ],
        [InlineKeyboardButton(text="✍️ Другое", callback_data="add_check:currency:other")],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{ADD_CHECK_NAV_PREFIX}:back"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"{ADD_CHECK_NAV_PREFIX}:cancel"),
        ],
    ]
)

PAY_METHOD_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="💳 Карта", callback_data="add_check:pay_method:card"),
            InlineKeyboardButton(text="💵 Наличные", callback_data="add_check:pay_method:cash"),
        ],
        [InlineKeyboardButton(text="🪙 Крипта", callback_data="add_check:pay_method:crypto")],
        [InlineKeyboardButton(text="✍️ Другое", callback_data="add_check:pay_method:other")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"{ADD_CHECK_NAV_PREFIX}:skip")],
        [
             InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{ADD_CHECK_NAV_PREFIX}:back"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"{ADD_CHECK_NAV_PREFIX}:cancel"),
        ],
    ]
)


def register_admin_handlers(dispatcher: Dispatcher) -> None:
    dispatcher.include_router(router)


def _is_admin(user_id: Optional[int], settings: Settings) -> bool:
    return user_id is not None and int(user_id) in set(settings.ACCOUNTANT_ADMIN_IDS)


def get_admin_tz(settings: Settings, admin_id: int) -> ZoneInfo:
    return ZoneInfo(settings.get_admin_timezone(int(admin_id)))


async def _check_access(event: Message | CallbackQuery, settings: Settings) -> bool:
    user = event.from_user
    if _is_admin(user.id if user else None, settings):
        return True

    if isinstance(event, Message):
        return False

    await event.answer()
    return False


@router.message(CommandStart())
async def handle_start(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await safe_send_message(message.bot, message.chat.id, "Привет! Выберите действие 👇", reply_markup=START_KEYBOARD)


@router.message((F.text == BTN_REVIEWS) | (F.text == BTN_REVIEWS_LEGACY))
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

    admin_id = int(callback.from_user.id)
    admin_tz = get_admin_tz(settings, admin_id)
    date_range = period_to_utc_range(period, tz=admin_tz)
    stats = await reviews_service.get_stats_reviews(date_range)

    await safe_send_message(
        callback.message.bot,
        callback.message.chat.id,
        f"📊 Статистика отзывов ({period_to_label[period]})\n"
        f"➕ Добавлено: {stats['added']}\n"
        f"➖ Удалено: {stats['deleted']}\n"
        f"✅ Активных: {stats['active']}\n"
        f"🌍 TZ: {admin_tz.key}",
    )
    await callback.answer()


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
    return text in {BTN_CANCEL, "❌ Отменить", "❌ Отмена"}


def _is_back(text: str) -> bool:
    return text in {BTN_BACK, "⬅️ Назад"}


def _is_skip(text: str) -> bool:
    return text in {BTN_SKIP, "⏭ Пропустить"}


def _is_dash_skip(text: str) -> bool:
    return text.strip() == "-"


NUMERIC_INPUT_ERROR_TEXT = "❌ Нужно число. Например: 10 или 10.5"


def _item_name_prompt() -> str:
    return "📝 Название товара (например: Revive Token) или «-» чтобы пропустить"


def _resolve_product_category(text: str, game_code: Optional[str]) -> Optional[str]:
    text_norm = (text or "").strip()
    text_upper = text_norm.upper()
    game = (game_code or "COS").upper()
    game_mapping = PRODUCT_CATEGORY_BUTTONS_BY_GAME.get(game, PRODUCT_CATEGORY_BUTTONS_BY_GAME["COS"])
    if text_upper in game_mapping.values():
        return text_upper
    return game_mapping.get(text_norm) or game_mapping.get(text_upper)


def _product_category_keyboard(game_code: Optional[str], *, with_cancel: bool) -> ReplyKeyboardMarkup:
    game = (game_code or "COS").upper()
    if game == "DA":
        rows = [
            [KeyboardButton(text="DRAGONS"), KeyboardButton(text="POTIONS")],
            [KeyboardButton(text="COINS"), KeyboardButton(text="OTHER")],
        ]
    else:
        rows = [
            [KeyboardButton(text="Виды"), KeyboardButton(text="Токены")],
            [KeyboardButton(text="Другое")],
        ]
    tail = [KeyboardButton(text=BTN_BACK)]
    if with_cancel:
        tail.append(KeyboardButton(text=BTN_CANCEL))
    rows.append(tail)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def _build_products_keyboard(
    *,
    category: str,
    products: list[dict[str, Any]],
    page: int,
    total_pages: int,
    include_add_button: bool,
    mode: str,
    include_reset_search: bool = False,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    callback_prefix = "prod:pick" if mode == "pick" else "prod:del"
    for idx in range(0, len(products), 2):
        pair = products[idx : idx + 2]
        rows.append(
            [
                InlineKeyboardButton(text=item["name"], callback_data=f"{callback_prefix}:{category}:{item['id']}")
                for item in pair
            ]
        )

    if include_add_button:
        rows.append([InlineKeyboardButton(text="➕ Добавить товар", callback_data=f"prod:add:{category}")])

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"prod:page:{category}:{page - 1}:{mode}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"prod:page:{category}:{page + 1}:{mode}"))
    if nav_row:
        rows.append(nav_row)

    if include_reset_search:
        rows.append([InlineKeyboardButton(text="❌ Сбросить поиск", callback_data="prod:del_reset_query")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_product_page(
    message: Message,
    *,
    pool: asyncpg.Pool,
    game_code: str,
    category: str,
    page: int,
    mode: str,
    query: Optional[str] = None,
) -> None:
    query_norm = " ".join((query or "").split())
    if mode == "delete" and query_norm:
        total = await count_search_products(pool, game_code, category, query_norm)
        total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
        current_page = min(max(1, page), total_pages)
        offset = (current_page - 1) * PRODUCTS_PAGE_SIZE
        products = await list_search_products(pool, game_code, category, query_norm, offset, PRODUCTS_PAGE_SIZE)
        if not products:
            await safe_send_message(
                message.bot,
                message.chat.id,
                f"По запросу «{query_norm}» ничего не найдено.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="❌ Сбросить поиск", callback_data="prod:del_reset_query")]]
                ),
            )
            return

        text = f"Результаты поиска «{query_norm}» (Страница {current_page} из {total_pages})"
        kb = _build_products_keyboard(
            category=category,
            products=products,
            page=current_page,
            total_pages=total_pages,
            include_add_button=False,
            mode=mode,
            include_reset_search=True,
        )
        await safe_send_message(message.bot, message.chat.id, text, reply_markup=kb)
        return

    total = await count_products(pool, game_code, category)
    total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
    current_page = min(max(1, page), total_pages)
    offset = (current_page - 1) * PRODUCTS_PAGE_SIZE
    products = await list_products(pool, game_code, category, offset, PRODUCTS_PAGE_SIZE)
    if not products:
        if mode == "delete":
            text = "В категории нет товаров"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="prod:del_back")]]
            )
        else:
            text = "Список товаров пуст."
            kb = _build_products_keyboard(
                category=category,
                products=[],
                page=1,
                total_pages=1,
                include_add_button=True,
                mode=mode,
            )
        await safe_send_message(message.bot, message.chat.id, text, reply_markup=kb)
        return

    text = "Выберите товар" if mode == "pick" else "Выберите товар для удаления"
    text = f"{text} (Страница {current_page} из {total_pages})"
    kb = _build_products_keyboard(
        category=category,
        products=products,
        page=current_page,
        total_pages=total_pages,
        include_add_button=(mode == "pick"),
        mode=mode,
    )
    await safe_send_message(message.bot, message.chat.id, text, reply_markup=kb)


def _trim_product_name(value: str, max_len: int = 80) -> str:
    return (value or "").strip()[:max_len]


def _item_qty_prompt(category: str = "OTHER") -> str:
    if category == "COINS":
        return "🔢 Введите количество коинов (в 100k, например 1 или 2.5)"
    return "🔢 Количество (число)"


def _item_unit_price_prompt(category: str) -> str:
    if category == "MUSHROOMS":
        return "💰 Цена за 1000 грибов (число)"
    return "💰 Цена за 1 шт (число)"
    if category == "COINS":
        return "💰 Цена за 100000 коинов (число)"


def _item_edit_value_prompt(field: str, item: dict[str, Any]) -> str:
    if field == "item_name":
        return _item_name_prompt()
    if field == "qty":
        return _item_qty_prompt(item.get("category", "OTHER"))
    if field == "unit_price":
        return _item_unit_price_prompt(item.get("category", "OTHER"))
    return "Введите новое значение:"




def _unit_price_prompt(category: str) -> str:
    if category == "MUSHROOMS":
        return "Цена за 1000 грибов"
    if category == "COINS":
        return "Цена за 100000 коинов"
    return "Цена за 1"

def _parse_decimal(raw: str) -> Optional[Decimal]:
    try:
        return Decimal((raw or "").replace(",", ".").strip())
    except InvalidOperation:
        return None


def _calc_line(category: str, qty: Decimal, unit_price: Decimal) -> tuple[str, Decimal]:
    if category == "MUSHROOMS":
        return "per_1000", (qty / Decimal("1000")) * unit_price
    if category == "COINS":
        return "per_100000", qty * unit_price
    return "unit", qty * unit_price


def _parse_coins_qty(raw: str) -> Optional[Decimal]:
    normalized = (raw or "").strip().lower().replace(",", ".")
    if not normalized:
        return None
    multiplier = Decimal("1")
    if normalized.endswith("k"):
        multiplier = Decimal("1000")
        normalized = normalized[:-1]
    elif normalized.endswith("m"):
        multiplier = Decimal("1000000")
        normalized = normalized[:-1]
    try:
        coins = Decimal(normalized.strip()) * multiplier
    except InvalidOperation:
        return None
    return coins / Decimal("100000")


def _items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "🧾 Позиции пока не добавлены. Нажмите ➕ «Добавить позицию»."
    lines = ["Позиции:"]
    for idx, item in enumerate(items, start=1):
        category_code = item.get("category") or "OTHER"
        lines.append(
            f"{idx}. [{category_label(category_code)}] {item['item_name']} — Количество: {item['qty']}, "
            f"{_unit_price_prompt(category_code)}: {item['unit_price']}, Итог: {item['line_total']}"
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
            category_code = item.get("category") or "OTHER"
            lines.append(
                f"{idx}. [{category_label(category_code)}] {item.get('item_name') or '-'} — "
                f"Количество: {item.get('qty') or '0'}, "
                f"{_unit_price_prompt(category_code)}: {item.get('unit_price') or '0'}, "
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


@router.message(F.text == BTN_RECEIPTS_MENU)
async def receipts_menu(message: Message, settings: Settings, state: FSMContext) -> None:
    if not await _check_access(message, settings):
        return
    await state.clear()
    await state.set_state(ReceiptMenuFSM.menu)
    await safe_send_message(message.bot, message.chat.id, "Управление чеками:", reply_markup=RECEIPTS_MENU_KEYBOARD)


@router.message(ReceiptMenuFSM.menu, (F.text == BTN_FIND_RECEIPT) | (F.text == BTN_FIND_RECEIPT_LEGACY))
async def start_receipt_lookup(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
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
        await state.set_state(ReceiptMenuFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Поиск чека завершён.", reply_markup=RECEIPTS_MENU_KEYBOARD)
        return
    if not text.isdigit():
        await safe_send_message(message.bot, message.chat.id, "ID должен быть числом.", reply_markup=NAV_BACK)
        return

    await _send_receipt_details(message, pool, int(text))
    await state.set_state(ReceiptMenuFSM.menu)
    await safe_send_message(message.bot, message.chat.id, "Выберите действие:", reply_markup=RECEIPTS_MENU_KEYBOARD)


@router.message(ReceiptMenuFSM.menu, F.text == BTN_RECENT_RECEIPTS)
async def show_recent_receipts(message: Message, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(message, settings):
        return
    rows = await _fetch_recent_receipts(pool, limit=10)
    if not rows:
        await safe_send_message(message.bot, message.chat.id, "Чеки пока отсутствуют.", reply_markup=RECEIPTS_MENU_KEYBOARD)
        return
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Последние чеки:",
        reply_markup=_receipt_list_keyboard(rows),
    )




@router.message(ReceiptMenuFSM.menu)
async def receipts_menu_actions(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    if text == "⬅️ Назад" or _is_back(text) or _is_cancel(text):
        await state.clear()
        await safe_send_message(message.bot, message.chat.id, "Возврат в меню.", reply_markup=START_KEYBOARD)
        return
    if text == BTN_ADD_RECEIPT or text == BTN_ADD_RECEIPT_LEGACY:
        await state.clear()
        await state.update_data(items=[])
        await _prompt_game(message, state=state)
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите действие кнопками.", reply_markup=RECEIPTS_MENU_KEYBOARD)

@router.message(F.text == BTN_PRODUCTS)
async def products_menu(message: Message, settings: Settings, state: FSMContext) -> None:
    if not await _check_access(message, settings):
        return
    await state.clear()
    await state.set_state(ProductCatalogFSM.choose_game)
    await safe_send_message(message.bot, message.chat.id, "Выберите игру:", reply_markup=PRODUCT_GAME_KEYBOARD)


@router.message(ProductCatalogFSM.choose_game)
async def products_choose_game(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    if text == "⬅️ Назад" or _is_back(text) or _is_cancel(text):
        await state.clear()
        await safe_send_message(message.bot, message.chat.id, "Возврат в меню.", reply_markup=START_KEYBOARD)
        return
    mapping = {"🍄": "COS", "🍄 COS": "COS", "COS": "COS", "🐉": "DA", "🐉 DA": "DA", "DA": "DA"}
    game_code = mapping.get(text)
    if game_code is None:
        await safe_send_message(message.bot, message.chat.id, "Выберите игру кнопками.", reply_markup=PRODUCT_GAME_KEYBOARD)
        return
    await state.update_data(game_code=game_code, product_category=None, product_delete_query=None, product_delete_page=1)
    await state.set_state(ProductCatalogFSM.menu)
    await safe_send_message(message.bot, message.chat.id, f"Управление товарами ({game_code}):", reply_markup=PRODUCTS_MENU_KEYBOARD)


@router.message(ProductCatalogFSM.menu)
async def products_menu_actions(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    data = await state.get_data()
    if text == "⬅️ Назад" or _is_back(text) or _is_cancel(text):
        await state.set_state(ProductCatalogFSM.choose_game)
        await safe_send_message(message.bot, message.chat.id, "Выберите игру:", reply_markup=PRODUCT_GAME_KEYBOARD)
        return
    if text == BTN_PRODUCTS_ADD:
        await state.set_state(ProductCatalogFSM.add_category)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Выберите категорию:",
            reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True),
        )
        return
    if text == BTN_PRODUCTS_DELETE:
        await state.update_data(product_delete_query=None, product_delete_page=1)
        await state.set_state(ProductCatalogFSM.delete_category)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Выберите категорию:",
            reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True),
        )
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите действие кнопками.", reply_markup=PRODUCTS_MENU_KEYBOARD)


@router.message(ProductCatalogFSM.add_category)
async def products_add_category(message: Message, state: FSMContext, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await state.set_state(ProductCatalogFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Отменено.", reply_markup=PRODUCTS_MENU_KEYBOARD)
        return
    if _is_back(text):
        await state.set_state(ProductCatalogFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Управление товарами:", reply_markup=PRODUCTS_MENU_KEYBOARD)
        return
        category = _resolve_product_category(text, game_code)
    if category is None:
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию из кнопок.", reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True))
        return
    await state.update_data(product_category=category)
    await state.set_state(ProductCatalogFSM.add_name)
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Введите название товара (желательно по-английски). Например: Revive Token",
        reply_markup=NAV_BACK_CANCEL,
    )


@router.message(ProductCatalogFSM.add_name)
async def products_add_name(message: Message, state: FSMContext, pool: asyncpg.Pool, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await state.set_state(ProductCatalogFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Отменено.", reply_markup=PRODUCTS_MENU_KEYBOARD)
        return
    if _is_back(text):
        await state.set_state(ProductCatalogFSM.add_category)
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию:", reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True))
        return
    clean_name = _trim_product_name(text)
    if not clean_name:
        await safe_send_message(message.bot, message.chat.id, "Название не должно быть пустым.")
        return
    category = data.get("product_category")
    if category not in _valid_item_categories(data.get("game_code")):
        await state.set_state(ProductCatalogFSM.add_category)
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию:", reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True))
        return
    product = await add_product(pool, game_code, category, clean_name)
    await state.set_state(ProductCatalogFSM.menu)
    await safe_send_message(
        message.bot,
        message.chat.id,
        f"✅ Добавлено: {product['name']} ({category_label(category)})",
        reply_markup=PRODUCTS_MENU_KEYBOARD,
    )


@router.message(ProductCatalogFSM.delete_category)
async def products_delete_category(message: Message, state: FSMContext, pool: asyncpg.Pool, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await state.set_state(ProductCatalogFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Отменено.", reply_markup=PRODUCTS_MENU_KEYBOARD)
        return
    if _is_back(text):
        await state.set_state(ProductCatalogFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Управление товарами:", reply_markup=PRODUCTS_MENU_KEYBOARD)
        return
    category = _resolve_product_category(text, game_code)
    if category is None:
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию из кнопок.", reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True))
        return
    await state.update_data(product_category=category, product_delete_page=1, product_delete_query=None)
    await state.set_state(ProductCatalogFSM.delete_list)
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Введите часть названия для поиска или выберите товар кнопками. «Назад» — категории, «Отменить» — в меню.",
        reply_markup=NAV_BACK_CANCEL,
    )
    await _send_product_page(message, pool=pool, game_code=game_code, category=category, page=1, mode="delete")


@router.message(ProductCatalogFSM.delete_list)
async def products_delete_list(message: Message, state: FSMContext, pool: asyncpg.Pool, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await state.set_state(ProductCatalogFSM.menu)
        await safe_send_message(message.bot, message.chat.id, "Отменено.", reply_markup=PRODUCTS_MENU_KEYBOARD)
        return
    if _is_back(text):
        await state.set_state(ProductCatalogFSM.delete_category)
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию:", reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True))
        return
    category = data.get("product_category")
    if category not in _valid_item_categories(data.get("game_code")):
        await state.set_state(ProductCatalogFSM.delete_category)
        await safe_send_message(message.bot, message.chat.id, "Выберите категорию:", reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True))
        return
    query = " ".join(text.split())
    if len(query) < 2:
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Введите минимум 2 символа для поиска или выберите товар кнопками.",
            reply_markup=NAV_BACK_CANCEL,
        )
        return
    await state.update_data(product_delete_query=query, product_delete_page=1)
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Введите часть названия для поиска или выберите товар кнопками. «Назад» — категории, «Отменить» — в меню.",
        reply_markup=NAV_BACK_CANCEL,
    )
    await _send_product_page(message, pool=pool, game_code=game_code, category=category, page=1, mode="delete", query=query)


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


@router.callback_query(F.data.startswith("prod:page:"))
async def products_page_callback(callback: CallbackQuery, settings: Settings, state: FSMContext, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    parts = (callback.data or "").split(":")
    if len(parts) != 5:
        await callback.answer()
        return
    _, _, category, page_raw, mode = parts
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if category not in _valid_item_categories(game_code) or not page_raw.isdigit() or mode not in {"pick", "delete"}:
        await callback.answer()
        return
    page = int(page_raw)
    if mode == "delete":
        query = " ".join(str(data.get("product_delete_query") or "").split())
        if query:
            total = await count_search_products(pool, game_code, category, query)
            total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
            current_page = min(max(1, page), total_pages)
            products = await list_search_products(pool, game_code, category, query, (current_page - 1) * PRODUCTS_PAGE_SIZE, PRODUCTS_PAGE_SIZE)
            text = f"Результаты поиска «{query}» (Страница {current_page} из {total_pages})"
            kb = _build_products_keyboard(
                category=category,
                products=products,
                page=current_page,
                total_pages=total_pages,
                include_add_button=False,
                mode=mode,
                include_reset_search=True,
            )
            await callback.message.edit_text(text, reply_markup=kb)
            await state.update_data(product_category=category, product_delete_page=current_page)
            await callback.answer()
            return

    total = await count_products(pool, game_code, category)
    total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
    current_page = min(max(1, page), total_pages)
    products = await list_products(pool, game_code, category, (current_page - 1) * PRODUCTS_PAGE_SIZE, PRODUCTS_PAGE_SIZE)
    text = ("Выберите товар" if mode == "pick" else "Выберите товар для удаления") + f" (Страница {current_page} из {total_pages})"
    kb = _build_products_keyboard(
        category=category,
        products=products,
        page=current_page,
        total_pages=total_pages,
        include_add_button=(mode == "pick"),
        mode=mode,
    )
    await callback.message.edit_text(text, reply_markup=kb)
    if mode == "delete":
        await state.update_data(product_category=category, product_delete_page=current_page)
    await callback.answer()


@router.callback_query(F.data.startswith("prod:add:"))
async def add_check_product_add_callback(callback: CallbackQuery, settings: Settings, state: FSMContext) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    parts = (callback.data or "").split(":")
    if len(parts) != 3:
        await callback.answer()
        return
    category = parts[2]
    data = await state.get_data()
    if category not in _valid_item_categories(data.get("game_code")):
        await callback.answer()
        return
    item_draft = data.get("item_draft", {})
    item_draft["category"] = category
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_name_add_new)
    await safe_send_message(callback.message.bot, callback.message.chat.id, "Введите название нового товара", reply_markup=NAV_BACK_CANCEL)
    await callback.answer()


@router.callback_query(F.data.startswith("prod:pick:"))
async def add_check_product_pick_callback(callback: CallbackQuery, settings: Settings, state: FSMContext, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    parts = (callback.data or "").split(":")
    if len(parts) != 4 or not parts[3].isdigit():
        await callback.answer()
        return
    _, _, category, product_id_raw = parts
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    product = await get_product(pool, game_code, category, int(product_id_raw))
    if not product:
        await callback.answer("Товар не найден", show_alert=True)
        return
    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    item_draft["item_name"] = product["name"]
    item_draft["category"] = category
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_qty)
    await safe_send_message(callback.message.bot, callback.message.chat.id, _item_qty_prompt(item_draft.get("category", "OTHER")), reply_markup=INLINE_NAV_BACK_CANCEL)
    await callback.answer()


@router.callback_query(F.data.startswith("prod:del:"))
async def products_delete_pick_callback(callback: CallbackQuery, settings: Settings, state: FSMContext, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    parts = (callback.data or "").split(":")
    if len(parts) != 4 or not parts[3].isdigit():
        await callback.answer()
        return
    _, _, category, product_id_raw = parts
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    product = await get_product(pool, game_code, category, int(product_id_raw))
    if not product:
        await callback.answer("Товар не найден", show_alert=True)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data=f"prod:del_confirm:{category}:{product['id']}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"prod:del_cancel:{category}:{product['id']}"),
            ]
        ]
    )
    await callback.message.edit_text(f"Удалить товар '{product['name']}'?", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("prod:del_cancel:"))
async def products_delete_cancel_callback(callback: CallbackQuery, settings: Settings, state: FSMContext, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    category = data.get("product_category", "OTHER")
    page = int(data.get("product_delete_page") or 1)
    query = " ".join(str(data.get("product_delete_query") or "").split())
    if category not in _valid_item_categories(data.get("game_code")):
        await callback.answer()
        return

    if query:
        total = await count_search_products(pool, game_code, category, query)
        if total == 0:
            await callback.message.edit_text(
                f"По запросу «{query}» ничего не найдено.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="❌ Сбросить поиск", callback_data="prod:del_reset_query")]]
                ),
            )
            await callback.answer()
            return
        total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
        page = min(max(1, page), total_pages)
        products = await list_search_products(pool, game_code, category, query, (page - 1) * PRODUCTS_PAGE_SIZE, PRODUCTS_PAGE_SIZE)
        await callback.message.edit_text(
            f"Результаты поиска «{query}» (Страница {page} из {total_pages})",
            reply_markup=_build_products_keyboard(
                category=category,
                products=products,
                page=page,
                total_pages=total_pages,
                include_add_button=False,
                mode="delete",
                include_reset_search=True,
            ),
        )
        await callback.answer()
        return

    total = await count_products(pool, game_code, category)
    if total == 0:
        await callback.message.edit_text(
            "В категории нет товаров",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="prod:del_back")]]
            ),
        )
        await callback.answer()
        return
    total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
    page = min(max(1, page), total_pages)
    products = await list_products(pool, game_code, category, (page - 1) * PRODUCTS_PAGE_SIZE, PRODUCTS_PAGE_SIZE)
    await callback.message.edit_text(
        f"Выберите товар для удаления (Страница {page} из {total_pages})",
        reply_markup=_build_products_keyboard(
            category=category,
            products=products,
            page=page,
            total_pages=total_pages,
            include_add_button=False,
            mode="delete",
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("prod:del_confirm:"))
async def products_delete_confirm_callback(callback: CallbackQuery, settings: Settings, state: FSMContext, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    parts = (callback.data or "").split(":")
    if len(parts) != 4 or not parts[3].isdigit():
        await callback.answer()
        return
    _, _, category, product_id_raw = parts
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    await delete_product(pool, game_code, category, int(product_id_raw))
    page = int(data.get("product_delete_page") or 1)
    query = " ".join(str(data.get("product_delete_query") or "").split())

    if query:
        total = await count_search_products(pool, game_code, category, query)
        if total == 0:
            await callback.message.edit_text(
                "✅ Удалено\n\nПо запросу ничего не найдено.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="❌ Сбросить поиск", callback_data="prod:del_reset_query")]]
                ),
            )
            await callback.answer()
            return
        total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
        page = min(max(1, page), total_pages)
        products = await list_search_products(pool, game_code, category, query, (page - 1) * PRODUCTS_PAGE_SIZE, PRODUCTS_PAGE_SIZE)
        await callback.message.edit_text(
            f"✅ Удалено\n\nРезультаты поиска «{query}» (Страница {page} из {total_pages})",
            reply_markup=_build_products_keyboard(
                category=category,
                products=products,
                page=page,
                total_pages=total_pages,
                include_add_button=False,
                mode="delete",
                include_reset_search=True,
            ),
        )
        await callback.answer()
        return

    total = await count_products(pool, game_code, category)
    if total == 0:
        await callback.message.edit_text(
            "✅ Удалено\n\nВ категории нет товаров",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="prod:del_back")]]
            ),
        )
        await callback.answer()
        return
    total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
    page = min(max(1, page), total_pages)
    products = await list_products(pool, game_code, category, (page - 1) * PRODUCTS_PAGE_SIZE, PRODUCTS_PAGE_SIZE)
    await callback.message.edit_text(
        f"✅ Удалено\n\nВыберите товар для удаления (Страница {page} из {total_pages})",
        reply_markup=_build_products_keyboard(
            category=category,
            products=products,
            page=page,
            total_pages=total_pages,
            include_add_button=False,
            mode="delete",
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "prod:del_reset_query")
async def products_delete_reset_query_callback(callback: CallbackQuery, settings: Settings, state: FSMContext, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    category = data.get("product_category")
    if category not in _valid_item_categories(data.get("game_code")):
        await callback.answer()
        return
    await state.update_data(product_delete_query=None, product_delete_page=1)
    total = await count_products(pool, game_code, category)
    if total == 0:
        await callback.message.edit_text(
            "В категории нет товаров",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="prod:del_back")]]
            ),
        )
        await callback.answer("Поиск сброшен")
        return
    total_pages = max(1, (total + PRODUCTS_PAGE_SIZE - 1) // PRODUCTS_PAGE_SIZE)
    products = await list_products(pool, game_code, category, 0, PRODUCTS_PAGE_SIZE)
    await callback.message.edit_text(
        f"Выберите товар для удаления (Страница 1 из {total_pages})",
        reply_markup=_build_products_keyboard(
            category=category,
            products=products,
            page=1,
            total_pages=total_pages,
            include_add_button=False,
            mode="delete",
        ),
    )
    await callback.answer("Поиск сброшен")


@router.callback_query(F.data == "prod:del_back")
async def products_delete_back_callback(callback: CallbackQuery, settings: Settings, state: FSMContext) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return
    data = await state.get_data()
    await state.update_data(product_delete_query=None, product_delete_page=1)
    await state.set_state(ProductCatalogFSM.delete_category)
    await safe_send_message(
        callback.message.bot,
        callback.message.chat.id,
        "Выберите категорию:",
        reply_markup=_product_category_keyboard(data.get("game_code"), with_cancel=True),
    )
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
    await state.set_state(ReceiptMenuFSM.menu)
    await safe_send_message(message.bot, message.chat.id, "Добавление чека отменено.", reply_markup=RECEIPTS_MENU_KEYBOARD)


ADD_CHECK_PREV_STATE: dict[str, State | None] = {
    AddCheckFSM.game.state: None,
    AddCheckFSM.currency.state: AddCheckFSM.game,
    AddCheckFSM.currency_custom.state: AddCheckFSM.currency,
    AddCheckFSM.pay_method.state: AddCheckFSM.currency,
    AddCheckFSM.pay_method_custom.state: AddCheckFSM.pay_method,
    AddCheckFSM.note.state: AddCheckFSM.pay_method,
    AddCheckFSM.items_menu.state: AddCheckFSM.note,
    AddCheckFSM.item_category.state: AddCheckFSM.items_menu,
    AddCheckFSM.item_name.state: AddCheckFSM.item_category,
    AddCheckFSM.item_name_add_new.state: AddCheckFSM.item_name,
    AddCheckFSM.item_qty.state: AddCheckFSM.item_name,
    AddCheckFSM.item_unit_price.state: AddCheckFSM.item_qty,
    AddCheckFSM.item_note.state: AddCheckFSM.item_unit_price,
    AddCheckFSM.item_delete.state: AddCheckFSM.items_menu,
    AddCheckFSM.item_edit_select.state: AddCheckFSM.items_menu,
    AddCheckFSM.item_edit_field.state: AddCheckFSM.item_edit_select,
    AddCheckFSM.item_edit_value.state: AddCheckFSM.item_edit_field,
    AddCheckFSM.receipt.state: AddCheckFSM.items_menu,
    AddCheckFSM.confirm.state: AddCheckFSM.receipt,
}

def _item_category_keyboard(game_code: Optional[str]) -> ReplyKeyboardMarkup:
    return DA_CATEGORY_KEYBOARD if (game_code or "").upper() == "DA" else CATEGORY_KEYBOARD


def _valid_item_categories(game_code: Optional[str]) -> tuple[str, ...]:
    return GAME_ITEM_CATEGORIES.get((game_code or "").upper(), GAME_ITEM_CATEGORIES["COS"])


async def _prompt_item_name(message: Message, state: FSMContext, pool: Optional[asyncpg.Pool] = None) -> None:
    data = await state.get_data()
    category = (data.get("item_draft", {}) or {}).get("category", "OTHER")
    await state.set_state(AddCheckFSM.item_name)
    if pool is not None and category in PRODUCT_CATEGORIES:
        await _send_product_page(message, pool=pool, game_code=game_code, category=category, page=1, mode="pick")
        return
    await safe_send_message(message.bot, message.chat.id, _item_name_prompt(), reply_markup=INLINE_NAV_BACK_CANCEL)



async def _render_add_check_state(
    message: Message,
    state: FSMContext,
    target_state: State | None,
    pool: Optional[asyncpg.Pool] = None,
) -> None:
    if target_state is None:
        await safe_send_message(message.bot, message.chat.id, "Это первый шаг.", reply_markup=GAME_KEYBOARD)
        return

    if target_state == AddCheckFSM.game:
        await _prompt_game(message, state=state)
        return
    if target_state == AddCheckFSM.currency:
        await _prompt_currency(message, state=state)
        return
    if target_state == AddCheckFSM.pay_method:
        await _prompt_pay_method(message, state=state)
        return
    if target_state == AddCheckFSM.note:
        await state.set_state(AddCheckFSM.note)
        await safe_send_message(message.bot, message.chat.id, "Комментарий к чеку:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
        return
    if target_state == AddCheckFSM.items_menu:
        await _show_items_menu(message, state)
        return
    if target_state == AddCheckFSM.item_category:
        data = await state.get_data()
        await state.set_state(AddCheckFSM.item_category)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Выберите категорию:",
            reply_markup=_item_category_keyboard(data.get("game_code")),
        )
        return
    if target_state == AddCheckFSM.item_name:
        await _prompt_item_name(message, state, pool)
        return
    if target_state == AddCheckFSM.item_qty:
        data = await state.get_data()
        await state.set_state(AddCheckFSM.item_qty)
        await safe_send_message(
            message.bot,
            message.chat.id,
            _item_qty_prompt(data.get("item_draft", {}).get("category", "OTHER")),
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
        return
    if target_state == AddCheckFSM.item_unit_price:
        data = await state.get_data()
        item_draft = data.get("item_draft", {})
        category = item_draft.get("category", "OTHER")
        await state.set_state(AddCheckFSM.item_unit_price)
        await safe_send_message(message.bot, message.chat.id, _item_unit_price_prompt(category), reply_markup=INLINE_NAV_BACK_CANCEL)
        return
    if target_state == AddCheckFSM.item_note:
        await state.set_state(AddCheckFSM.item_note)
        await safe_send_message(message.bot, message.chat.id, "Комментарий к позиции:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
        return
    if target_state == AddCheckFSM.item_delete:
        await state.set_state(AddCheckFSM.item_delete)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Введите номер позиции для удаления:",
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
        return
    if target_state == AddCheckFSM.item_edit_select:
        await state.set_state(AddCheckFSM.item_edit_select)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Введите номер позиции для изменения:",
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
        return
    if target_state == AddCheckFSM.item_edit_field:
        await state.set_state(AddCheckFSM.item_edit_field)
        await safe_send_message(message.bot, message.chat.id, "Что изменить?", reply_markup=EDIT_FIELD_KEYBOARD)
        return
    if target_state == AddCheckFSM.item_edit_value:
        data = await state.get_data()
        edit_field = data.get("edit_field")
        edit_index = data.get("edit_index")
        items = data.get("items", [])
        item = items[edit_index] if edit_index is not None and 0 <= edit_index < len(items) else {}
        kb = (
            _item_category_keyboard(data.get("game_code"))
            if edit_field == "category"
            else (INLINE_NAV_BACK_CANCEL_SKIP if edit_field == "note" else INLINE_NAV_BACK_CANCEL)
        )
        await state.set_state(AddCheckFSM.item_edit_value)
        await safe_send_message(
            message.bot,
            message.chat.id,
            _item_edit_value_prompt(edit_field or "", item),
            reply_markup=kb,
        )
        return
    if target_state == AddCheckFSM.receipt:
        await state.set_state(AddCheckFSM.receipt)
        await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ чека:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
        return
    if target_state == AddCheckFSM.confirm:
        await _show_summary(message, state)


@router.callback_query(F.data.startswith(f"{ADD_CHECK_NAV_PREFIX}:"))
async def add_check_navigation_callback(
    callback: CallbackQuery,
    state: FSMContext,
    pool: asyncpg.Pool,
    settings: Settings,
) -> None:
    if not await _check_access(callback, settings):
        return
    if callback.message is None:
        await callback.answer()
        return

    action = callback.data.split(":")[-1]
    current_state = await state.get_state()
    if not current_state or not current_state.startswith("AddCheckFSM:"):
        await callback.answer()
        return

    if action == "cancel":
        await _cancel_add_check(callback.message, state)
        await callback.answer()
        return

    if action == "skip":
        if current_state == AddCheckFSM.pay_method.state:
            await state.update_data(pay_method=None)
            await state.set_state(AddCheckFSM.note)
            await safe_send_message(callback.message.bot, callback.message.chat.id, "Комментарий к чеку:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
            await callback.answer()
            return
        if current_state == AddCheckFSM.note.state:
            await state.update_data(note=None)
            await _show_items_menu(callback.message, state)
            await callback.answer()
            return
        if current_state == AddCheckFSM.item_note.state:
            data = await state.get_data()
            item_draft = data.get("item_draft", {})
            item_draft["note"] = None

            items = data.get("items", [])
            edit_index = data.get("edit_index")
            if edit_index is None:
                items.append(item_draft)
            else:
                items[edit_index] = item_draft
            await state.update_data(items=items, item_draft={}, edit_index=None)
            await _show_items_menu(callback.message, state)
            await callback.answer()
            return
        if current_state == AddCheckFSM.receipt.state:
            await state.update_data(receipt_file_id=None, receipt_file_type=None)
            await _show_summary(callback.message, state)
            await callback.answer()
            return
        if current_state == AddCheckFSM.item_edit_value.state:
            data = await state.get_data()
            if data.get("edit_field") == "note":
                idx = data.get("edit_index")
                items = data.get("items", [])
                if idx is not None and 0 <= idx < len(items):
                    items[idx]["note"] = None
                    await state.update_data(items=items, edit_index=None, edit_field=None)
                    await _show_items_menu(callback.message, state)
                    await callback.answer()
                    return

        await callback.answer("Для этого шага пропуск недоступен", show_alert=True)
        return

    if action == "back":
        prev_state = ADD_CHECK_PREV_STATE.get(current_state)
        await _render_add_check_state(callback.message, state, prev_state, pool=pool)
        await callback.answer()
        return

    await callback.answer()


async def _show_items_menu(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    items = data.get("items", [])
    await state.set_state(AddCheckFSM.items_menu)
    await safe_send_message(
        message.bot,
        message.chat.id,
        f"{_items_text(items)}\n\nВыберите действие с позициями:",
        reply_markup=_items_menu_keyboard(len(items)),
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
        f"Игра: {data.get('game_code', 'COS')}\n"
        f"Валюта: {data.get('currency', 'RUB')}\n"
        f"Способ оплаты: {data.get('pay_method') or '-'}\n"
        f"Комментарий: {data.get('note') or '-'}\n"
        f"Файл чека: {'есть' if data.get('receipt_file_id') else 'нет'}\n"
        f"Итог по позициям: {total}\n\n"
        f"{_items_text(items)}",
        reply_markup=CONFIRM_KEYBOARD,
    )


async def _prompt_currency(message: Message, *, state: FSMContext | None = None) -> None:
    if state is not None:
        await state.set_state(AddCheckFSM.currency)
    await safe_send_message(message.bot, message.chat.id, "💱 Выберите валюту:", reply_markup=CURRENCY_KEYBOARD)


async def _prompt_game(message: Message, *, state: FSMContext | None = None) -> None:
    if state is not None:
        await state.set_state(AddCheckFSM.game)
    await safe_send_message(message.bot, message.chat.id, "🎮 Выберите игру:", reply_markup=GAME_KEYBOARD)


@router.message(AddCheckFSM.game)
async def add_check_game(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите игру кнопками.", reply_markup=GAME_KEYBOARD)


@router.callback_query(AddCheckFSM.game, F.data.startswith("add_check:game:"))
async def add_check_game_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message is None:
        await callback.answer()
        return

    game_code = callback.data.split(":")[-1].upper()
    if game_code not in GAME_CODES:
        await callback.answer("Неизвестная игра", show_alert=True)
        return

    await state.update_data(game_code=game_code)
    await _prompt_currency(callback.message, state=state)
    await callback.answer()


async def _prompt_pay_method(message: Message, *, state: FSMContext | None = None) -> None:
    if state is not None:
        await state.set_state(AddCheckFSM.pay_method)
    await safe_send_message(message.bot, message.chat.id, "💳 Способ оплаты:", reply_markup=PAY_METHOD_KEYBOARD)


@router.message(AddCheckFSM.currency)
async def add_check_currency(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_game(message, state=state)
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите валюту кнопками.", reply_markup=CURRENCY_KEYBOARD)


@router.callback_query(AddCheckFSM.currency, F.data.startswith("add_check:currency:"))
async def add_check_currency_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message is None:
        await callback.answer()
        return

    action = callback.data.split(":")[-1]
    if action == "other":
        await state.set_state(AddCheckFSM.currency_custom)
        await safe_send_message(
            callback.message.bot,
            callback.message.chat.id,
            "Введите валюту (2–6 символов, например AED):",
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
    else:
        await state.update_data(currency=action.upper())
        await _prompt_pay_method(callback.message, state=state)
    await callback.answer()


@router.message(AddCheckFSM.currency_custom)
async def add_check_currency_custom(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_currency(message, state=state)
        return

    currency = text.upper()
    if not (2 <= len(currency) <= 6):
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Валюта должна быть длиной от 2 до 6 символов.",
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
        return

    await state.update_data(currency=currency)
    await _prompt_pay_method(message, state=state)


@router.message(AddCheckFSM.pay_method)
async def add_check_pay_method(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_currency(message, state=state)
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите способ оплаты кнопками.", reply_markup=PAY_METHOD_KEYBOARD)


@router.callback_query(AddCheckFSM.pay_method, F.data.startswith("add_check:pay_method:"))
async def add_check_pay_method_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message is None:
        await callback.answer()
        return

    action = callback.data.split(":")[-1]
    pay_method_map = {
        "card": "Карта",
        "cash": "Наличные",
        "crypto": "Крипта",
    }
    if action == "other":
        await state.set_state(AddCheckFSM.pay_method_custom)
        await safe_send_message(
            callback.message.bot,
            callback.message.chat.id,
            "Введите способ оплаты:",
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
    elif action in pay_method_map:
        await state.update_data(pay_method=pay_method_map[action])
        await state.set_state(AddCheckFSM.note)
        await safe_send_message(callback.message.bot, callback.message.chat.id, "Комментарий к чеку:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
    else:
        await safe_send_message(callback.message.bot, callback.message.chat.id, "Неизвестный способ оплаты.", reply_markup=PAY_METHOD_KEYBOARD)
    await callback.answer()


@router.message(AddCheckFSM.pay_method_custom)
async def add_check_pay_method_custom(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_pay_method(message, state=state)
        return
    if not text:
        await safe_send_message(message.bot, message.chat.id, "Введите способ оплаты текстом.", reply_markup=INLINE_NAV_BACK_CANCEL)
        return

    await state.update_data(pay_method=text)
    await state.set_state(AddCheckFSM.note)
    await safe_send_message(message.bot, message.chat.id, "Комментарий к чеку:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.note)
async def add_check_note(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_pay_method(message, state=state)
        return

    note = None if _is_skip(text) or not text or _is_dash_skip(text) else text
    await state.update_data(note=note)
    await _show_items_menu(message, state)


@router.message(AddCheckFSM.items_menu)
async def add_check_items_menu(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.note)
        await safe_send_message(message.bot, message.chat.id, "Комментарий к чеку:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
        return
    if text == "➕ Добавить позицию":
        data = await state.get_data()
        await state.update_data(item_draft={}, edit_index=None)
        await state.set_state(AddCheckFSM.item_category)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Выберите категорию:",
            reply_markup=_item_category_keyboard(data.get("game_code")),
        )
        return
    if text == "🗑 Удалить позицию":
        await state.set_state(AddCheckFSM.item_delete)
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции для удаления:", reply_markup=INLINE_NAV_BACK_CANCEL)
        return
    if text in {f"✏️ {BTN_FIX} позицию", "✏️ Изменить позицию"}:
        await state.set_state(AddCheckFSM.item_edit_select)
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции для изменения:", reply_markup=INLINE_NAV_BACK_CANCEL)
        return
    if text in {"✅ К чеку", "➡️ К файлу"}:
        await state.set_state(AddCheckFSM.receipt)
        await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ чека:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
        return

    data = await state.get_data()
    items = data.get("items", [])
    await safe_send_message(
        message.bot,
        message.chat.id,
        "Выберите действие кнопками.",
        reply_markup=_items_menu_keyboard(len(items)),
    )


@router.message(AddCheckFSM.item_category)
async def add_check_item_category(message: Message, state: FSMContext, pool: asyncpg.Pool) -> None:
    text = (message.text or "").strip()
    text_upper = text.upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _show_items_menu(message, state)
        return

    data = await state.get_data()
    allowed_categories = _valid_item_categories(data.get("game_code"))
    category = text_upper if text_upper in allowed_categories else CATEGORY_CODES_BY_LABEL.get(text)
    if category not in allowed_categories:
        category = None
    if category is None:
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Выберите категорию из кнопок.",
            reply_markup=_item_category_keyboard(data.get("game_code")),
        )
        return

    item_draft = data.get("item_draft", {})
    item_draft["category"] = category
    await state.update_data(item_draft=item_draft)
    await _prompt_item_name(message, state, pool)


@router.message(AddCheckFSM.item_name)
async def add_check_item_name(message: Message, state: FSMContext, pool: asyncpg.Pool) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        data = await state.get_data()
        await state.set_state(AddCheckFSM.item_category)
        await safe_send_message(
            message.bot,
            message.chat.id,
            "Выберите категорию:",
            reply_markup=_item_category_keyboard(data.get("game_code")),
        )
        return
    if not text:
        await safe_send_message(message.bot, message.chat.id, "Название не должно быть пустым.")
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    category = item_draft.get("category", "OTHER")
    game_code = str(data.get("game_code") or "COS").upper()

    if category in PRODUCT_CATEGORIES and not _is_dash_skip(text):
        found = await search_products(pool, game_code, category, text, limit=20)
        if not found:
            await safe_send_message(
                message.bot,
                message.chat.id,
                "Ничего не найдено. Можете нажать ‘➕ Добавить товар’ или попробуйте другой запрос.",
            )
            await _send_product_page(message, pool=pool, game_code=game_code, category=category, page=1, mode="pick")
            return
        kb = _build_products_keyboard(
            category=category,
            products=found[:PRODUCTS_PAGE_SIZE],
            page=1,
            total_pages=1,
            include_add_button=True,
            mode="pick",
        )
        await safe_send_message(message.bot, message.chat.id, "Найдено:", reply_markup=kb)
        return

    item_name = None if _is_dash_skip(text) else text
    item_draft["item_name"] = item_name
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_qty)
    await safe_send_message(message.bot, message.chat.id, _item_qty_prompt(item_draft.get("category", "OTHER")), reply_markup=INLINE_NAV_BACK_CANCEL)


@router.message(AddCheckFSM.item_name_add_new)
async def add_check_item_name_add_new(message: Message, state: FSMContext, pool: asyncpg.Pool) -> None:
    text = _trim_product_name((message.text or "").strip())
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_item_name(message, state, pool)
        return
    if not text:
        await safe_send_message(message.bot, message.chat.id, "Название не должно быть пустым.")
        return
    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    category = item_draft.get("category")
    if category not in _valid_item_categories(data.get("game_code")):
        await _prompt_item_name(message, state, pool)
        return
    game_code = str(data.get("game_code") or "COS").upper()
    product = await add_product(pool, game_code, category, text)
    item_draft["item_name"] = product["name"]
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_qty)
    await safe_send_message(message.bot, message.chat.id, _item_qty_prompt(item_draft.get("category", "OTHER")), reply_markup=INLINE_NAV_BACK_CANCEL)


@router.message(AddCheckFSM.item_qty)
async def add_check_item_qty(message: Message, state: FSMContext, pool: asyncpg.Pool) -> None:
    text = (message.text or "").strip()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await _prompt_item_name(message, state, pool)
        return
    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    category = item_draft.get("category", "OTHER")
    qty = _parse_coins_qty(text) if category == "COINS" else _parse_decimal(text)
    if qty is None:
        error_text = (
            "Введите число. Для COINS поддерживаются суффиксы k/m, например: 250k, 1m, 2.5m."
            if category == "COINS"
            else NUMERIC_INPUT_ERROR_TEXT
        )
        await safe_send_message(message.bot, message.chat.id, error_text)
        return

    item_draft["qty"] = str(qty)
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_unit_price)
    await safe_send_message(
        message.bot,
        message.chat.id,
        _item_unit_price_prompt(item_draft.get("category", "OTHER")),
        reply_markup=INLINE_NAV_BACK_CANCEL,
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
        await safe_send_message(message.bot, message.chat.id, _item_qty_prompt(item_draft.get("category", "OTHER")), reply_markup=INLINE_NAV_BACK_CANCEL)
        return
    unit_price = _parse_decimal(text)
    if unit_price is None:
        await safe_send_message(message.bot, message.chat.id, NUMERIC_INPUT_ERROR_TEXT)
        return

    category = item_draft.get("category", "OTHER")
    qty = Decimal(item_draft["qty"])
    unit_basis, line_total = _calc_line(category, qty, unit_price)

    item_draft["unit_price"] = str(unit_price)
    item_draft["unit_basis"] = unit_basis
    item_draft["line_total"] = str(line_total)
    await state.update_data(item_draft=item_draft)
    await state.set_state(AddCheckFSM.item_note)
    await safe_send_message(message.bot, message.chat.id, "Комментарий к позиции:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)


@router.message(AddCheckFSM.item_note)
async def add_check_item_note(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
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
            _item_unit_price_prompt(item_draft.get("category", "OTHER")),
            reply_markup=INLINE_NAV_BACK_CANCEL,
        )
        return

    data = await state.get_data()
    item_draft = data.get("item_draft", {})
    item_draft["note"] = None if _is_skip(text) or not text or _is_dash_skip(text) else text
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
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
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
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
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
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.item_edit_select)
        await safe_send_message(message.bot, message.chat.id, "Введите номер позиции для изменения:", reply_markup=INLINE_NAV_BACK_CANCEL)
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

    data = await state.get_data()
    idx = data.get("edit_index")
    items = data.get("items", [])
    if idx is None or idx < 0 or idx >= len(items):
        await _show_items_menu(message, state)
        return

    item = items[idx]
    await state.update_data(edit_field=field)
    await state.set_state(AddCheckFSM.item_edit_value)
    kb = (
        _item_category_keyboard(data.get("game_code"))
        if field == "category"
        else (INLINE_NAV_BACK_CANCEL_SKIP if field == "note" else INLINE_NAV_BACK_CANCEL)
    )
    await safe_send_message(message.bot, message.chat.id, _item_edit_value_prompt(field, item), reply_markup=kb)


@router.message(AddCheckFSM.item_edit_value)
async def add_check_item_edit_value(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
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
        text_upper = text.upper()
        allowed_categories = _valid_item_categories(data.get("game_code"))
        val = text_upper if text_upper in allowed_categories else CATEGORY_CODES_BY_LABEL.get(text)
        if val not in allowed_categories:
            val = None
        if val is None:
            await safe_send_message(
                message.bot,
                message.chat.id,
                "Выберите категорию из кнопок.",
                reply_markup=_item_category_keyboard(data.get("game_code")),
            )
            return
        item[field] = val
    elif field in {"qty", "unit_price"}:
        dec = _parse_decimal(text)
        if dec is None:
            await safe_send_message(message.bot, message.chat.id, NUMERIC_INPUT_ERROR_TEXT)
            return
        item[field] = str(dec)
    elif field == "note":
        item[field] = None if _is_skip(text) or not text or _is_dash_skip(text) else text
    elif field == "item_name":
        if not text:
            await safe_send_message(message.bot, message.chat.id, "Название не должно быть пустым.")
            return
        item[field] = None if _is_dash_skip(text) else text
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
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
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
    data = await state.get_data()
    game_code = str(data.get("game_code") or "COS").upper()
    if _is_cancel(text):
        await _cancel_add_check(message, state)
        return
    if _is_back(text):
        await state.set_state(AddCheckFSM.receipt)
        await safe_send_message(message.bot, message.chat.id, "Отправьте фото/документ чека:", reply_markup=INLINE_NAV_BACK_CANCEL_SKIP)
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
        game_code=data.get("game_code") or "COS",
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



@router.message((F.text == BTN_EXPORT_EXCEL) | (F.text == BTN_EXPORT_EXCEL_LEGACY))
async def ask_export_period(message: Message, settings: Settings) -> None:
    if not await _check_access(message, settings):
        return
    await safe_send_message(message.bot, message.chat.id, "Выберите игру для выгрузки:", reply_markup=EXPORT_GAME_KEYBOARD)


@router.callback_query(F.data.startswith("export:"))
async def export_excel(callback: CallbackQuery, settings: Settings, pool: asyncpg.Pool) -> None:
    if not await _check_access(callback, settings):
        return

    callback_data = str(callback.data or "")
    if callback_data == "export:back":
        if callback.message:
            await callback.message.edit_text("Выберите игру для выгрузки:", reply_markup=EXPORT_GAME_KEYBOARD)
        await callback.answer()
        return

    if callback_data.startswith("export:game:"):
        game_code = callback_data.split(":", maxsplit=2)[2].strip().upper()
        if game_code not in GAME_CODES:
            await callback.answer("Неверная игра", show_alert=True)
            return
        if callback.message:
            await callback.message.edit_text(
                f"Выберите период выгрузки для {game_code}:",
                reply_markup=export_period_keyboard(game_code),
            )
        await callback.answer()
        return

    parts = callback_data.split(":")
    if len(parts) != 3 or parts[0] != "export":
        await callback.answer("Неверный формат запроса", show_alert=True)
        return

    game_code = parts[1].strip().upper()
    period = parts[2].strip()

    if game_code not in GAME_CODES:
        await callback.answer("Неверная игра", show_alert=True)
        return

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
    admin_id = int(callback.from_user.id)
    admin_tz = get_admin_tz(settings, admin_id)
    date_range = period_to_utc_range(period_for_filter, tz=admin_tz)
    receipts = await list_receipts_by_period(pool, date_range=date_range, game_code=game_code)

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
                "game": receipt_data.get("game_code") or game_code,
                "currency": receipt_data.get("currency"),
                "pay_method": receipt_data.get("pay_method"),
                "total_sum": sum((Decimal(str(item.get("line_total") or "0")) for item in items), Decimal("0")),
                "note": receipt_data.get("note"),
                "receipt_file_id": receipt_data.get("receipt_file_id"),
                "status": receipt_data.get("status"),
                "items": items,
            }
        )

    report_bytes = build_transactions_report(export_rows, admin_tz)
    filename = f"transactions_{game_code}_{period}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    document = BufferedInputFile(report_bytes, filename=filename)

    if callback.message:
        await safe_send_document(
            callback.message.bot,
            callback.message.chat.id,
            document,
            caption=f"📤 Отчёт Excel: {game_code}, {label_by_period[period]}\n🌍 TZ: {admin_tz.key}",
        )

    await callback.answer("✅ Excel отправлен")