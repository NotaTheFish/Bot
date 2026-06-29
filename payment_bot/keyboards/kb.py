from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ─── MAIN ADMIN PANEL ─────────────────────────────────────────────────────────

def main_admin_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="💰 Создать сделку", callback_data="deal:create"))
    builder.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="deal:list:all"))
    builder.row(InlineKeyboardButton(text="💵 Мой баланс", callback_data="balance:my"))
    builder.row(InlineKeyboardButton(text="📊 Оборот суб-админов", callback_data="balance:subadmins"))
    builder.row(InlineKeyboardButton(text="🔑 Создать ключ суб-админа", callback_data="admin_key:create"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки агрегатора", callback_data="aggregator:setup"))
    return builder.as_markup()


def sub_admin_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="💰 Создать сделку", callback_data="deal:create"))
    builder.row(InlineKeyboardButton(text="📋 Мои сделки", callback_data="deal:list:all"))
    builder.row(InlineKeyboardButton(text="💵 Мой баланс", callback_data="balance:my"))
    builder.row(InlineKeyboardButton(text="⚙️ Настройки агрегатора", callback_data="aggregator:setup"))
    return builder.as_markup()


# ─── DEAL STATUS FILTER ───────────────────────────────────────────────────────

def deal_filter_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Все", callback_data="deal:list:all"),
        InlineKeyboardButton(text="⏳ Ожидают", callback_data="deal:list:WAITING_PAYMENT"),
    )
    builder.row(
        InlineKeyboardButton(text="✅ Оплачены", callback_data="deal:list:PAID"),
        InlineKeyboardButton(text="🔒 Закрыты", callback_data="deal:list:CLOSED"),
    )
    builder.row(InlineKeyboardButton(text="↩️ Назад", callback_data="menu:main"))
    return builder.as_markup()


# ─── DEAL ACTIONS ─────────────────────────────────────────────────────────────

def deal_actions_keyboard(deal_id: int, status: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if status == "PAID":
        builder.row(InlineKeyboardButton(
            text="🔒 Закрыть сделку",
            callback_data=f"deal:close:{deal_id}"
        ))
    builder.row(InlineKeyboardButton(text="↩️ Назад к сделкам", callback_data="deal:list:all"))
    return builder.as_markup()


def confirm_close_keyboard(deal_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да, закрыть", callback_data=f"deal:close_confirm:{deal_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"deal:view:{deal_id}"),
    )
    return builder.as_markup()


# ─── CURRENCY SELECTION ───────────────────────────────────────────────────────

def currency_keyboard(available_currencies: list[str], deal_id: int = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    currency_labels = {
        "RUB": "🇷🇺 Рубли (RUB)",
        "UAH": "🇺🇦 Гривны (UAH)",
        "EUR": "🇪🇺 Евро (EUR)",
        "USD": "🇺🇸 Доллары (USD)",
        "KZT": "🇰🇿 Тенге (KZT)",
        "BYN": "🇧🇾 Белорусские рубли (BYN)",
        "MDL": "🇲🇩 Молдавские леи (MDL)",
    }
    for currency in available_currencies:
        label = currency_labels.get(currency, currency)
        builder.row(InlineKeyboardButton(
            text=label,
            callback_data=f"pay:currency:{currency}"
        ))
    if deal_id is not None:
        builder.row(InlineKeyboardButton(
            text="❌ Отменить сделку",
            callback_data=f"client:cancel:{deal_id}"
        ))
    return builder.as_markup()


# ─── PAYMENT CONFIRMATION ─────────────────────────────────────────────────────

def payment_done_keyboard(deal_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="✅ Я оплатил",
        callback_data=f"pay:done:{deal_id}"
    ))
    return builder.as_markup()


# ─── AGGREGATOR SETUP ─────────────────────────────────────────────────────────

def aggregator_choice_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🟢 Lava.ru", callback_data="aggregator:choose:lava"))
    builder.row(InlineKeyboardButton(text="🔵 Payok.io", callback_data="aggregator:choose:payok"))
    builder.row(InlineKeyboardButton(text="🟠 Freekassa", callback_data="aggregator:choose:freekassa"))
    builder.row(InlineKeyboardButton(text="↩️ Назад", callback_data="menu:main"))
    return builder.as_markup()


def cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="menu:main"))
    return builder.as_markup()
