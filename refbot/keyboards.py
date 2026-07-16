from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import CURRENCY_EMOJI, CURRENCY_NAME


def main_menu(currency: str, is_admin: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="🔗 Моя ссылка", callback_data="mylink")
    kb.button(text=f"{CURRENCY_EMOJI[currency]} Валюта: {CURRENCY_NAME[currency]}",
              callback_data="toggle_cur")
    kb.button(text="👥 Мои рефералы", callback_data="myrefs")
    kb.button(text="💸 Вывод", callback_data="wd_menu")
    if is_admin:
        kb.button(text="🛠 Админка", callback_data="admin")
    kb.adjust(2, 1, 2, 1)
    return kb.as_markup()


def wd_menu(has_active: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_active:
        kb.button(text="✏️ Изменить сумму", callback_data="wd_amount")
        kb.button(text="❌ Отменить заявку", callback_data="wd_cancel")
    else:
        kb.button(text="💸 Создать заявку", callback_data="wd_amount")
    kb.button(text="⬅️ Назад", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def admin_wd_card(wid: int, version: int) -> InlineKeyboardMarkup:
    # version зашит в кнопку. Юзер поменял сумму -> version вырос -> старая кнопка мертва.
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Подтвердить вывод", callback_data=f"wdok:{wid}:{version}"),
        InlineKeyboardButton(text="🚫 Отклонить", callback_data=f"wdno:{wid}:{version}"),
    ]])


def admin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏆 Топ-25", callback_data="a_top")
    kb.button(text="🔍 Найти юзера", callback_data="a_find")
    kb.button(text="🚩 На проверке", callback_data="a_flagged")
    kb.button(text="📊 Сводка", callback_data="a_stats")
    kb.button(text="⬅️ Назад", callback_data="menu")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")]])
