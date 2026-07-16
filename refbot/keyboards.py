from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from services import settings
from services.ui import btn


async def main_menu(currency: str, is_admin: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Профиль", "profile", "profile")
    await btn(kb, "Моя ссылка", "mylink", "link")
    await btn(kb, f"Валюта: {await settings.label(currency)}", "toggle_cur", currency)
    await btn(kb, "Мои рефералы", "myrefs", "refs")
    await btn(kb, "Вывод", "wd_menu", "withdraw")
    if is_admin:
        await btn(kb, "Админка", "admin", "admin")
    kb.adjust(2, 1, 2, 1)
    return kb.as_markup()


async def wd_menu(has_active: bool) -> InlineKeyboardMarkup:
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


async def admin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Топ-25", "a_top", "top")
    kb.button(text="🔍 Найти юзера", callback_data="a_find")
    kb.button(text="🚩 На проверке", callback_data="a_flagged")
    kb.button(text="📊 Сводка", callback_data="a_stats")
    await btn(kb, "Чаты", "a_chats", "chat")
    kb.button(text="🎨 Кастомизация", callback_data="a_skin")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


async def chat_admin_list(chats) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for ch in chats:
        mark = "🟢" if ch["active"] else "⚪️"
        kb.button(text=f"{mark} {ch['title']}", callback_data=f"a_chat:{ch['chat_id']}")
    kb.button(text="⬅️ Админка", callback_data="admin")
    kb.adjust(1)
    return kb.as_markup()


async def chat_card(chat_id: int, active: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if active:
        kb.button(text="⚪️ Отключить чат", callback_data=f"a_choff:{chat_id}")
    else:
        kb.button(text="🟢 Включить обратно", callback_data=f"a_chon:{chat_id}")
    kb.button(text="⬅️ К списку", callback_data="a_chats")
    kb.adjust(1)
    return kb.as_markup()


async def skin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="😀 Эмодзи", callback_data="sk_emoji")
    kb.button(text="🏷 Названия валют", callback_data="sk_label")
    kb.button(text="📝 Шаблон профиля", callback_data="sk_tpl")
    kb.button(text="🧪 Тест премиум-эмодзи", callback_data="sk_test")
    kb.button(text="♻️ Сбросить всё", callback_data="sk_reset")
    kb.button(text="⬅️ Админка", callback_data="admin")
    kb.adjust(2, 1, 1, 1, 1)
    return kb.as_markup()


async def slot_list(slots: dict, current: dict, prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for slot, (desc, _) in slots.items():
        star = "⭐️" if current.get(f"premium.{slot}") else ""
        kb.button(text=f"{current.get(slot, '')}{star} {desc}",
                  callback_data=f"{prefix}:{slot}")
    kb.button(text="⬅️ Кастомизация", callback_data="a_skin")
    kb.adjust(2)
    return kb.as_markup()


async def slot_card(slot: str, prefix: str, has_premium: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_premium:
        kb.button(text="🗑 Убрать премиум", callback_data=f"sk_prem_off:{slot}")
    kb.button(text="♻️ Сброс к дефолту", callback_data=f"sk_def:{prefix}:{slot}")
    kb.button(text="⬅️ Назад", callback_data="sk_emoji" if prefix == "sk_e" else "sk_label")
    kb.adjust(1)
    return kb.as_markup()


def confirm(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да", callback_data=action),
        InlineKeyboardButton(text="❌ Нет", callback_data="a_skin"),
    ]])


async def back_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Меню", "menu", "back")
    return kb.as_markup()


async def chat_picker(chats, prefix: str) -> InlineKeyboardMarkup:
    """prefix: 'lnk' — выбор чата для реф-ссылки."""
    kb = InlineKeyboardBuilder()
    for ch in chats:
        await btn(kb, ch["title"], f"{prefix}:{ch['chat_id']}", "chat")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def link_card(multi: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if multi:
        kb.button(text="🔄 Другой чат", callback_data="mylink")
    kb.button(text="⬅️ Меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()
