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
        await btn(kb, "✏️ Изменить сумму", "wd_amount")
        await btn(kb, "❌ Отменить заявку", "wd_cancel")
    else:
        await btn(kb, "Создать заявку", "wd_amount", "withdraw")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def find_card(tg_id: int, banned: bool) -> InlineKeyboardMarkup:
    """Тумблер под найденным юзером: не забанен -> «🥳 бан» (забанит),
    забанен -> «😡 бан» (разбанит)."""
    kb = InlineKeyboardBuilder()
    if banned:
        await btn(kb, "😡 бан", f"a_toggleban:{tg_id}")
    else:
        await btn(kb, "🥳 бан", f"a_toggleban:{tg_id}")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def admin_wd_card(wid: int, version: int) -> InlineKeyboardMarkup:
    # version зашит в кнопку. Юзер поменял сумму -> version вырос -> старая кнопка мертва.
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Подтвердить вывод", f"wdok:{wid}:{version}")
    await btn(kb, "🚫 Отклонить", f"wdno:{wid}:{version}")
    kb.adjust(1)
    return kb.as_markup()


async def admin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Топ-25", "a_top", "top")
    await btn(kb, "🔍 Найти юзера", "a_find")
    await btn(kb, "🚩 На проверке", "a_flagged")
    await btn(kb, "📊 Сводка", "a_stats")
    await btn(kb, "Чаты", "a_chats", "chat")
    await btn(kb, "🚫 Баны", "a_bans")
    await btn(kb, "🎨 Кастомизация", "a_skin")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


async def chat_admin_list(chats) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for ch in chats:
        mark = "🟢" if ch["active"] else "⚪️"
        await btn(kb, f"{mark} {ch['title']}", f"a_chat:{ch['chat_id']}")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def chat_card(chat_id: int, active: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if active:
        await btn(kb, "⚪️ Отключить чат", f"a_choff:{chat_id}")
    else:
        await btn(kb, "🟢 Включить обратно", f"a_chon:{chat_id}")
    await btn(kb, "К списку", "a_chats", "back")
    kb.adjust(1)
    return kb.as_markup()


async def skin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "😀 Эмодзи", "sk_emoji")
    await btn(kb, "🏷 Названия валют", "sk_label")
    await btn(kb, "♻️ Свободные замены", "sk_free")
    await btn(kb, "📝 Шаблон профиля", "sk_tpl")
    await btn(kb, "🧪 Тест премиум-эмодзи", "sk_test")
    await btn(kb, "🗑 Сбросить всё", "sk_reset")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(2, 1, 1, 1, 1, 1)
    return kb.as_markup()


async def slot_list(slots: dict, current: dict, prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for slot, (desc, _) in slots.items():
        star = " ⭐️" if current.get(f"premium.{slot}") else ""
        await btn(kb, f"{current.get(slot, '')} {desc}{star}".strip(), f"{prefix}:{slot}")
    await btn(kb, "Кастомизация", "a_skin", "back")
    kb.adjust(2)
    return kb.as_markup()


async def slot_card(slot: str, prefix: str, has_premium: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_premium:
        await btn(kb, "🗑 Убрать премиум", f"sk_prem_off:{slot}")
    await btn(kb, "♻️ Сброс к дефолту", f"sk_def:{prefix}:{slot}")
    await btn(kb, "Назад", "sk_emoji" if prefix == "sk_e" else "sk_label", "back")
    kb.adjust(1)
    return kb.as_markup()


async def bans_panel(rows, links_mode: bool) -> InlineKeyboardMarkup:
    """
    links_mode=False — обычный вид: заголовок-строки (текст в сообщении), тут только действия.
    links_mode=True  — на месте действий появляются кнопки-ссылки tg://user на каждого.
    """
    kb = InlineKeyboardBuilder()
    if links_mode:
        for r in rows:
            name = f"@{r['username']}" if r["username"] else (r["first_name"] or str(r["tg_id"]))
            kb.button(text=f"👤 {name}", url=f"tg://user?id={r['tg_id']}")
        await btn(kb, "⬅️ Назад к списку", "a_bans")
        kb.adjust(1)
        return kb.as_markup()

    if rows:
        await btn(kb, "🔗 Ссылки на профили", "a_bans_links")
        await btn(kb, "♻️ Разбанить по ID", "a_unban")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def confirm(action: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Да", action)
    await btn(kb, "❌ Нет", "a_skin")
    kb.adjust(2)
    return kb.as_markup()


async def free_list(pairs: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for ch in pairs:
        await btn(kb, f"{ch}  ✕", f"sk_free_del:{ch}")
    await btn(kb, "➕ Добавить замену", "sk_free_add")
    await btn(kb, "Кастомизация", "a_skin", "back")
    kb.adjust(4, 1, 1)
    return kb.as_markup()


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
        await btn(kb, "🔄 Другой чат", "mylink", "chat")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()
