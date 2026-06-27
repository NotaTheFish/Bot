from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton


def admin_menu_kb(channel_setup_done: bool = True) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📋 Все актуальные заявки")],
        [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="🚫 Забаненные")],
    ]
    if not channel_setup_done:
        rows.append([KeyboardButton(text="➕ Добавить канал")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def submission_admin_kb(submission_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Перейти в личку", url=f"tg://user?id={user_id}")],
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"sub:accept:{submission_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"sub:reject:{submission_id}"),
        ],
        [
            InlineKeyboardButton(text="💬 Ответить", callback_data=f"sub:reply:{submission_id}:{user_id}"),
            InlineKeyboardButton(text="🚫 Забанить", callback_data=f"sub:ban:{submission_id}:{user_id}"),
        ],
    ])


def accept_options_kb(submission_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚀 Публикую как есть", callback_data=f"pub:now:{submission_id}"),
            InlineKeyboardButton(text="✏️ Редактирую", callback_data=f"pub:edit:{submission_id}"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"pub:back:{submission_id}")],
    ])


def author_btn_kb(submission_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Да, добавить кнопку автора", callback_data=f"pub:author_yes:{submission_id}"),
            InlineKeyboardButton(text="🚫 Нет", callback_data=f"pub:author_no:{submission_id}"),
        ],
    ])


def settings_kb(forward_mode: str, show_author_btn: str) -> InlineKeyboardMarkup:
    fm_label = "🔁 Режим: Пересылка" if forward_mode == "forward" else "♻️ Режим: Без пересылки"
    ab_label = "👤 Кнопка автора: Вкл" if show_author_btn == "true" else "👤 Кнопка автора: Выкл"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=fm_label, callback_data="settings:toggle_forward")],
        [InlineKeyboardButton(text=ab_label, callback_data="settings:toggle_author")],
    ])


def banned_list_kb(banned_users: list) -> InlineKeyboardMarkup:
    rows = []
    for u in banned_users:
        name = f"@{u['username']}" if u["username"] else u["first_name"]
        rows.append([
            InlineKeyboardButton(
                text=f"🔓 Разбанить {name}",
                callback_data=f"ban:unban:{u['user_id']}"
            )
        ])
    if not rows:
        rows = [[InlineKeyboardButton(text="Список пуст", callback_data="noop")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def pending_nav_kb(submission_id: int, idx: int, total: int) -> InlineKeyboardMarkup:
    nav = []
    if idx > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"nav:{idx - 1}"))
    nav.append(InlineKeyboardButton(text=f"{idx + 1}/{total}", callback_data="noop"))
    if idx < total - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"nav:{idx + 1}"))

    return InlineKeyboardMarkup(inline_keyboard=[
        nav,
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"sub:accept:{submission_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"sub:reject:{submission_id}"),
        ],
        [
            InlineKeyboardButton(text="💬 Ответить", callback_data=f"sub:reply:{submission_id}:0"),
            InlineKeyboardButton(text="🚫 Забанить", callback_data=f"sub:ban:{submission_id}:0"),
        ],
    ])
