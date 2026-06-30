from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ── Admin reply keyboard ──────────────────────────────────────────────────────

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать конкурс")],
            [KeyboardButton(text="📋 Мои конкурсы"), KeyboardButton(text="🏆 Выбрать конкурс")],
            [KeyboardButton(text="⚖️ Страйки и баны")],
        ],
        resize_keyboard=True
    )


def giveaway_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📢 Запустить в канале"), KeyboardButton(text="✏️ Редактировать")],
            [KeyboardButton(text="✅ Завершить конкурс"), KeyboardButton(text="❌ Отменить конкурс")],
            [KeyboardButton(text="🔄 Обновить ссылки"), KeyboardButton(text="👥 Участники")],
            [KeyboardButton(text="↩️ Вернуть выбывших"), KeyboardButton(text="🗑 Удалить конкурс")],
            [KeyboardButton(text="◀️ Назад к списку")],
        ],
        resize_keyboard=True
    )


def edit_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Изменить текст"), KeyboardButton(text="🏆 Изменить призы")],
            [KeyboardButton(text="📡 Изменить каналы")],
            [KeyboardButton(text="◀️ Назад к конкурсу")],
        ],
        resize_keyboard=True
    )


def channels_edit_kb(channels: list[dict]) -> InlineKeyboardMarkup:
    """Shows current channels with a delete button each, plus an 'add channel' option."""
    builder = InlineKeyboardBuilder()
    for ch in channels:
        title = ch.get('chat_title') or str(ch['chat_id'])
        builder.row(
            InlineKeyboardButton(
                text=f"🗑 Удалить: {title}",
                callback_data=f"del_channel:{ch['id']}"
            )
        )
    return builder.as_markup()


# ── Inline keyboards ──────────────────────────────────────────────────────────

def channels_kb(channels: list[dict], giveaway_id: int) -> InlineKeyboardMarkup:
    """Subscribe buttons + participate button for announcement message."""
    builder = InlineKeyboardBuilder()
    for ch in channels:
        title = ch.get('chat_title') or str(ch['chat_id'])
        builder.row(
            InlineKeyboardButton(
                text=f"✅ Подписаться → {title}",
                url=ch['invite_link']
            )
        )
    builder.row(
        InlineKeyboardButton(
            text="🎉 Участвовать",
            callback_data=f"participate:{giveaway_id}"
        )
    )
    return builder.as_markup()


def giveaway_list_kb(giveaways: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for g in giveaways:
        status_icon = "🟢" if g['status'] == 'active' else ("🏁" if g['status'] == 'finished' else "🔴")
        builder.row(
            InlineKeyboardButton(
                text=f"{status_icon} {g['title']}",
                callback_data=f"select_giveaway:{g['id']}"
            )
        )
    return builder.as_markup()


def confirm_kb(action: str, giveaway_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_{action}:{giveaway_id}"),
        InlineKeyboardButton(text="❌ Нет", callback_data="cancel_action")
    )
    return builder.as_markup()


def channel_select_kb(channels: list[dict], giveaway_id: int) -> InlineKeyboardMarkup:
    """Select which channel to publish the announcement to."""
    builder = InlineKeyboardBuilder()
    for ch in channels:
        builder.row(
            InlineKeyboardButton(
                text=f"📢 {ch['chat_title']}",
                callback_data=f"publish_to:{giveaway_id}:{ch['chat_id']}"
            )
        )
    return builder.as_markup()


def strikes_list_kb(users: list[dict]) -> InlineKeyboardMarkup:
    """One row per flagged user with their strike count; tapping opens management."""
    builder = InlineKeyboardBuilder()
    for u in users:
        uname = f"@{u['username']}" if u.get('username') else (u.get('full_name') or str(u['user_id']))
        ban = " 🚫" if u.get('is_banned') else ""
        builder.row(
            InlineKeyboardButton(
                text=f"{uname} — {u['strikes']}/3{ban}",
                callback_data=f"strike_manage:{u['user_id']}"
            )
        )
    return builder.as_markup()


def strike_manage_kb(user_id: int, is_banned: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="➖ Снять страйк", callback_data=f"strike_remove:{user_id}")
    )
    if is_banned:
        builder.row(
            InlineKeyboardButton(text="✅ Снять бан", callback_data=f"strike_unban:{user_id}")
        )
    builder.row(
        InlineKeyboardButton(text="◀️ Назад к списку", callback_data="strike_back")
    )
    return builder.as_markup()


def disqualified_list_kb(participants: list[dict]) -> InlineKeyboardMarkup:
    """List disqualified participants with a 'return to giveaway' button each."""
    builder = InlineKeyboardBuilder()
    for p in participants:
        uname = f"@{p['username']}" if p.get('username') else (p.get('full_name') or str(p['user_id']))
        builder.row(
            InlineKeyboardButton(
                text=f"↩️ Вернуть: {uname}",
                callback_data=f"requalify:{p['giveaway_id']}:{p['user_id']}"
            )
        )
    return builder.as_markup()
