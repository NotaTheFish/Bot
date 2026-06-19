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
        ],
        resize_keyboard=True
    )


def giveaway_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📢 Запустить в канале"), KeyboardButton(text="✅ Завершить конкурс")],
            [KeyboardButton(text="❌ Отменить конкурс"), KeyboardButton(text="👥 Участники")],
            [KeyboardButton(text="◀️ Назад к списку")],
        ],
        resize_keyboard=True
    )


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