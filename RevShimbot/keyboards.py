from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from constants import TEMPLATES, STARS_MODES, ITEM_MODES


def kb_templates(selected: str = None) -> InlineKeyboardMarkup:
    buttons = []
    for tid, label in TEMPLATES.items():
        text = f"✅ {label}" if tid == selected else label
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"tpl:{tid}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_stars_mode(selected: str = None) -> InlineKeyboardMarkup:
    buttons = []
    for mode, label in STARS_MODES.items():
        text = f"✅ {label}" if mode == selected else label
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"stars_mode:{mode}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_stars_value() -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(text=str(i)+"★", callback_data=f"stars_val:{i}") for i in range(1, 6)]
    return InlineKeyboardMarkup(inline_keyboard=[row])


def kb_item_mode(selected: str = None) -> InlineKeyboardMarkup:
    buttons = []
    for mode, label in ITEM_MODES.items():
        text = f"✅ {label}" if mode == selected else label
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"item_mode:{mode}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_allow_template_choice(current: bool = False) -> InlineKeyboardMarkup:
    yes = "✅ Да" if current else "Да"
    no = "✅ Нет" if not current else "Нет"
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=yes, callback_data="tpl_choice:yes"),
        InlineKeyboardButton(text=no, callback_data="tpl_choice:no"),
    ]])


def kb_setup_done() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Готово — получить ссылку", callback_data="setup:done")
    ]])


def kb_seller_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Моя реферальная ссылка", callback_data="menu:mylink")],
        [InlineKeyboardButton(text="👁 Предпросмотр шаблона", callback_data="menu:preview")],
        [InlineKeyboardButton(text="✏️ Изменить шаблон", callback_data="menu:edit")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="menu:stats")],
    ])


def kb_buyer_stars() -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(text="★"*i, callback_data=f"review_stars:{i}") for i in range(1, 6)]
    return InlineKeyboardMarkup(inline_keyboard=[row])


def kb_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    ]])
