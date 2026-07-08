from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from constants import TEMPLATES, STARS_MODES, ITEM_MODES


def kb_templates_filtered(selected: str = None, custom_templates: list = None,
                          show_standard: bool = True) -> InlineKeyboardMarkup:
    """Как kb_templates, но можно скрыть стандартные шаблоны."""
    buttons = []
    if show_standard:
        for tid, label in TEMPLATES.items():
            text = f"✅ {label}" if tid == selected else label
            buttons.append([InlineKeyboardButton(text=text, callback_data=f"tpl:{tid}")])
    if custom_templates:
        for tpl in custom_templates:
            own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
            cid = f"custom_{tpl['id']}"
            mark = "✅ " if selected == cid else ""
            buttons.append([InlineKeyboardButton(
                text=f"{mark}{own} {tpl['name']}",
                callback_data=f"tpl:{cid}"
            )])
    if not buttons:
        # на всякий случай — если вообще нечего показать, даём стандартные
        for tid, label in TEMPLATES.items():
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"tpl:{tid}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_templates(selected: str = None, custom_templates: list = None) -> InlineKeyboardMarkup:
    buttons = []
    for tid, label in TEMPLATES.items():
        text = f"✅ {label}" if tid == selected else label
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"tpl:{tid}")])

    # Кастомные шаблоны из конструктора (свои и полученные)
    if custom_templates:
        for tpl in custom_templates:
            own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
            cid = f"custom_{tpl['id']}"
            mark = "✅ " if selected == cid else ""
            buttons.append([InlineKeyboardButton(
                text=f"{mark}{own} {tpl['name']}",
                callback_data=f"tpl:{cid}"
            )])

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


def kb_back_to_menu() -> InlineKeyboardMarkup:
    """Кнопка возврата в меню продавца — для информационных экранов."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад в меню", callback_data="menu:seller_home")]
    ])


def kb_seller_menu(pub_id: str = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📋 Мой шаблон", callback_data="menu:mytemplate")],
    ]
    if pub_id:
        from aiogram.types import SwitchInlineQueryChosenChat
        rows.append([InlineKeyboardButton(
            text="📨 Поделиться (отправить приглашение)",
            switch_inline_query_chosen_chat=SwitchInlineQueryChosenChat(
                query=pub_id,
                allow_user_chats=True,
                allow_group_chats=True,
                allow_channel_chats=True,
            )
        )])
    rows.append([InlineKeyboardButton(text="🔗 Реф-ссылка и фраза для чата", callback_data="menu:mylink")])
    rows.append([InlineKeyboardButton(text="👁 Предпросмотр карточки", callback_data="menu:preview")])
    rows.append([InlineKeyboardButton(text="📊 Статистика", callback_data="menu:stats")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_template_view(seller: dict) -> InlineKeyboardMarkup:
    """Кнопки редактирования конкретных полей шаблона."""
    rows = [
        [InlineKeyboardButton(text="✏️ Название магазина", callback_data="edit:shop_name")],
        [InlineKeyboardButton(text="🎨 Стиль карточки", callback_data="edit:template")],
        [InlineKeyboardButton(text="⭐️ Звёзды", callback_data="edit:stars")],
        [InlineKeyboardButton(text="📦 Поле «Что купил»", callback_data="edit:item")],
        [InlineKeyboardButton(text="🎴 Карточки для отзывов", callback_data="edit:cards")],
        [InlineKeyboardButton(text="🔘 Управление инлайн-кнопкой", callback_data="edit:inlinebtn")],
        [InlineKeyboardButton(text="💬 Отзывы через личку (инлайн)", callback_data="edit:inlinecfg")],
        [InlineKeyboardButton(text="🕵️ Анонимные отзывы", callback_data="edit:anon")],
    ]
    pub_id = seller.get("pub_id")
    if pub_id:
        from aiogram.types import SwitchInlineQueryChosenChat
        rows.append([InlineKeyboardButton(
            text="📨 Поделиться (отправить приглашение)",
            switch_inline_query_chosen_chat=SwitchInlineQueryChosenChat(
                query=pub_id,
                allow_user_chats=True,
                allow_group_chats=True,
                allow_channel_chats=True,
            )
        )])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Фича: источник карточек и выбор своих ──────────────────────────────────

CARD_SOURCE_LABELS = {
    "standard": "🎴 Только стандартные",
    "custom":   "👑 Только мои карточки",
    "both":     "🎴👑 Мои + стандартные",
}


def kb_card_source(seller: dict) -> InlineKeyboardMarkup:
    """Меню выбора режима источника карточек для отзывов."""
    mode = seller.get("card_source_mode", "standard")
    rows = []
    for key, label in CARD_SOURCE_LABELS.items():
        mark = "✅ " if mode == key else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{label}", callback_data=f"cardsrc:{key}")])
    # Если режим включает свои карточки — кнопка выбора каких именно
    if mode in ("custom", "both"):
        rows.append([InlineKeyboardButton(text="⚙️ Выбрать какие мои карточки показывать",
                                           callback_data="cardsrc:pick")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:mytemplate")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_pick_custom_cards(customs: list, allowed_ids: list) -> InlineKeyboardMarkup:
    """Тумблеры по каждой своей карточке — какие показывать клиенту."""
    rows = []
    allowed = set(allowed_ids or [])
    for tpl in customs:
        cid = tpl["id"]
        mark = "✅" if cid in allowed else "⬜️"
        own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
        rows.append([InlineKeyboardButton(
            text=f"{mark} {own} {tpl['name']}",
            callback_data=f"cardpick:{cid}"
        )])
    if not customs:
        rows.append([InlineKeyboardButton(text="У тебя пока нет своих карточек",
                                           callback_data="cardsrc:noop")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="edit:cards")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Фича: режим инлайн-кнопки ──────────────────────────────────────────────

INLINE_BTN_LABELS = {
    "hidden": "🚫 Спрятать (кнопки нет)",
    "shown":  "✅ Включить (кнопка всегда)",
    "ask":    "❓ Спросить клиента",
}


def kb_inline_button(seller: dict) -> InlineKeyboardMarkup:
    """Меню режима инлайн-кнопки со ссылкой на клиента."""
    mode = seller.get("inline_button_mode", "shown")
    rows = []
    for key, label in INLINE_BTN_LABELS.items():
        mark = "✅ " if mode == key else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{label}", callback_data=f"inlbtn:{key}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:mytemplate")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_anon_settings(seller: dict) -> InlineKeyboardMarkup:
    """Настройки анонимных отзывов: режим off/on/ask + ник и аватар анона."""
    mode = seller.get("anon_mode", "off")
    labels = {"off": "🚫 Выключены", "on": "✅ Всегда анонимно", "ask": "❓ Спрашивать клиента"}
    rows = []
    for key, label in labels.items():
        mark = "✅ " if mode == key else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{label}", callback_data=f"anonset:{key}")])
    rows.append([InlineKeyboardButton(text="✏️ Ник анонима", callback_data="anonset:nick")])
    has_av = bool(seller.get("anon_avatar"))
    rows.append([InlineKeyboardButton(
        text="🖼 Аватар анонима" + (" ✅" if has_av else ""),
        callback_data="anonset:avatar")])
    if has_av:
        rows.append([InlineKeyboardButton(text="🗑 Сбросить аватар", callback_data="anonset:avatar_reset")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:mytemplate")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_inline_config(seller: dict) -> InlineKeyboardMarkup:
    """Настройки отзывов через личку/инлайн: карточка + кнопка-ссылка + уведомления + анон."""
    btn_on = seller.get("inline_button_show", True)
    anon_on = seller.get("inline_anon", False)
    # Если анон включён — ссылка на покупателя физически невозможна, показываем это
    if anon_on:
        btn_label = "🔒 Ссылка на покупателя: недоступна (анон)"
    else:
        btn_label = "✅ Ссылка на покупателя: ВКЛ" if btn_on else "⬜️ Ссылка на покупателя: ВЫКЛ"
    notify_on = seller.get("inline_notify_seller", False)
    notify_label = "🔔 Уведомления о чат-отзывах: ВКЛ" if notify_on else "🔕 Уведомления о чат-отзывах: ВЫКЛ"
    anon_label = "🕵️ Анонимные инлайн-отзывы: ВКЛ" if anon_on else "👤 Анонимные инлайн-отзывы: ВЫКЛ"
    rows = [
        [InlineKeyboardButton(text="🎨 Карточка по умолчанию", callback_data="inlcfg:tpl")],
        [InlineKeyboardButton(text=btn_label, callback_data="inlcfg:btntoggle")],
        [InlineKeyboardButton(text=anon_label, callback_data="inlcfg:anontoggle")],
        [InlineKeyboardButton(text=notify_label, callback_data="inlcfg:notifytoggle")],
        [InlineKeyboardButton(text="« Назад", callback_data="menu:mytemplate")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_buyer_stars() -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(text="★"*i, callback_data=f"review_stars:{i}") for i in range(1, 6)]
    return InlineKeyboardMarkup(inline_keyboard=[row])


def kb_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
    ]])


def kb_main_reply() -> ReplyKeyboardMarkup:
    """Постоянная reply-клавиатура снизу."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Главное меню")],
        ],
        resize_keyboard=True,
        is_persistent=False,
    )
