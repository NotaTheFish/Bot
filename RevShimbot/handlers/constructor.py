"""
Хендлер конструктора карточек — пошаговое создание с живым превью.
"""
import secrets
import string
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile, InputMediaPhoto
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database
from services.constructor import (
    LAYOUTS, FONTS, TEXT_COLORS, ACCENT_COLORS, BG_COLORS,
    BG_GRADIENTS, CARD_BORDERS, CARD_RADIUS, CARD_SHADOW, TEXT_SIZES,
    VISIBILITY_DEFAULTS,
    render_preview
)

router = Router()

MAX_TEMPLATES = 15


class ConstructorSG(StatesGroup):
    building = State()      # активное конструирование с превью
    enter_bg_image = State()
    enter_name = State()
    enter_key = State()     # ввод ключа для получения шаблона


def _default_cfg() -> dict:
    return {
        "layout": "classic",
        "font": "montserrat",
        "title_font": "caveat",
        "text_color": "#f0f0f0",
        "accent_color": "#c9a84c",
        "bg_color": "#1a1a2e",
        "bg_image": None,
        "creator_username": None,
        "is_edited": False,
        "extra_cfg": {},
    }


# ── Клавиатуры конструктора ────────────────────────────────────────────────

def kb_constructor_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📐 Раскладка", callback_data="con:menu:layout")],
        [InlineKeyboardButton(text="🔤 Шрифт названия", callback_data="con:menu:title_font"),
         InlineKeyboardButton(text="🔡 Шрифт текста", callback_data="con:menu:font")],
        [InlineKeyboardButton(text="🎨 Цвет текста", callback_data="con:menu:text_color"),
         InlineKeyboardButton(text="✨ Акцент", callback_data="con:menu:accent_color")],
        [InlineKeyboardButton(text="🌌 Фон (цвет)", callback_data="con:menu:bg_color"),
         InlineKeyboardButton(text="🌈 Градиент", callback_data="con:menu:bg_gradient")],
        [InlineKeyboardButton(text="🖼 Фон (картинка)", callback_data="con:menu:bg_image")],
        [InlineKeyboardButton(text="🔲 Рамка", callback_data="con:menu:card_border"),
         InlineKeyboardButton(text="⬜️ Углы", callback_data="con:menu:card_radius")],
        [InlineKeyboardButton(text="🌫 Тень", callback_data="con:menu:card_shadow"),
         InlineKeyboardButton(text="🔠 Размер текста", callback_data="con:menu:text_size")],
        [InlineKeyboardButton(text="👁 Показать/скрыть блоки", callback_data="con:menu:visibility")],
        [InlineKeyboardButton(text="💾 Сохранить шаблон", callback_data="con:save")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="con:cancel")],
    ])


def _kb_options(prefix: str, options: dict, back="con:back") -> InlineKeyboardMarkup:
    rows = []
    items = list(options.items())
    for key, val in items:
        label = val[0] if isinstance(val, tuple) else val
        rows.append([InlineKeyboardButton(text=label, callback_data=f"con:set:{prefix}:{key}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


VIS_LABELS = {
    "show_date": "📅 Дата",
    "show_item": "📦 Товар",
    "show_avatar": "👤 Аватар",
    "show_seller_tag": "🏷 Тег продавца",
    "show_quote": "❝ Кавычки",
}


def _kb_visibility(cfg: dict) -> InlineKeyboardMarkup:
    ex = cfg.get("extra_cfg") or {}
    rows = []
    for key, label in VIS_LABELS.items():
        cur = ex.get(key, VISIBILITY_DEFAULTS[key])
        mark = "✅" if cur else "❌"
        rows.append([InlineKeyboardButton(
            text=f"{mark} {label}", callback_data=f"con:toggle:{key}"
        )])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="con:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Запуск конструктора ────────────────────────────────────────────────────

async def start_constructor(message: Message, state: FSMContext, db: Database, bot_username: str = "reviewbot"):
    count = await db.count_custom_templates(message.from_user.id)
    if count >= MAX_TEMPLATES:
        await message.answer(
            f"❌ У тебя уже {MAX_TEMPLATES} шаблонов — это максимум. "
            f"Удали один из старых чтобы создать новый."
        )
        return

    cfg = _default_cfg()
    await state.set_state(ConstructorSG.building)
    await state.update_data(cfg=cfg, preview_msg_id=None, bot_username=bot_username)
    await _send_or_update_preview(message, state, db, first=True)


async def start_constructor_edit(message: Message, state: FSMContext, db: Database,
                                  tpl: dict, bot_username: str = "reviewbot"):
    """Запускает конструктор с загруженным существующим шаблоном для редактирования."""
    cfg = {
        "layout": tpl["layout"],
        "font": tpl["font"],
        "title_font": tpl.get("title_font", "caveat"),
        "text_color": tpl["text_color"],
        "accent_color": tpl["accent_color"],
        "bg_color": tpl["bg_color"],
        "bg_image": tpl["bg_image"],
        "creator_username": tpl["creator_username"],
        "is_edited": tpl["is_edited"],
        "extra_cfg": tpl.get("extra_cfg") or {},
    }
    await state.set_state(ConstructorSG.building)
    await state.update_data(
        cfg=cfg, preview_msg_id=None, bot_username=bot_username,
        edit_template_id=tpl["id"],          # id редактируемого шаблона
        edit_template_name=tpl["name"],
        is_received=(tpl["owner_id"] != tpl["creator_id"]),  # чужой ли шаблон
    )
    await _send_or_update_preview(message, state, db, first=True)


async def _send_or_update_preview(message_or_call, state: FSMContext, db: Database, first=False):
    data = await state.get_data()
    cfg = data["cfg"]

    if isinstance(message_or_call, CallbackQuery):
        chat = message_or_call.message.chat
        bot = message_or_call.bot
    else:
        chat = message_or_call.chat
        bot = message_or_call.bot

    bot_username = data.get("bot_username", "reviewbot")
    png = await render_preview(cfg, bot_username)
    media = InputMediaPhoto(
        media=BufferedInputFile(png, filename="preview.png"),
        caption="🎨 <b>Конструктор карточки</b>\n\nНастраивай элементы — превью обновляется вживую."
    )

    preview_id = data.get("preview_msg_id")
    if preview_id:
        try:
            await bot.edit_message_media(
                chat_id=chat.id, message_id=preview_id,
                media=media, reply_markup=kb_constructor_main()
            )
            return
        except Exception:
            pass

    sent = await bot.send_photo(
        chat_id=chat.id,
        photo=BufferedInputFile(png, filename="preview.png"),
        caption="🎨 <b>Конструктор карточки</b>\n\nНастраивай элементы — превью обновляется вживую.",
        reply_markup=kb_constructor_main()
    )
    await state.update_data(preview_msg_id=sent.message_id)


# ── Навигация по меню настроек ─────────────────────────────────────────────

MENU_MAP = {
    "layout": ("📐 Выбери раскладку:", LAYOUTS),
    "font": ("🔡 Шрифт основного текста:", FONTS),
    "title_font": ("🔤 Шрифт названия магазина:", FONTS),
    "text_color": ("🎨 Цвет текста:", TEXT_COLORS),
    "accent_color": ("✨ Цвет акцента (рамки, звёзды):", ACCENT_COLORS),
    "bg_color": ("🌌 Цвет фона:", BG_COLORS),
    "bg_gradient": ("🌈 Градиентный фон:", BG_GRADIENTS),
    "card_border": ("🔲 Рамка карточки:", CARD_BORDERS),
    "card_radius": ("⬜️ Скругление углов:", CARD_RADIUS),
    "card_shadow": ("🌫 Тень карточки:", CARD_SHADOW),
    "text_size": ("🔠 Размер текста отзыва:", TEXT_SIZES),
}


@router.callback_query(ConstructorSG.building, F.data.startswith("con:menu:"))
async def cb_open_menu(call: CallbackQuery, state: FSMContext):
    field = call.data.split(":")[2]
    await call.answer()

    if field == "bg_image":
        await state.set_state(ConstructorSG.enter_bg_image)
        await call.message.answer(
            "🖼 Пришли картинку которая станет фоном карточки.\n"
            "<i>Текст автоматически затемнится для читаемости. "
            "Отправь /skip чтобы вернуться без картинки.</i>"
        )
        return

    if field == "visibility":
        data = await state.get_data()
        cfg = data["cfg"]
        await call.message.answer(
            "👁 <b>Показать / скрыть блоки</b>\n\nНажми чтобы переключить:",
            reply_markup=_kb_visibility(cfg)
        )
        return

    title, options = MENU_MAP[field]
    await call.message.answer(title, reply_markup=_kb_options(field, options))


@router.callback_query(ConstructorSG.building, F.data.startswith("con:set:"))
async def cb_set_option(call: CallbackQuery, state: FSMContext, db: Database):
    _, _, field, value = call.data.split(":", 3)
    data = await state.get_data()
    cfg = data["cfg"]

    # Вычисляем новое значение и сравниваем с текущим
    if field == "text_color":
        new_val = TEXT_COLORS[value][1]
        changed = cfg.get("text_color") != new_val
        cfg["text_color"] = new_val
    elif field == "accent_color":
        new_val = ACCENT_COLORS[value][1]
        changed = cfg.get("accent_color") != new_val
        cfg["accent_color"] = new_val
    elif field == "bg_color":
        new_val = BG_COLORS[value][1]
        changed = cfg.get("bg_color") != new_val or cfg.get("bg_image") is not None
        cfg["bg_color"] = new_val
        cfg["bg_image"] = None  # цвет отменяет картинку
    elif field == "layout":
        changed = cfg.get("layout") != value
        cfg["layout"] = value
    elif field == "font":
        changed = cfg.get("font") != value
        cfg["font"] = value
    elif field == "title_font":
        changed = cfg.get("title_font") != value
        cfg["title_font"] = value
    elif field in ("bg_gradient", "card_border", "card_radius", "card_shadow", "text_size"):
        ex = cfg.setdefault("extra_cfg", {})
        changed = ex.get(field) != value
        ex[field] = value
        # Градиент отменяет картинку-фон
        if field == "bg_gradient" and value != "none":
            cfg["bg_image"] = None
    else:
        changed = False

    # Если ничего не изменилось — просто всплывающий тост, превью не трогаем
    if not changed:
        await call.answer("Уже выбрано ✓")
        return

    await state.update_data(cfg=cfg)
    await call.answer("✅ Применено")
    try:
        await call.message.delete()
    except Exception:
        pass
    await _send_or_update_preview(call, state, db)


@router.callback_query(ConstructorSG.building, F.data.startswith("con:toggle:"))
async def cb_toggle_visibility(call: CallbackQuery, state: FSMContext, db: Database):
    key = call.data.split(":")[2]
    if key not in VISIBILITY_DEFAULTS:
        await call.answer()
        return
    data = await state.get_data()
    cfg = data["cfg"]
    ex = cfg.setdefault("extra_cfg", {})
    cur = ex.get(key, VISIBILITY_DEFAULTS[key])
    ex[key] = not cur
    await state.update_data(cfg=cfg)
    await call.answer("Переключено")
    # Обновляем клавиатуру тумблеров на месте
    try:
        await call.message.edit_reply_markup(reply_markup=_kb_visibility(cfg))
    except Exception:
        pass


@router.callback_query(ConstructorSG.building, F.data == "con:back")
async def cb_back_to_preview(call: CallbackQuery, state: FSMContext, db: Database):
    await call.answer()
    try:
        await call.message.delete()
    except Exception:
        pass


@router.message(ConstructorSG.enter_bg_image, F.photo)
async def cb_bg_image(message: Message, state: FSMContext, db: Database, bot):
    import base64
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    buf = await bot.download_file(file.file_path)
    raw = buf.read()
    b64 = base64.b64encode(raw).decode()

    data = await state.get_data()
    cfg = data["cfg"]
    cfg["bg_image"] = b64
    await state.update_data(cfg=cfg)
    await state.set_state(ConstructorSG.building)
    await message.answer("✅ Фон установлен!")
    await _send_or_update_preview(message, state, db)


@router.message(ConstructorSG.enter_bg_image, F.text == "/skip")
async def cb_bg_skip(message: Message, state: FSMContext, db: Database):
    await state.set_state(ConstructorSG.building)
    await _send_or_update_preview(message, state, db)


# ── Сохранение ─────────────────────────────────────────────────────────────

@router.callback_query(ConstructorSG.building, F.data == "con:save")
async def cb_save(call: CallbackQuery, state: FSMContext, db: Database):
    await call.answer()
    data = await state.get_data()

    # Режим редактирования существующего шаблона
    if data.get("edit_template_id"):
        cfg = data["cfg"]
        tpl_id = data["edit_template_id"]
        is_received = data.get("is_received", False)

        update_fields = {
            "layout": cfg["layout"],
            "font": cfg["font"],
            "title_font": cfg.get("title_font", "caveat"),
            "text_color": cfg["text_color"],
            "accent_color": cfg["accent_color"],
            "bg_color": cfg["bg_color"],
            "bg_image": cfg.get("bg_image"),
            "extra_cfg": cfg.get("extra_cfg", {}),
        }
        # Если редактируем полученный чужой шаблон — становимся владельцем
        if is_received:
            update_fields["is_edited"] = True

        await db.update_custom_template(tpl_id, **update_fields)
        await state.clear()

        note = ""
        if is_received:
            note = "\n\n👑 Ты отредактировал полученный шаблон — теперь ты его владелец, " \
                   "водяной знак создателя убран, можешь делиться им дальше."
        await call.message.answer(
            f"✅ Шаблон <b>«{data.get('edit_template_name', '')}»</b> обновлён!{note}"
        )
        return

    # Режим создания нового — спрашиваем название
    await state.set_state(ConstructorSG.enter_name)
    await call.message.answer("💾 Придумай название для шаблона:\n<i>Например: Мой золотой, Тёмная тема</i>")


@router.message(ConstructorSG.enter_name)
async def cb_save_name(message: Message, state: FSMContext, db: Database):
    name = message.text.strip()[:64]
    data = await state.get_data()
    cfg = data["cfg"]

    tpl = await db.create_custom_template(
        owner_id=message.from_user.id,
        creator_id=message.from_user.id,
        creator_username=message.from_user.username,
        name=name,
        layout=cfg["layout"],
        font=cfg["font"],
        title_font=cfg.get("title_font", "caveat"),
        text_color=cfg["text_color"],
        accent_color=cfg["accent_color"],
        bg_color=cfg["bg_color"],
        bg_image=cfg.get("bg_image"),
        extra_cfg=cfg.get("extra_cfg", {}),
        is_edited=False,
    )
    await state.clear()
    await message.answer(
        f"✅ Шаблон <b>«{name}»</b> сохранён!\n\n"
        f"Найдёшь его в меню «🎨 Мои шаблоны». Оттуда можно использовать, "
        f"поделиться или удалить."
    )


@router.callback_query(F.data == "con:cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer("Отменено")
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.message.answer("❌ Конструктор закрыт.")


@router.message(ConstructorSG.enter_key)
async def cb_enter_key(message: Message, state: FSMContext, db: Database):
    key = message.text.strip()
    if not key.startswith("!"):
        key = "!" + key
    await state.clear()
    from handlers.my_templates import claim_template
    ok, msg = await claim_template(message.from_user.id, message.from_user.username, key, db)
    await message.answer(msg)
