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
    CORNER_FILL, VISIBILITY_DEFAULTS,
    TITLE_EFFECTS, BG_TEXTURES, PRESETS,
    VERIFIED_BADGES, CORNER_ORNAMENTS,
    render_preview
)

import logging
router = Router()
logger = logging.getLogger(__name__)

MAX_TEMPLATES = 15


class ConstructorSG(StatesGroup):
    building = State()      # активное конструирование с превью
    enter_bg_image = State()
    enter_logo = State()
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
        [InlineKeyboardButton(text="✨ Готовые стили (пресеты)", callback_data="con:menu:presets")],
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
        [InlineKeyboardButton(text="🎨 Заливка углов", callback_data="con:menu:corner_fill"),
         InlineKeyboardButton(text="🌫 Тень", callback_data="con:menu:card_shadow")],
        [InlineKeyboardButton(text="💫 Эффект названия", callback_data="con:menu:title_effect"),
         InlineKeyboardButton(text="🧩 Текстура фона", callback_data="con:menu:bg_texture")],
        [InlineKeyboardButton(text="🔠 Размер текста", callback_data="con:menu:text_size")],
        [InlineKeyboardButton(text="👁 Показать/скрыть блоки", callback_data="con:menu:visibility")],
        [InlineKeyboardButton(text="✅ Бейдж продавца", callback_data="con:menu:verified_badge"),
         InlineKeyboardButton(text="⌜ Орнамент углов", callback_data="con:menu:corner_ornament")],
        [InlineKeyboardButton(text="🖼 Логотип магазина", callback_data="con:menu:logo")],
        [InlineKeyboardButton(text="💾 Сохранить шаблон", callback_data="con:save")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="con:cancel")],
    ])


def _kb_options(prefix: str, options: dict, back="con:back", cols: int = 1) -> InlineKeyboardMarkup:
    rows = []
    items = list(options.items())
    row = []
    for key, val in items:
        label = val[0] if isinstance(val, tuple) else val
        row.append(InlineKeyboardButton(text=label, callback_data=f"con:set:{prefix}:{key}"))
        if len(row) >= cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="« Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


VIS_LABELS = {
    "show_date": "📅 Дата",
    "show_item": "📦 Товар",
    "show_avatar": "👤 Аватар",
    "show_seller_tag": "🏷 Тег продавца",
    "show_quote": "❝ Кавычки",
}


def _kb_presets() -> InlineKeyboardMarkup:
    rows = []
    for key, preset in PRESETS.items():
        rows.append([InlineKeyboardButton(text=preset["label"], callback_data=f"con:preset:{key}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="con:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


async def _refresh_preview_in_place(call: CallbackQuery, state: FSMContext, db: Database,
                                    keyboard=None):
    """Перерисовывает превью в существующем сообщении.
    keyboard=None — возвращает главное меню конструктора;
    иначе оставляет переданную клавиатуру (например меню выбора цвета),
    чтобы можно было удобно перебирать варианты подряд."""
    data = await state.get_data()
    cfg = data["cfg"]
    bot_username = data.get("bot_username", "reviewbot")
    preview_id = data.get("preview_msg_id")
    chat_id = call.message.chat.id

    kb = keyboard if keyboard is not None else kb_constructor_main()

    png = await render_preview(cfg, bot_username)
    media = InputMediaPhoto(
        media=BufferedInputFile(png, filename="preview.png"),
        caption="🎨 <b>Конструктор карточки</b>\n\nНастраивай элементы — превью обновляется вживую."
    )
    if preview_id:
        try:
            await call.bot.edit_message_media(
                chat_id=chat_id, message_id=preview_id,
                media=media, reply_markup=kb
            )
            return
        except Exception:
            pass
    # Фолбэк — отправляем заново
    sent = await call.bot.send_photo(
        chat_id=chat_id,
        photo=BufferedInputFile(png, filename="preview.png"),
        caption="🎨 <b>Конструктор карточки</b>\n\nНастраивай элементы — превью обновляется вживую.",
        reply_markup=kb
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
    "corner_fill": ("🎨 Заливка углов (когда скруглены):", CORNER_FILL),
    "title_effect": ("💫 Эффект названия магазина:", TITLE_EFFECTS),
    "bg_texture": ("🧩 Текстура фона:", BG_TEXTURES),
    "verified_badge": ("✅ Бейдж проверенного продавца:", VERIFIED_BADGES),
    "corner_ornament": ("⌜ Орнамент углов карточки:", CORNER_ORNAMENTS),
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

    if field == "logo":
        await state.set_state(ConstructorSG.enter_logo)
        await call.message.answer(
            "🖼 Пришли логотип магазина.\n\n"
            "💡 <b>Важно про прозрачность:</b> если в логотипе прозрачный фон, "
            "отправь его как <b>файл</b> (📎 → «Файл» / «Без сжатия»), а не как фото — "
            "иначе Telegram зальёт прозрачность белым.\n\n"
            "<i>Он появится над названием. Отправь /skip чтобы убрать логотип.</i>"
        )
        return

    if field == "presets":
        data = await state.get_data()
        preview_id = data.get("preview_msg_id")
        kb = _kb_presets()
        if preview_id:
            try:
                await call.bot.edit_message_reply_markup(
                    chat_id=call.message.chat.id, message_id=preview_id, reply_markup=kb)
                return
            except Exception:
                pass
        await call.message.answer("✨ <b>Готовые стили</b>", reply_markup=kb)
        return

    if field == "visibility":
        data = await state.get_data()
        cfg = data["cfg"]
        preview_id = data.get("preview_msg_id")
        kb = _kb_visibility(cfg)
        if preview_id:
            try:
                await call.bot.edit_message_reply_markup(
                    chat_id=call.message.chat.id, message_id=preview_id, reply_markup=kb)
                return
            except Exception:
                pass
        await call.message.answer("👁 <b>Показать / скрыть блоки</b>", reply_markup=kb)
        return

    title, options = MENU_MAP[field]
    data = await state.get_data()
    preview_id = data.get("preview_msg_id")
    # Цветовые палитры — в 2 колонки, чтобы меню было компактным
    color_fields = {"text_color", "accent_color", "bg_color"}
    cols = 2 if field in color_fields else 1
    kb = _kb_options(field, options, cols=cols)
    # Редактируем клавиатуру самого превью-сообщения (не плодим новые сообщения)
    if preview_id:
        try:
            await call.bot.edit_message_reply_markup(
                chat_id=call.message.chat.id, message_id=preview_id, reply_markup=kb)
            return
        except Exception:
            pass
    await call.message.answer(title, reply_markup=kb)


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
        ex = cfg.setdefault("extra_cfg", {})
        changed = (cfg.get("bg_color") != new_val or cfg.get("bg_image") is not None
                   or ex.get("bg_gradient", "none") != "none")
        cfg["bg_color"] = new_val
        cfg["bg_image"] = None       # цвет отменяет картинку
        ex["bg_gradient"] = "none"   # и градиент — иначе он перекрыл бы цвет
    elif field == "layout":
        changed = cfg.get("layout") != value
        cfg["layout"] = value
    elif field == "font":
        changed = cfg.get("font") != value
        cfg["font"] = value
    elif field == "title_font":
        changed = cfg.get("title_font") != value
        cfg["title_font"] = value
    elif field in ("bg_gradient", "card_border", "card_radius", "card_shadow", "text_size", "corner_fill",
                   "title_effect", "bg_texture", "verified_badge", "corner_ornament"):
        ex = cfg.setdefault("extra_cfg", {})
        changed = ex.get(field) != value
        ex[field] = value
        # Градиент отменяет картинку-фон
        if field == "bg_gradient" and value != "none":
            cfg["bg_image"] = None
    else:
        changed = False

    await state.update_data(cfg=cfg)
    if not changed:
        await call.answer("Уже выбрано ✓")
    else:
        await call.answer("✅ Применено")
    # Перерисовываем превью, НО оставляем то же меню выбора открытым —
    # чтобы можно было перебирать варианты подряд (цвета, шрифты и т.д.)
    if field in MENU_MAP:
        _, options = MENU_MAP[field]
        color_fields = {"text_color", "accent_color", "bg_color"}
        cols = 2 if field in color_fields else 1
        same_menu = _kb_options(field, options, cols=cols)
        await _refresh_preview_in_place(call, state, db, keyboard=same_menu)
    else:
        await _refresh_preview_in_place(call, state, db)


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
    # Перерисовываем превью + оставляем клавиатуру тумблеров
    bot_username = data.get("bot_username", "reviewbot")
    preview_id = data.get("preview_msg_id")
    png = await render_preview(cfg, bot_username)
    media = InputMediaPhoto(
        media=BufferedInputFile(png, filename="preview.png"),
        caption="👁 <b>Показать / скрыть блоки</b>\n\nНажми чтобы переключить. «Назад» — к меню."
    )
    try:
        await call.bot.edit_message_media(
            chat_id=call.message.chat.id, message_id=preview_id,
            media=media, reply_markup=_kb_visibility(cfg)
        )
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=_kb_visibility(cfg))
        except Exception:
            pass


@router.callback_query(ConstructorSG.building, F.data.startswith("con:preset:"))
async def cb_apply_preset(call: CallbackQuery, state: FSMContext, db: Database):
    key = call.data.split(":")[2]
    preset = PRESETS.get(key)
    if not preset:
        await call.answer("Стиль не найден", show_alert=True)
        return
    data = await state.get_data()
    cfg = data["cfg"]
    # Применяем базовые поля
    for k, v in preset["base"].items():
        cfg[k] = v
    # Картинка-фон отменяется пресетом
    cfg["bg_image"] = None
    # Применяем extra_cfg (сохраняя тумблеры видимости пользователя)
    ex = cfg.setdefault("extra_cfg", {})
    vis_keys = set(VISIBILITY_DEFAULTS.keys())
    saved_vis = {k: ex[k] for k in vis_keys if k in ex}
    ex.update(preset["extra"])
    ex.update(saved_vis)  # видимость блоков не трогаем
    await state.update_data(cfg=cfg)
    await call.answer(f"✅ Применён стиль: {preset['label']}")
    await _refresh_preview_in_place(call, state, db)


@router.callback_query(ConstructorSG.building, F.data == "con:back")
async def cb_back_to_preview(call: CallbackQuery, state: FSMContext, db: Database):
    await call.answer()
    data = await state.get_data()
    preview_id = data.get("preview_msg_id")
    # Просто возвращаем главное меню на превью-сообщение
    if preview_id:
        try:
            await call.bot.edit_message_reply_markup(
                chat_id=call.message.chat.id, message_id=preview_id,
                reply_markup=kb_constructor_main()
            )
            return
        except Exception:
            pass
    # Фолбэк — перерисовать
    await _refresh_preview_in_place(call, state, db)


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
    # Убираем загруженную картинку из чата, чтобы не мешала настройке
    try:
        await message.delete()
    except Exception:
        pass
    await _send_or_update_preview(message, state, db)


@router.message(ConstructorSG.enter_bg_image, F.text == "/skip")
async def cb_bg_skip(message: Message, state: FSMContext, db: Database):
    await state.set_state(ConstructorSG.building)
    await _send_or_update_preview(message, state, db)


async def _process_logo(message: Message, state: FSMContext, db: Database, raw: bytes):
    """Общая обработка логотипа: сжатие (с сохранением прозрачности) и сохранение."""
    import base64, io
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(raw)).convert("RGBA")  # RGBA сохраняет альфа-канал
        im.thumbnail((240, 240), Image.LANCZOS)
        out = io.BytesIO()
        im.save(out, format="PNG", optimize=True)  # PNG сохраняет прозрачность
        raw = out.getvalue()
    except Exception:
        await message.answer("❌ Не удалось обработать картинку. Пришли PNG или JPG.")
        return

    b64 = base64.b64encode(raw).decode()
    if len(b64) > 200_000:
        await message.answer("❌ Логотип слишком большой. Пришли картинку поменьше.")
        return

    data = await state.get_data()
    cfg = data["cfg"]
    ex = cfg.setdefault("extra_cfg", {})
    ex["logo_b64"] = b64
    await state.update_data(cfg=cfg)
    await state.set_state(ConstructorSG.building)
    # Убираем загруженную картинку/файл из чата, чтобы не мешала настройке
    try:
        await message.delete()
    except Exception:
        pass
    await _send_or_update_preview(message, state, db)


@router.message(ConstructorSG.enter_logo, F.photo)
async def cb_logo_photo(message: Message, state: FSMContext, db: Database, bot):
    # Фото — Telegram уже убрал прозрачность, но логотип всё равно встроится
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    buf = await bot.download_file(file.file_path)
    await _process_logo(message, state, db, buf.read())


@router.message(ConstructorSG.enter_logo, F.document)
async def cb_logo_document(message: Message, state: FSMContext, db: Database, bot):
    logger.info(f"[LOGO] Документ получен в состоянии enter_logo от {message.from_user.id}")
    # Документ сохраняет прозрачность PNG
    doc = message.document
    mime = (doc.mime_type or "").lower()
    name = (doc.file_name or "").lower()
    logger.info(f"[LOGO] mime={mime}, name={name}, size={doc.file_size}")
    is_image = mime.startswith("image/") or name.endswith((".png", ".jpg", ".jpeg", ".webp"))
    if not is_image:
        await message.answer("❌ Это не картинка. Пришли логотип в формате PNG, JPG или WEBP.")
        return
    # Защита: не качаем гигантские файлы
    if doc.file_size and doc.file_size > 5 * 1024 * 1024:
        await message.answer("❌ Файл слишком большой (макс. 5 МБ). Пришли логотип поменьше.")
        return
    file = await bot.get_file(doc.file_id)
    buf = await bot.download_file(file.file_path)
    await _process_logo(message, state, db, buf.read())


@router.message(ConstructorSG.enter_logo, F.text == "/skip")
async def cb_logo_skip(message: Message, state: FSMContext, db: Database):
    data = await state.get_data()
    cfg = data["cfg"]
    ex = cfg.setdefault("extra_cfg", {})
    ex.pop("logo_b64", None)  # /skip убирает логотип
    await state.update_data(cfg=cfg)
    await state.set_state(ConstructorSG.building)
    await _send_or_update_preview(message, state, db)


@router.message(ConstructorSG.enter_logo, F.sticker)
async def cb_logo_sticker(message: Message, state: FSMContext, db: Database, bot):
    # Telegram превращает .webp в стикер! Скачиваем и обрабатываем как логотип.
    logger.info(f"[LOGO] Стикер в enter_logo, animated={message.sticker.is_animated}, video={message.sticker.is_video}")
    st = message.sticker
    if st.is_animated or st.is_video:
        await message.answer("❌ Анимированный стикер не подойдёт. Пришли статичную картинку (PNG-файл).")
        return
    file = await bot.get_file(st.file_id)
    buf = await bot.download_file(file.file_path)
    await _process_logo(message, state, db, buf.read())


@router.message(ConstructorSG.enter_logo)
async def cb_logo_fallback(message: Message, state: FSMContext, db: Database, bot):
    # Ловим ВСЁ остальное в состоянии логотипа — покажем что именно пришло
    ct = message.content_type
    logger.info(f"[LOGO] Fallback: content_type={ct}")
    await message.answer(
        f"⚠️ Я ожидаю картинку, а получил: <b>{ct}</b>.\n\n"
        "Пришли логотип как обычную картинку или PNG-файл. Или /skip чтобы убрать логотип."
    )


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


# ВРЕМЕННАЯ ДИАГНОСТИКА: ловим документ в любом состоянии конструктора
@router.message(F.document)
async def _diag_any_document(message: Message, state: FSMContext):
    cur = await state.get_state()
    logger.info(f"[DIAG] Документ пришёл, текущее состояние FSM = {cur}, "
                f"mime={message.document.mime_type}, name={message.document.file_name}")
    if cur is None:
        await message.answer(
            "📎 Я получил файл, но сейчас не жду логотип. "
            "Открой конструктор → «🖼 Логотип магазина» и пришли файл там."
        )
