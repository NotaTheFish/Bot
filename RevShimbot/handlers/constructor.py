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
        "text_color": "#f0f0f0",
        "accent_color": "#c9a84c",
        "bg_color": "#1a1a2e",
        "bg_image": None,
        "creator_username": None,
        "is_edited": False,
    }


# ── Клавиатуры конструктора ────────────────────────────────────────────────

def kb_constructor_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📐 Раскладка", callback_data="con:menu:layout"),
         InlineKeyboardButton(text="🔤 Шрифт", callback_data="con:menu:font")],
        [InlineKeyboardButton(text="🎨 Цвет текста", callback_data="con:menu:text_color"),
         InlineKeyboardButton(text="✨ Акцент", callback_data="con:menu:accent_color")],
        [InlineKeyboardButton(text="🌌 Фон (цвет)", callback_data="con:menu:bg_color"),
         InlineKeyboardButton(text="🖼 Фон (картинка)", callback_data="con:menu:bg_image")],
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
    "font": ("🔤 Выбери шрифт:", FONTS),
    "text_color": ("🎨 Цвет текста:", TEXT_COLORS),
    "accent_color": ("✨ Цвет акцента (рамки, звёзды):", ACCENT_COLORS),
    "bg_color": ("🌌 Цвет фона:", BG_COLORS),
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
async def cb_save(call: CallbackQuery, state: FSMContext):
    await call.answer()
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
        text_color=cfg["text_color"],
        accent_color=cfg["accent_color"],
        bg_color=cfg["bg_color"],
        bg_image=cfg.get("bg_image"),
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
