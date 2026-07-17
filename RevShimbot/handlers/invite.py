"""Настройка приглашения (пасты) для инлайна: профиль → Настройка приглашения."""
import logging
import html as _h
from aiogram import Router, F
from aiogram.types import (Message, CallbackQuery, InlineKeyboardMarkup,
                           InlineKeyboardButton)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database

router = Router()
logger = logging.getLogger(__name__)

DEFAULT_BUTTON = "✍️ Написать отзыв в боте"


def _invite_btn(text: str, url: str, icon_id=None) -> InlineKeyboardButton:
    """Кнопка приглашения. icon_custom_emoji_id — премиум-иконка перед текстом
    (Bot API 9.4+; работает, если у владельца бота есть Telegram Premium)."""
    if icon_id:
        try:
            return InlineKeyboardButton(text=text[:64], url=url,
                                        icon_custom_emoji_id=icon_id)
        except Exception:
            pass
    return InlineKeyboardButton(text=text[:64], url=url)


class InviteSG(StatesGroup):
    enter_text = State()
    enter_button = State()
    enter_photo = State()


def _default_text(shop: str, pub_id: str, bot_username: str) -> str:    return (f"⭐️ <b>{_h.escape(shop)}</b> просит оставить отзыв!\n\n"
            f"Нажми кнопку ниже, чтобы написать красивый отзыв.\n\n"
            f"💡 <i>Быстрее: напиши прямо в чате "
            f"<code>@{bot_username} {pub_id} твой отзыв</code> "
            f"и нажми на всплывающую картинку.</i>")


def build_invite_caption(seller: dict, bot_username: str) -> str:
    """Собирает текст приглашения: свой (с премиум-эмодзи) или стандартный.
    Плейсхолдеры {shop} и {id} подставляются. Общая точка для инлайна и предпросмотра."""
    custom = seller.get("invite_text")
    if custom:
        return (custom
                .replace("{shop}", _h.escape(seller["shop_name"]))
                .replace("{id}", _h.escape(str(seller["pub_id"]))))
    return _default_text(seller["shop_name"], seller["pub_id"], bot_username)


def build_invite_kb(seller: dict, bot_username: str) -> InlineKeyboardMarkup:
    """Кнопка приглашения с премиум-иконкой (если задана)."""
    ref_link = f"https://t.me/{bot_username}?start=seller_{seller['pub_id']}"
    btn = seller.get("invite_button") or DEFAULT_BUTTON
    return InlineKeyboardMarkup(inline_keyboard=[[
        _invite_btn(btn, ref_link, seller.get("invite_button_icon"))
    ]])


def _kb_invite(seller: dict) -> InlineKeyboardMarkup:
    has_text = bool(seller.get("invite_text"))
    has_btn = bool(seller.get("invite_button"))
    has_photo = bool(seller.get("invite_photo"))
    has_tpl = bool(seller.get("invite_template_id"))
    rows = [
        [InlineKeyboardButton(text="✏️ Текст приглашения" + (" ✅" if has_text else ""),
                              callback_data="invite:text")],
        [InlineKeyboardButton(text="🔘 Название кнопки" + (" ✅" if has_btn else ""),
                              callback_data="invite:button")],
        [InlineKeyboardButton(text="🎨 Превью: карточка" + (" ✅" if has_tpl and not has_photo else ""),
                              callback_data="invite:tplpick")],
        [InlineKeyboardButton(text="📷 Превью: своё фото" + (" ✅" if has_photo else ""),
                              callback_data="invite:photo")],
    ]
    if has_text or has_btn or has_photo or has_tpl:
        rows.append([InlineKeyboardButton(text="🗑 Сбросить всё к стандартному",
                                          callback_data="invite:reset")])
    rows.append([InlineKeyboardButton(text="👁 Предпросмотр", callback_data="invite:preview")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:profile")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _invite_text(seller: dict, bot_username: str) -> str:
    txt = seller.get("invite_text")
    btn = seller.get("invite_button") or DEFAULT_BUTTON
    photo = seller.get("invite_photo")
    tpl = seller.get("invite_template_id")

    if photo:
        preview_src = "своё фото"
    elif tpl:
        preview_src = "карточка (выбран шаблон)"
    else:
        preview_src = "карточка (шаблон по умолчанию)"

    return (
        "📨 <b>Настройка приглашения</b>\n\n"
        f"Когда ты пишешь <code>@{bot_username} {seller.get('pub_id','ID')}</code> "
        "в чате с клиентом, бот показывает приглашение оставить отзыв. "
        "Здесь можно настроить его вид.\n\n"
        f"• <b>Текст:</b> {'свой ✅' if txt else 'стандартный'}\n"
        f"• <b>Кнопка:</b> {'✨ ' if seller.get('invite_button_icon') else ''}{_h.escape(btn)}\n"
        f"• <b>Превью:</b> {preview_src}\n\n"
        "<i>В тексте можно использовать премиум-эмодзи и плейсхолдеры "
        "<code>{shop}</code> (название магазина) и <code>{id}</code> (твой ID).</i>"
    )


async def _show_menu(message: Message, db: Database, user_id: int, bot_username: str):
    seller = await db.get_seller(user_id)
    if not seller:
        await message.answer("Сначала настрой торговый шаблон.")
        return
    await message.answer(_invite_text(seller, bot_username),
                         reply_markup=_kb_invite(seller))


@router.callback_query(F.data == "invite:menu")
async def cb_invite_menu(call: CallbackQuery, db: Database, config):
    await call.answer()
    await _show_menu(call.message, db, call.from_user.id, config.BOT_USERNAME)


@router.callback_query(F.data == "invite:text")
async def cb_invite_text(call: CallbackQuery, state: FSMContext, config):
    await call.answer()
    await state.set_state(InviteSG.enter_text)
    await call.message.answer(
        "✏️ Пришли свой текст приглашения.\n\n"
        "Можно использовать премиум-эмодзи и форматирование.\n\n"
        "<b>Плейсхолдеры:</b>\n"
        "• <code>{shop}</code> — название твоего магазина\n"
        "• <code>{id}</code> — твой ID для отзыва\n\n"
        "<b>Пример:</b>\n"
        "<code>🍄 Оставь отзыв о {shop}!\n"
        "Или напиши: @" + config.BOT_USERNAME + " {id} твой отзыв</code>\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(InviteSG.enter_text, F.text == "/cancel")
async def invite_text_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(InviteSG.enter_text, F.text)
async def invite_text_input(message: Message, state: FSMContext, db: Database, config):
    if len(message.text) > 900:
        await message.answer("Слишком длинный текст (макс 900 символов). Попробуй короче.")
        return
    # html_text сохраняет премиум-эмодзи как <tg-emoji emoji-id="...">
    tpl_html = message.html_text
    await state.clear()
    await db.update_seller(message.from_user.id, invite_text=tpl_html)
    await message.answer("✅ Текст приглашения сохранён.")
    await _show_menu(message, db, message.from_user.id, config.BOT_USERNAME)


@router.callback_query(F.data == "invite:button")
async def cb_invite_button(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(InviteSG.enter_button)
    await call.message.answer(
        "🔘 Пришли название кнопки под приглашением.\n\n"
        f"Сейчас: <b>{DEFAULT_BUTTON}</b>\n\n"
        "✨ <b>Можно с премиум-эмодзи</b> — просто вставь его в название, "
        "бот подхватит автоматически.\n\n"
        "⚠️ <i>Важно: Telegram показывает премиум-эмодзи только как иконку "
        "<b>слева</b> от текста — куда бы ты его ни поставил. Обычные эмодзи "
        "остаются на своих местах.</i>\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(InviteSG.enter_button, F.text == "/cancel")
async def invite_button_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


def _split_premium_icon(text: str, entities):
    """Вынимает первое премиум-эмодзи из текста: возвращает (текст_без_него, emoji_id).
    Кнопки Telegram не поддерживают эмодзи внутри текста — только иконку слева
    (icon_custom_emoji_id), поэтому эмодзи вырезаем и передаём отдельным полем.
    Позиции entity считаются в UTF-16, поэтому режем в UTF-16."""
    icon = None
    for e in (entities or []):
        if e.type == "custom_emoji" and e.custom_emoji_id:
            icon = e
            break
    if not icon:
        return text.strip(), None
    b = text.encode("utf-16-le")
    cut = (b[:icon.offset * 2] + b[(icon.offset + icon.length) * 2:])
    return cut.decode("utf-16-le").strip(), icon.custom_emoji_id


@router.message(InviteSG.enter_button, F.text)
async def invite_button_input(message: Message, state: FSMContext, db: Database, config):
    # Премиум-эмодзи из названия становится иконкой кнопки
    btn, icon_id = _split_premium_icon(message.text, message.entities)
    btn = btn[:64]
    if not btn:
        await message.answer("Нужен ещё и текст кнопки, не только эмодзи. "
                             "Пришли название или /cancel.")
        return
    await state.clear()
    await db.update_seller(message.from_user.id, invite_button=btn,
                           invite_button_icon=icon_id)
    note = "\n✨ Премиум-эмодзи подхвачено как иконка кнопки." if icon_id else ""
    await message.answer(f"✅ Кнопка: <b>{_h.escape(btn)}</b>{note}")
    await _show_menu(message, db, message.from_user.id, config.BOT_USERNAME)


@router.callback_query(F.data == "invite:photo")
async def cb_invite_photo(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(InviteSG.enter_photo)
    await call.message.answer(
        "📷 Пришли фото для превью приглашения.\n\n"
        "Оно заменит карточку-превью. Чтобы вернуться к карточке — "
        "нажми «🎨 Превью: карточка» в меню.\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(InviteSG.enter_photo, F.text == "/cancel")
async def invite_photo_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(InviteSG.enter_photo, F.photo)
async def invite_photo_input(message: Message, state: FSMContext, db: Database, config):
    photo = message.photo[-1]
    if photo.file_size and photo.file_size > 10 * 1024 * 1024:
        await message.answer("⚠️ Фото слишком большое (макс 10 МБ).")
        return
    # Храним file_id — инлайн отдаёт его напрямую, без перегенерации
    await state.clear()
    await db.update_seller(message.from_user.id, invite_photo=photo.file_id)
    await message.answer("✅ Фото для приглашения сохранено.")
    await _show_menu(message, db, message.from_user.id, config.BOT_USERNAME)


@router.message(InviteSG.enter_photo)
async def invite_photo_wrong(message: Message):
    await message.answer("Пришли именно фото или /cancel.")


def _kb_tpl_pick(selected, customs):
    from constants import TEMPLATES
    rows = []
    for tid, label in TEMPLATES.items():
        mark = "✅ " if tid == selected else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{label}",
                                          callback_data=f"invtpl:{tid}")])
    for tpl in customs or []:
        own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
        cid = f"custom_{tpl['id']}"
        mark = "✅ " if selected == cid else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{own} {tpl['name']}",
                                          callback_data=f"invtpl:{cid}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="invite:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "invite:tplpick")
async def cb_invite_tplpick(call: CallbackQuery, db: Database):
    await call.answer()
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    customs = await db.list_custom_templates(call.from_user.id)
    selected = seller.get("invite_template_id")
    await call.message.answer(
        "🎨 Выбери карточку для превью приглашения.\n\n"
        "<i>Выбор карточки уберёт своё фото, если оно было задано.</i>",
        reply_markup=_kb_tpl_pick(selected, customs))


@router.callback_query(F.data.startswith("invtpl:"))
async def cb_invite_tpl_set(call: CallbackQuery, db: Database):
    tid = call.data.split(":", 1)[1]
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    # Выбор карточки отменяет своё фото (иначе фото было бы приоритетнее)
    await db.update_seller(call.from_user.id, invite_template_id=tid, invite_photo=None)
    await call.answer("✅ Карточка для приглашения обновлена")
    customs = await db.list_custom_templates(call.from_user.id)
    try:
        await call.message.edit_reply_markup(reply_markup=_kb_tpl_pick(tid, customs))
    except Exception:
        pass


@router.callback_query(F.data == "invite:reset")
async def cb_invite_reset(call: CallbackQuery, db: Database, config):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    await db.update_seller(call.from_user.id, invite_text=None, invite_button=None,
                           invite_photo=None, invite_template_id=None,
                           invite_button_icon=None)
    await call.answer("🗑 Сброшено к стандартному")
    seller = await db.get_seller(call.from_user.id)
    try:
        await call.message.edit_text(_invite_text(seller, config.BOT_USERNAME),
                                     reply_markup=_kb_invite(seller))
    except Exception:
        pass


@router.callback_query(F.data == "invite:preview")
async def cb_invite_preview(call: CallbackQuery, db: Database, bot, config):
    """Показывает приглашение так, как его увидит клиент."""
    await call.answer()
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        return
    ref_link = f"https://t.me/{config.BOT_USERNAME}?start=seller_{seller['pub_id']}"
    btn = seller.get("invite_button") or DEFAULT_BUTTON
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        _invite_btn(btn, ref_link, seller.get("invite_button_icon"))
    ]])
    custom = seller.get("invite_text")
    if custom:
        caption = (custom
                   .replace("{shop}", _h.escape(seller["shop_name"]))
                   .replace("{id}", _h.escape(str(seller["pub_id"]))))
    else:
        caption = _default_text(seller["shop_name"], seller["pub_id"], config.BOT_USERNAME)

    photo = seller.get("invite_photo")
    try:
        if photo:
            await bot.send_photo(call.from_user.id, photo=photo, caption=caption,
                                 reply_markup=kb, parse_mode="HTML")
        else:
            # Генерируем карточку-превью тем же способом, что и инлайн
            from services.card_generator import generate_card
            from aiogram.types import BufferedInputFile
            preview_tpl = (seller.get("invite_template_id")
                           or seller.get("inline_template_id")
                           or seller["template_id"])
            img = await generate_card({
                "shop_name": seller["shop_name"],
                "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID:{seller['pub_id']}",
                "buyer_name": "Трейдер", "buyer_initials": "ТР",
                "review_text": "Здесь появится твой отзыв о продавце ✍️",
                "item_bought": seller["item_value"] if seller["item_mode"] == "fixed" else "",
                "stars": seller["stars_value"] if seller["stars_mode"] == "fixed" else 5,
                "stars_mode": seller["stars_mode"], "template_id": preview_tpl,
                "avatar_bytes": None, "bot_username": config.BOT_USERNAME, "db": db,
            })
            await bot.send_photo(call.from_user.id,
                                 photo=BufferedInputFile(img, filename="invite.png"),
                                 caption=caption, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Предпросмотр приглашения не удался: {e}")
        await call.message.answer("⚠️ Не удалось показать предпросмотр. "
                                  "Проверь текст — возможно, в нём битая разметка.")
