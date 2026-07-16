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
    enter_icon = State()


def _default_text(shop: str, pub_id: str, bot_username: str) -> str:
    return (f"⭐️ <b>{_h.escape(shop)}</b> просит оставить отзыв!\n\n"
            f"Нажми кнопку ниже, чтобы написать красивый отзыв.\n\n"
            f"💡 <i>Быстрее: напиши прямо в чате "
            f"<code>@{bot_username} {pub_id} твой отзыв</code> "
            f"и нажми на всплывающую картинку.</i>")


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
        [InlineKeyboardButton(text="✨ Иконка кнопки (премиум-эмодзи)"
                                   + (" ✅" if seller.get("invite_button_icon") else ""),
                              callback_data="invite:icon")],
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
        f"• <b>Кнопка:</b> {_h.escape(btn)}\n"
        f"• <b>Иконка кнопки:</b> {'премиум-эмодзи ✅' if seller.get('invite_button_icon') else 'нет'}\n"
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
        "Например: <code>💬 Оставить отзыв</code>, <code>⭐️ Написать отзыв</code>\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(InviteSG.enter_button, F.text == "/cancel")
async def invite_button_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(InviteSG.enter_button, F.text)
async def invite_button_input(message: Message, state: FSMContext, db: Database, config):
    btn = message.text.strip()[:64]
    if not btn:
        await message.answer("Пустое название. Пришли текст или /cancel.")
        return
    await state.clear()
    await db.update_seller(message.from_user.id, invite_button=btn)
    await message.answer(f"✅ Кнопка: <b>{_h.escape(btn)}</b>")
    await _show_menu(message, db, message.from_user.id, config.BOT_USERNAME)


@router.callback_query(F.data == "invite:icon")
async def cb_invite_icon(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(InviteSG.enter_icon)
    await call.message.answer(
        "✨ Пришли <b>премиум-эмодзи</b> — оно станет иконкой на кнопке приглашения.\n\n"
        "Просто отправь одно премиум-эмодзи сообщением. Бот возьмёт его ID.\n\n"
        "<i>Иконка показывается перед текстом кнопки. Работает благодаря Telegram "
        "Premium у владельца бота.</i>\n\n"
        "Отправь /off чтобы убрать иконку, /cancel — отменить."
    )


@router.message(InviteSG.enter_icon, F.text == "/cancel")
async def invite_icon_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(InviteSG.enter_icon, F.text == "/off")
async def invite_icon_off(message: Message, state: FSMContext, db: Database, config):
    await state.clear()
    await db.update_seller(message.from_user.id, invite_button_icon=None)
    await message.answer("✅ Иконка убрана.")
    await _show_menu(message, db, message.from_user.id, config.BOT_USERNAME)


@router.message(InviteSG.enter_icon, F.text)
async def invite_icon_input(message: Message, state: FSMContext, db: Database, config):
    # Достаём custom_emoji_id из премиум-эмодзи, которое прислал пользователь
    emoji_id = None
    for e in (message.entities or []):
        if e.type == "custom_emoji" and e.custom_emoji_id:
            emoji_id = e.custom_emoji_id
            break
    if not emoji_id:
        await message.answer(
            "⚠️ Это не премиум-эмодзи. Нужно именно <b>премиум</b> (анимированное) "
            "эмодзи — обычные не подойдут.\n\n"
            "Пришли ещё раз или /cancel."
        )
        return
    await state.clear()
    await db.update_seller(message.from_user.id, invite_button_icon=emoji_id)
    await message.answer("✅ Иконка кнопки сохранена! Глянь «👁 Предпросмотр».")
    await _show_menu(message, db, message.from_user.id, config.BOT_USERNAME)


@router.message(InviteSG.enter_icon)
async def invite_icon_wrong(message: Message):
    await message.answer("Пришли премиум-эмодзи сообщением, /off — убрать, /cancel — отмена.")


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
