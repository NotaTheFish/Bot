from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database
from keyboards import kb_seller_menu, kb_templates, kb_template_view, kb_main_reply
from utils.helpers import get_ref_link
from constants import TEMPLATES

router = Router()

TEMPLATE_NAMES = {
    "classic_gold": "✦ Classic Gold",
    "retro_paper":  "📜 Retro Paper",
    "dark_slate":   "🖥 Dark Slate",
    "clean_white":  "🤍 Clean White",
    "sketch_paper": "✏️ Sketch Paper",
}
STARS_LABELS = {
    "fixed":        "🔒 Фиксированные",
    "buyer_choice": "⭐️ Покупатель выбирает",
    "disabled":     "🚫 Отключены",
}
ITEM_LABELS = {
    "fixed": "🔒 Фиксировано",
    "hint":  "💬 Подсказка",
    "free":  "✏️ Свободное",
}


def kb_start_menu(has_seller: bool = False, has_client: bool = False) -> InlineKeyboardMarkup:
    rows = []
    # «Создать» показываем только если шаблона ещё нет
    if not has_seller:
        rows.append([InlineKeyboardButton(text="🏪 Создать шаблон магазина", callback_data="start:seller")])
    if not has_client:
        rows.append([InlineKeyboardButton(text="🎨 Создать клиентский шаблон", callback_data="start:client")])

    rows.append([InlineKeyboardButton(text="⚡️ Быстрый отзыв", callback_data="start:quick")])
    rows.append([InlineKeyboardButton(text="🎨 Мои шаблоны (конструктор)", callback_data="start:mytemplates")])

    # «Мой шаблон» показываем когда он уже создан
    if has_seller:
        rows.append([InlineKeyboardButton(text="📋 Мой торговый шаблон", callback_data="menu:mytemplate")])
    if has_client:
        rows.append([InlineKeyboardButton(text="🎨 Мой клиентский шаблон", callback_data="menu:myclienttemplate")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


class QuickReviewSG(StatesGroup):
    choose_template = State()
    enter_seller = State()
    enter_item = State()
    choose_stars = State()
    enter_text = State()


@router.message(CommandStart())
async def cmd_start(message: Message, db: Database, bot, state: FSMContext):
    await state.clear()
    args = message.text.split()
    deep_link = args[1] if len(args) > 1 else None

    # Получение шаблона по реф-ссылке (одноразовый ключ)
    if deep_link and deep_link.startswith("tpl_"):
        key = "!" + deep_link.replace("tpl_", "")
        from handlers.my_templates import claim_template
        ok, msg = await claim_template(message.from_user.id, message.from_user.username, key, db)
        await message.answer(msg)
        if ok:
            from handlers.my_templates import show_templates_list
            await show_templates_list(message, db, message.from_user.id)
        return

    # Покупатель пришёл по реф-ссылке продавца
    if deep_link and deep_link.startswith("seller_"):
        raw = deep_link.replace("seller_", "")
        seller = None
        # Сначала пробуем как pub_id (GTEF), потом как числовой (обратная совместимость)
        if raw.isdigit():
            seller = await db.get_seller(int(raw))
        else:
            seller = await db.get_seller_by_pubid(raw.upper())
        if not seller:
            await message.answer("❌ Магазин не найден.")
            return
        await state.set_data({"seller_id": seller["id"], "mode": "buyer"})
        from handlers.review import start_review_flow
        await start_review_flow(message, seller, state, db)
        return

    await show_main_menu(message, db, message.from_user.id, message.from_user.first_name)


async def show_main_menu(message: Message, db: Database, user_id: int, first_name: str = None):
    """Показывает главное меню — используется и в /start, и в кнопке Главное меню."""
    name = first_name or "трейдер"
    has_seller = bool(await db.get_seller(user_id))
    has_client = bool(await db.get_client_template(user_id))

    # Сначала ставим постоянную reply-клавиатуру
    await message.answer(
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"Я <b>RevShimBot</b> — создаю красивые карточки отзывов для трейдеров "
        f"Creatures of Sonaria и Dragon Adventures.\n\n"
        f"Что хочешь сделать?",
        reply_markup=kb_main_reply()
    )
    # Затем инлайн-меню действий
    await message.answer(
        "Выбери действие 👇",
        reply_markup=kb_start_menu(has_seller, has_client)
    )


# ── Кнопки главного меню ──────────────────────────────────────────────────

@router.message(F.text == "🏠 Главное меню")
async def cb_main_menu_button(message: Message, db: Database, state: FSMContext):
    await state.clear()
    await show_main_menu(message, db, message.from_user.id, message.from_user.first_name)


@router.callback_query(F.data == "start:seller")
async def cb_start_seller(call: CallbackQuery, db: Database, state: FSMContext):
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    from handlers.setup import cmd_setup
    await cmd_setup(call.message, db, state)


@router.callback_query(F.data == "start:client")
async def cb_start_client(call: CallbackQuery, state: FSMContext, db: Database):
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    from handlers.client_setup import start_client_setup
    await start_client_setup(call.message, state, db, call.from_user.id)


@router.callback_query(F.data == "start:quick")
async def cb_start_quick(call: CallbackQuery, state: FSMContext, db: Database):
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    await state.set_state(QuickReviewSG.choose_template)
    rows = [[InlineKeyboardButton(text=label, callback_data=f"quick_tpl:{tid}")]
            for tid, label in TEMPLATE_NAMES.items()]
    # Добавляем кастомные шаблоны пользователя
    customs = await db.list_custom_templates(call.from_user.id)
    for tpl in customs:
        own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
        rows.append([InlineKeyboardButton(
            text=f"{own} {tpl['name']}",
            callback_data=f"quick_tpl:custom_{tpl['id']}"
        )])
    await call.message.answer(
        "⚡️ <b>Быстрый отзыв</b> — выбери стиль карточки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )


# ── Быстрый отзыв ─────────────────────────────────────────────────────────

@router.callback_query(QuickReviewSG.choose_template, F.data.startswith("quick_tpl:"))
async def cb_quick_template(call: CallbackQuery, state: FSMContext):
    tpl = call.data.split(":")[1]
    await state.update_data(template_id=tpl)
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    await state.set_state(QuickReviewSG.enter_seller)
    await call.message.answer(
        "🏪 Укажи <b>@username</b> продавца или название его магазина:\n"
        "<i>Например: @shimshop или ShimShop</i>"
    )


@router.message(QuickReviewSG.enter_seller)
async def cb_quick_seller(message: Message, state: FSMContext):
    await state.update_data(seller_tag=message.text.strip(), shop_name=message.text.strip())
    await state.set_state(QuickReviewSG.enter_item)
    await message.answer(
        "🎮 Что купил?\n"
        "<i>Например: Грибы, Токены, Коины, Существа</i>"
    )


@router.message(QuickReviewSG.enter_item)
async def cb_quick_item(message: Message, state: FSMContext):
    await state.update_data(item_bought=message.text.strip())
    await state.set_state(QuickReviewSG.choose_stars)
    await message.answer(
        "⭐️ Поставь оценку продавцу:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="★"*i, callback_data=f"quick_stars:{i}") for i in range(1, 6)
        ]])
    )


@router.callback_query(QuickReviewSG.choose_stars, F.data.startswith("quick_stars:"))
async def cb_quick_stars(call: CallbackQuery, state: FSMContext):
    stars = int(call.data.split(":")[1])
    await state.update_data(stars=stars)
    await call.answer(f"{'★'*stars} принято!")
    await call.message.edit_reply_markup(reply_markup=None)
    await state.set_state(QuickReviewSG.enter_text)
    await call.message.answer(
        "✏️ Напиши свой отзыв!\n"
        "<i>Расскажи о сделке — скорость, честность, всё ли пришло.</i>"
    )


@router.message(QuickReviewSG.enter_text)
async def cb_quick_text(message: Message, state: FSMContext, bot, db: Database, config):
    from aiogram.types import BufferedInputFile
    from services.card_generator import generate_card
    from constants import REVIEW_MAX_LEN

    text = message.text.strip()
    if len(text) > REVIEW_MAX_LEN:
        await message.answer(f"❌ Слишком длинно, максимум {REVIEW_MAX_LEN} символов.")
        return

    data = await state.get_data()
    buyer_name = message.from_user.full_name or "Трейдер"

    avatar_bytes = None
    try:
        photos = await bot.get_user_profile_photos(message.from_user.id, limit=1)
        if photos.total_count > 0:
            file = await bot.get_file(photos.photos[0][-1].file_id)
            buf = await bot.download_file(file.file_path)
            avatar_bytes = buf.read()
    except Exception:
        pass

    await message.answer("⏳ Генерирую карточку...")
    card_data = {
        "shop_name": data.get("shop_name", "Shop"),
        "seller_tag": data.get("seller_tag", ""),
        "buyer_name": buyer_name,
        "buyer_initials": "".join(w[0].upper() for w in buyer_name.split() if w)[:2],
        "review_text": text,
        "item_bought": data.get("item_bought", ""),
        "stars": data.get("stars", 5),
        "stars_mode": "buyer_choice",
        "template_id": data.get("template_id", "classic_gold"),
        "avatar_bytes": avatar_bytes,
        "entities": message.entities or [],
        "bot": bot,
        "bot_username": config.BOT_USERNAME,
        "db": db,
    }
    try:
        img = await generate_card(card_data)
        # Сначала подсказка отдельным сообщением
        await message.answer(
            "✅ Вот твоя карточка! Отправь её продавцу в личные сообщения. "
            "При пересылке можешь скрыть «переслано от...»"
        )
        # Затем карточка с дубликатом отзыва в подписи
        stars_line = "★" * data.get("stars", 0) if data.get("stars", 0) > 0 else ""
        cap_parts = [f"<b>{data.get('shop_name', 'Shop')}</b>"]
        if stars_line:
            cap_parts.append(stars_line)
        cap_parts.append(f"\n<i>«{text}»</i>")
        cap_parts.append(f"\n— {buyer_name}")
        await message.answer_photo(
            BufferedInputFile(img, filename="review.png"),
            caption="\n".join(cap_parts)
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка генерации: {e}")
    await state.clear()


# ── Меню продавца ─────────────────────────────────────────────────────────

@router.callback_query(F.data == "start:mytemplates")
async def cb_start_mytemplates(call: CallbackQuery, db: Database):
    await call.answer()
    await call.message.edit_reply_markup(reply_markup=None)
    from handlers.my_templates import show_templates_list
    await show_templates_list(call.message, db, call.from_user.id)


@router.message(Command("menu"))
async def cmd_menu(message: Message, db: Database):
    seller = await db.get_seller(message.from_user.id)
    if seller:
        pub_id = seller.get("pub_id") or await db.ensure_pub_id(message.from_user.id)
        await message.answer(f"🏪 <b>{seller['shop_name']}</b> — меню продавца\n🆔 ID: <code>{pub_id}</code>",
                             reply_markup=kb_seller_menu(pub_id))
    else:
        has_client = bool(await db.get_client_template(message.from_user.id))
        await message.answer("Выбери действие:",
                             reply_markup=kb_start_menu(False, has_client))


@router.callback_query(F.data == "menu:mylink")
async def cb_mylink(call: CallbackQuery, db: Database, config):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    pub_id = seller.get("pub_id") or await db.ensure_pub_id(call.from_user.id)
    link = f"https://t.me/{config.BOT_USERNAME}?start=seller_{pub_id}"
    await call.message.answer(
        f"🔗 <b>Реферальная ссылка</b>\n"
        f"Скинь покупателю — он откроет бот и оставит отзыв:\n"
        f"<code>{link}</code>\n\n"
        f"⭐️ <b>Отзыв прямо в чате</b>\n"
        f"Покупатель может оставить карточку-отзыв прямо в любом чате "
        f"не заходя в бот. Для этого ему нужно написать:\n\n"
        f"<code>@{config.BOT_USERNAME} {pub_id} текст отзыва</code>\n\n"
        f"🆔 Твой ID: <code>{pub_id}</code>\n"
        f"Скинь покупателю этот ID — и он отправит красивую карточку прямо в вашем чате."
    )
    await call.answer()


@router.callback_query(F.data == "menu:stats")
async def cb_stats(call: CallbackQuery, db: Database):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    stats = await db.get_seller_stats(call.from_user.id)
    await call.message.answer(
        f"📊 <b>Статистика — {seller['shop_name']}</b>\n\n"
        f"Всего отзывов: <b>{stats['total']}</b>\n"
        f"Средняя оценка: <b>{stats['avg_stars'] or '—'} ★</b>"
    )
    await call.answer()


@router.callback_query(F.data == "menu:edit")
async def cb_edit(call: CallbackQuery, db: Database, state: FSMContext):
    await call.answer()
    from handlers.setup import cmd_setup
    await cmd_setup(call.message, db, state)


@router.callback_query(F.data == "menu:preview")
async def cb_preview(call: CallbackQuery, db: Database, bot, config):
    from aiogram.types import BufferedInputFile
    from services.card_generator import generate_card
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    await call.answer()
    await call.message.answer("⏳ Генерирую превью...")
    card_data = {
        "shop_name": seller["shop_name"],
        "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID:{seller['id']}",
        "buyer_name": "Трейдер",
        "buyer_initials": "ТР",
        "review_text": "Грибы пришли быстро, продавец вежливый. Рекомендую всем трейдерам!",
        "item_bought": "Токены" if seller["item_mode"] != "fixed" else seller["item_value"],
        "stars": 5,
        "stars_mode": seller["stars_mode"],
        "template_id": seller["template_id"],
        "avatar_bytes": None,
        "bot_username": config.BOT_USERNAME,
        "db": db,
    }
    try:
        img = await generate_card(card_data)
        await call.message.answer_photo(
            BufferedInputFile(img, filename="preview.png"),
            caption="👆 Так будет выглядеть карточка отзыва для твоего магазина."
        )
    except Exception as e:
        await call.message.answer(f"❌ Ошибка: {e}")


# ── Просмотр шаблонов ─────────────────────────────────────────────────────

@router.callback_query(F.data == "menu:mytemplate")
async def cb_mytemplate(call: CallbackQuery, db: Database, config):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Торговый шаблон не настроен", show_alert=True)
        return
    await call.answer()

    stars_info = STARS_LABELS.get(seller["stars_mode"], seller["stars_mode"])
    if seller["stars_mode"] == "fixed":
        stars_info += f" ({seller['stars_value']}★)"
    item_info = ITEM_LABELS.get(seller["item_mode"], seller["item_mode"])
    if seller["item_mode"] in ("fixed", "hint") and seller["item_value"]:
        item_info += f": «{seller['item_value']}»"
    allow = "✅ Да" if seller["allow_template_choice"] else "❌ Нет"
    tpl = TEMPLATE_NAMES.get(seller["template_id"], seller["template_id"])
    pub_id = seller.get("pub_id") or await db.ensure_pub_id(call.from_user.id)
    seller["pub_id"] = pub_id  # чтобы клавиатура с кнопкой «Поделиться» увидела ID
    link = f"https://t.me/{config.BOT_USERNAME}?start=seller_{pub_id}"

    await call.message.answer(
        f"📋 <b>Торговый шаблон — {seller['shop_name']}</b>\n\n"
        f"🆔 Твой ID: <code>{pub_id}</code>\n"
        f"🎨 Стиль: <b>{tpl}</b>\n"
        f"⭐️ Звёзды: <b>{stars_info}</b>\n"
        f"📦 Что купил: <b>{item_info}</b>\n"
        f"🎨 Выбор стиля покупателем: <b>{allow}</b>\n\n"
        f"🔗 Реф-ссылка: <code>{link}</code>\n\n"
        f"💬 Отзыв в чате: <code>@{config.BOT_USERNAME} {pub_id} текст</code>\n\n"
        f"Что изменить?",
        reply_markup=kb_template_view(seller)
    )


@router.callback_query(F.data == "menu:myclienttemplate")
async def cb_myclienttemplate(call: CallbackQuery, db: Database):
    client = await db.get_client_template(call.from_user.id)
    if not client:
        await call.answer("Клиентский шаблон не настроен", show_alert=True)
        return
    await call.answer()
    tpl = TEMPLATE_NAMES.get(client["template_id"], client["template_id"])
    await call.message.answer(
        f"🎨 <b>Клиентский шаблон</b>\n\n"
        f"Стиль карточки: <b>{tpl}</b>\n\n"
        f"Используется когда ты пишешь <code>@RevShimbot текст</code> в чате.\n\n"
        f"Изменить стиль?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎨 Изменить стиль", callback_data="start:client")],
            [InlineKeyboardButton(text="« Назад", callback_data="menu:back")],
        ])
    )


@router.callback_query(F.data.startswith("edit:"))
async def cb_edit_field(call: CallbackQuery, db: Database, state: FSMContext):
    field = call.data.split(":")[1]
    # ID меняется отдельно — проверяем кулдаун сразу при нажатии
    if field == "pubid":
        seller = await db.get_seller(call.from_user.id)
        if not seller:
            await call.answer("Сначала настрой шаблон", show_alert=True)
            return
        # Проверяем кулдаун заранее, чтобы не заставлять вводить впустую
        changed_at = seller.get("pub_id_changed_at")
        if changed_at:
            from datetime import datetime, timezone, timedelta
            elapsed = datetime.now(timezone.utc) - changed_at
            cooldown = timedelta(hours=24)
            if elapsed < cooldown:
                remaining = int((cooldown - elapsed).total_seconds())
                h = remaining // 3600
                m = (remaining % 3600) // 60
                await call.answer(f"⏳ Изменить ID можно через {h} ч {m} мин", show_alert=False)
                return
        await call.answer()
        await state.set_state(SetupSG.pub_id)
        cur = seller.get("pub_id", "—")
        await call.message.answer(
            f"🆔 Текущий ID: <code>{cur}</code>\n\n"
            f"Введи новый ID:\n"
            f"<i>Только латинские буквы и цифры, до 4 символов, без пробелов. "
            f"Регистр не важен — всё станет заглавным (shim → SHIM).</i>"
        )
        return

    await call.answer()
    from handlers.setup import SetupSG
    from keyboards import kb_templates, kb_stars_mode, kb_stars_value, kb_item_mode, kb_allow_template_choice

    seller = await db.get_seller(call.from_user.id)
    if seller:
        await state.update_data(
            shop_name=seller["shop_name"],
            template_id=seller["template_id"],
            stars_mode=seller["stars_mode"],
            stars_value=seller["stars_value"],
            item_mode=seller["item_mode"],
            item_value=seller["item_value"],
            allow_template_choice=seller["allow_template_choice"],
        )

    # edit_mode=True — после изменения одного поля сразу сохраняем
    await state.update_data(edit_mode=True)
    data = await state.get_data()

    if field == "shop_name":
        await state.set_state(SetupSG.shop_name)
        await call.message.answer("🏪 Введи новое название магазина:")
    elif field == "template":
        await state.set_state(SetupSG.template)
        customs = await db.list_custom_templates(call.from_user.id)
        await call.message.answer("🎨 Выбери стиль:", reply_markup=kb_templates(data.get("template_id"), customs))
    elif field == "stars":
        await state.set_state(SetupSG.stars_mode)
        await call.message.answer("⭐️ Режим звёзд:", reply_markup=kb_stars_mode(data.get("stars_mode")))
    elif field == "item":
        await state.set_state(SetupSG.item_mode)
        await call.message.answer("📦 Режим «Что купил»:", reply_markup=kb_item_mode(data.get("item_mode")))
    elif field == "tpl_choice":
        await state.set_state(SetupSG.template_choice)
        await call.message.answer(
            "🎨 Разрешить покупателю выбирать стиль?",
            reply_markup=kb_allow_template_choice(data.get("allow_template_choice", False))
        )


@router.callback_query(F.data == "menu:back")
async def cb_back(call: CallbackQuery, db: Database):
    await call.answer()
    has_seller = bool(await db.get_seller(call.from_user.id))
    has_client = bool(await db.get_client_template(call.from_user.id))
    await call.message.answer("Выбери действие:", reply_markup=kb_start_menu(has_seller, has_client))


@router.callback_query(F.data == "cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext, db: Database):
    await state.clear()
    has_seller = bool(await db.get_seller(call.from_user.id))
    has_client = bool(await db.get_client_template(call.from_user.id))
    await call.message.answer("❌ Отменено.", reply_markup=kb_start_menu(has_seller, has_client))
    await call.answer()
