from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database
from keyboards import kb_seller_menu, kb_templates, kb_template_view, kb_main_reply, kb_back_to_menu
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
    if not has_seller:
        rows.append([InlineKeyboardButton(text="🏪 Создать шаблон магазина", callback_data="start:seller")])
    if not has_client:
        rows.append([InlineKeyboardButton(text="🎨 Создать клиентский шаблон", callback_data="start:client")])

    rows.append([InlineKeyboardButton(text="⚡️ Быстрый отзыв", callback_data="start:quick")])
    rows.append([InlineKeyboardButton(text="🎨 Конструктор", callback_data="start:mytemplates")])

    if has_seller:
        rows.append([InlineKeyboardButton(text="📋 Мой торговый шаблон", callback_data="menu:mytemplate")])
    if has_client:
        rows.append([InlineKeyboardButton(text="🎨 Мой клиентский шаблон", callback_data="menu:myclienttemplate")])

    rows.append([InlineKeyboardButton(text="🔎 Проверить продавца", callback_data="menu:verify")])
    rows.append([InlineKeyboardButton(text="👤 Профиль", callback_data="menu:profile")])
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
    # Регистрируем пользователя для возможной рассылки
    await db.track_user(message.from_user.id, message.from_user.username)
    args = message.text.split()
    deep_link = args[1] if len(args) > 1 else None

    # Получение шаблона по реф-ссылке (одноразовый ключ)
    if deep_link and deep_link.startswith("tpl_"):
        key = "!" + deep_link.replace("tpl_", "")
        from handlers.my_templates import claim_template
        ok, msg = await claim_template(message.from_user.id, message.from_user.username, key, db, bot)
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
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:back")])
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


@router.callback_query(F.data == "menu:profile")
async def cb_profile(call: CallbackQuery, db: Database, config):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала создай торговый шаблон", show_alert=True)
        return
    await call.answer()

    pub_id = seller.get("pub_id") or await db.ensure_pub_id(call.from_user.id)
    own_templates = await db.count_own_custom_templates(call.from_user.id)
    ch = await db.get_seller_channel(call.from_user.id)
    channel_verified = ch and ch["verified"]
    channel_info = f"✅ {ch['channel_title']}" if channel_verified else "❌ не подключён"
    pending = await db.get_pending_reviews(call.from_user.id)
    has_channel = bool(channel_verified)

    profile_kb_rows = [
        [InlineKeyboardButton(text="🆔 Изменить ID", callback_data="edit:pubid")],
        [InlineKeyboardButton(
            text="📢 Канал отзывов: " + ("✅ подключён" if channel_verified else "➕ добавить"),
            callback_data="channel:manage"
        )],
    ]
    if has_channel:
        profile_kb_rows.append([
            InlineKeyboardButton(
                text=f"📬 Мои заявки ({len(pending)})" if pending else "📬 Мои заявки",
                callback_data="menu:pending"
            )
        ])
    profile_kb_rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:back")])

    # Кнопка админки — только для админа
    if config.ADMIN_TG_ID and call.from_user.id == config.ADMIN_TG_ID:
        profile_kb_rows.insert(0, [InlineKeyboardButton(text="🛠 Админка", callback_data="admin:home")])

    await call.message.answer(
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 Твой ID: <code>{pub_id}</code>\n"
        f"🎨 Шаблонов в конструкторе: <b>{own_templates}</b>\n"
        f"📢 Канал отзывов: <b>{channel_info}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=profile_kb_rows)
    )


@router.callback_query(F.data == "menu:pending")
async def cb_pending_reviews(call: CallbackQuery, db: Database):
    pending = await db.get_pending_reviews(call.from_user.id)
    await call.answer()

    if not pending:
        await call.message.answer("📭 Нет необработанных заявок.")
        return

    await call.message.answer(f"📬 <b>Необработанные заявки ({len(pending)})</b>")

    for r in pending:
        stars_line = "★" * r["stars"] if r["stars"] > 0 else ""
        caption = (
            f"<b>{r['buyer_name']}</b>\n"
            f"{stars_line}\n"
            f"<i>«{r['review_text'][:200]}{'...' if len(r['review_text']) > 200 else ''}»</i>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Принять", callback_data=f"review:accept:{r['id']}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{r['id']}"),
        ]])
        if r.get("card_file_id"):
            await call.message.answer_photo(
                photo=r["card_file_id"],
                caption=caption,
                reply_markup=kb
            )
        else:
            await call.message.answer(caption, reply_markup=kb)


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
        f"Скинь покупателю этот ID — и он отправит красивую карточку прямо в вашем чате.",
        reply_markup=kb_back_to_menu()
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
        f"Средняя оценка: <b>{stats['avg_stars'] or '—'} ★</b>",
        reply_markup=kb_back_to_menu()
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
    tid = seller["template_id"]
    if tid.startswith("custom_"):
        try:
            ct = await db.get_custom_template(int(tid.split("_")[1]))
            tpl = ct["name"] if ct else tid
        except Exception:
            tpl = tid
    else:
        tpl = TEMPLATE_NAMES.get(tid, tid)
    pub_id = seller.get("pub_id") or await db.ensure_pub_id(call.from_user.id)
    seller["pub_id"] = pub_id  # чтобы клавиатура с кнопкой «Поделиться» увидела ID
    ch = await db.get_seller_channel(call.from_user.id)
    seller["channel_verified"] = ch and ch["verified"]
    link = f"https://t.me/{config.BOT_USERNAME}?start=seller_{pub_id}"

    from keyboards import CARD_SOURCE_LABELS, INLINE_BTN_LABELS
    card_src = CARD_SOURCE_LABELS.get(seller.get("card_source_mode", "standard"), "Стандартные")
    btn_mode = INLINE_BTN_LABELS.get(seller.get("inline_button_mode", "shown"), "Включена")

    await call.message.answer(
        f"📋 <b>Торговый шаблон — {seller['shop_name']}</b>\n\n"
        f"🆔 Твой ID: <code>{pub_id}</code>\n"
        f"🎨 Стиль: <b>{tpl}</b>\n"
        f"⭐️ Звёзды: <b>{stars_info}</b>\n"
        f"📦 Что купил: <b>{item_info}</b>\n"
        f"🎴 Карточки для отзывов: <b>{card_src}</b>\n"
        f"🔘 Инлайн-кнопка: <b>{btn_mode}</b>\n\n"
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
    tid = client["template_id"]
    if tid.startswith("custom_"):
        try:
            ct = await db.get_custom_template(int(tid.split("_")[1]))
            tpl = ct["name"] if ct else tid
        except Exception:
            tpl = tid
    else:
        tpl = TEMPLATE_NAMES.get(tid, tid)
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
async def cb_edit_field(call: CallbackQuery, db: Database, state: FSMContext, config):
    field = call.data.split(":")[1]

    from handlers.setup import SetupSG
    from keyboards import kb_templates, kb_stars_mode, kb_stars_value, kb_item_mode, kb_allow_template_choice

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
    elif field == "cards":
        seller = await db.get_seller(call.from_user.id)
        from keyboards import kb_card_source, CARD_SOURCE_LABELS
        cur_label = CARD_SOURCE_LABELS.get(seller.get("card_source_mode", "standard"))
        await call.message.answer(
            "🎴 <b>Карточки для отзывов</b>\n\n"
            "Выбери, какие карточки увидит клиент, когда пишет отзыв для твоего магазина:\n\n"
            "• <b>Только стандартные</b> — встроенные шаблоны бота\n"
            "• <b>Только мои</b> — карточки из конструктора (подчёркивают уникальность магазина)\n"
            "• <b>Мои + стандартные</b> — всё вместе\n\n"
            f"Сейчас: <b>{cur_label}</b>",
            reply_markup=kb_card_source(seller)
        )
    elif field == "inlinebtn":
        seller = await db.get_seller(call.from_user.id)
        from keyboards import kb_inline_button, INLINE_BTN_LABELS
        cur_label = INLINE_BTN_LABELS.get(seller.get("inline_button_mode", "shown"))
        await call.message.answer(
            "🔘 <b>Управление инлайн-кнопкой</b>\n\n"
            "К карточке отзыва, которую получает продавец, можно добавить кнопку "
            "со ссылкой на профиль покупателя:\n\n"
            "• <b>Спрятать</b> — кнопки не будет\n"
            "• <b>Включить</b> — кнопка всегда добавляется\n"
            "• <b>Спросить клиента</b> — у покупателя спросят, хочет ли он ссылку на себя\n\n"
            "🔔 <b>Уведомления о чат-отзывах</b> — когда покупатель пишет отзыв прямо в "
            "чате через <code>@" + config.BOT_USERNAME + " ID текст</code>, бот пришлёт тебе "
            "карточку в ЛС с кнопками «Принять / Отклонить», как при обычном отзыве.\n\n"
            f"Сейчас: <b>{cur_label}</b>",
            reply_markup=kb_inline_button(seller)
        )
    elif field == "inlinecfg":
        seller = await db.get_seller(call.from_user.id)
        from keyboards import kb_inline_config
        itid = seller.get("inline_template_id") or seller["template_id"]
        if itid.startswith("custom_"):
            try:
                ct = await db.get_custom_template(int(itid.split("_")[1]))
                tpl_name = ct["name"] if ct else itid
            except Exception:
                tpl_name = itid
        else:
            tpl_name = TEMPLATE_NAMES.get(itid, itid)
        await call.message.answer(
            "💬 <b>Отзывы через личку (инлайн)</b>\n\n"
            "Когда клиент пишет отзыв прямо в чате через "
            f"<code>@{config.BOT_USERNAME} ID текст</code>, он не может выбрать карточку — "
            "используется карточка по умолчанию.\n\n"
            f"🎨 Сейчас: <b>{tpl_name}</b>\n\n"
            "Здесь можно задать отдельную карточку и решить, показывать ли ссылку на покупателя.",
            reply_markup=kb_inline_config(seller)
        )


@router.callback_query(F.data.startswith("cardsrc:"))
async def cb_card_source(call: CallbackQuery, db: Database):
    action = call.data.split(":")[1]
    if action == "noop":
        await call.answer()
        return
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return

    if action == "pick":
        await call.answer()
        from keyboards import kb_pick_custom_cards
        customs = await db.list_custom_templates(call.from_user.id)
        await call.message.answer(
            "⚙️ <b>Выбор карточек</b>\n\n"
            "Отметь, какие из твоих карточек показывать клиенту. "
            "Если ничего не отмечено — клиент увидит все твои карточки.",
            reply_markup=kb_pick_custom_cards(customs, seller.get("allowed_custom_ids", []))
        )
        return

    # Смена режима источника
    if action in ("standard", "custom", "both"):
        await db.update_seller(call.from_user.id, card_source_mode=action)
        await call.answer("✅ Режим обновлён")
        seller = await db.get_seller(call.from_user.id)
        from keyboards import kb_card_source
        try:
            await call.message.edit_reply_markup(reply_markup=kb_card_source(seller))
        except Exception:
            pass


@router.callback_query(F.data.startswith("cardpick:"))
async def cb_card_pick(call: CallbackQuery, db: Database):
    cid = int(call.data.split(":")[1])
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    allowed = list(seller.get("allowed_custom_ids", []))
    if cid in allowed:
        allowed.remove(cid)
    else:
        allowed.append(cid)
    await db.update_seller(call.from_user.id, allowed_custom_ids=allowed)
    await call.answer("Переключено")
    from keyboards import kb_pick_custom_cards
    customs = await db.list_custom_templates(call.from_user.id)
    try:
        await call.message.edit_reply_markup(
            reply_markup=kb_pick_custom_cards(customs, allowed))
    except Exception:
        pass


@router.callback_query(F.data.startswith("inlbtn:"))
async def cb_inline_button_mode(call: CallbackQuery, db: Database):
    action = call.data.split(":")[1]
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return

    if action == "notifytoggle":
        new_val = not seller.get("inline_notify_seller", False)
        await db.update_seller(call.from_user.id, inline_notify_seller=new_val)
        await call.answer("🔔 Уведомления включены" if new_val else "🔕 Уведомления выключены")
        seller = await db.get_seller(call.from_user.id)
        from keyboards import kb_inline_button
        try:
            await call.message.edit_reply_markup(reply_markup=kb_inline_button(seller))
        except Exception:
            pass
        return

    if action not in ("hidden", "shown", "ask"):
        await call.answer()
        return
    await db.update_seller(call.from_user.id, inline_button_mode=action)
    await call.answer("✅ Режим кнопки обновлён")
    seller = await db.get_seller(call.from_user.id)
    from keyboards import kb_inline_button
    try:
        await call.message.edit_reply_markup(reply_markup=kb_inline_button(seller))
    except Exception:
        pass


@router.callback_query(F.data.startswith("inlcfg:"))
async def cb_inline_config(call: CallbackQuery, db: Database, state: FSMContext):
    action = call.data.split(":")[1]
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return

    if action == "btntoggle":
        new_val = not seller.get("inline_button_show", True)
        await db.update_seller(call.from_user.id, inline_button_show=new_val)
        await call.answer("✅ Обновлено")
        seller = await db.get_seller(call.from_user.id)
        from keyboards import kb_inline_config
        try:
            await call.message.edit_reply_markup(reply_markup=kb_inline_config(seller))
        except Exception:
            pass
        return

    if action == "tpl":
        await call.answer()
        from keyboards import kb_templates
        customs = await db.list_custom_templates(call.from_user.id)
        cur = seller.get("inline_template_id") or seller["template_id"]
        await state.update_data(inlcfg_picking=True)
        await call.message.answer(
            "🎨 Выбери карточку по умолчанию для инлайн-отзывов:",
            reply_markup=_kb_inline_tpl_pick(cur, customs)
        )
        return


def _kb_inline_tpl_pick(selected, customs):
    """Клавиатура выбора инлайн-карточки (callback inltpl:*)."""
    from constants import TEMPLATES
    rows = []
    for tid, label in TEMPLATES.items():
        mark = "✅ " if tid == selected else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{label}", callback_data=f"inltpl:{tid}")])
    if customs:
        for tpl in customs:
            own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
            cid = f"custom_{tpl['id']}"
            mark = "✅ " if selected == cid else ""
            rows.append([InlineKeyboardButton(text=f"{mark}{own} {tpl['name']}",
                                               callback_data=f"inltpl:{cid}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="edit:inlinecfg")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("inltpl:"))
async def cb_inline_tpl_set(call: CallbackQuery, db: Database):
    tid = call.data.split(":", 1)[1]
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    await db.update_seller(call.from_user.id, inline_template_id=tid)
    await call.answer("✅ Карточка для инлайн обновлена")
    seller = await db.get_seller(call.from_user.id)
    customs = await db.list_custom_templates(call.from_user.id)
    try:
        await call.message.edit_reply_markup(reply_markup=_kb_inline_tpl_pick(tid, customs))
    except Exception:
        pass


@router.callback_query(F.data == "menu:back")
async def cb_back(call: CallbackQuery, db: Database):
    await call.answer()
    has_seller = bool(await db.get_seller(call.from_user.id))
    has_client = bool(await db.get_client_template(call.from_user.id))
    await call.message.answer("Выбери действие:", reply_markup=kb_start_menu(has_seller, has_client))


@router.callback_query(F.data == "menu:seller_home")
async def cb_seller_home(call: CallbackQuery, db: Database):
    """Возврат в меню продавца с информационных экранов."""
    await call.answer()
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        has_client = bool(await db.get_client_template(call.from_user.id))
        await call.message.answer("Выбери действие:", reply_markup=kb_start_menu(False, has_client))
        return
    pub_id = seller.get("pub_id") or await db.ensure_pub_id(call.from_user.id)
    await call.message.answer(
        f"🏪 <b>{seller['shop_name']}</b> — меню продавца\n🆔 ID: <code>{pub_id}</code>",
        reply_markup=kb_seller_menu(pub_id)
    )


@router.callback_query(F.data == "cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext, db: Database):
    await state.clear()
    has_seller = bool(await db.get_seller(call.from_user.id))
    has_client = bool(await db.get_client_template(call.from_user.id))
    await call.message.answer("❌ Отменено.", reply_markup=kb_start_menu(has_seller, has_client))
    await call.answer()


# ── Проверка подлинности: продавец по ID или отзыв по коду с карточки ───────

class VerifySG(StatesGroup):
    waiting = State()


_VERIFY_HITS: dict = {}  # user_id -> [timestamps] — лимит 10 проверок/час


def _verify_allowed(uid: int, admin_id: int) -> bool:
    import time
    if admin_id and uid == admin_id:
        return True
    now = time.time()
    hits = [t for t in _VERIFY_HITS.get(uid, []) if now - t < 3600]
    if len(hits) >= 10:
        _VERIFY_HITS[uid] = hits
        return False
    hits.append(now)
    if len(_VERIFY_HITS) > 5000:
        _VERIFY_HITS.clear()
    _VERIFY_HITS[uid] = hits
    return True


def _mask_name(name: str) -> str:
    """Маскирует имя: хватает для сверки, бесполезно для сбора данных."""
    def m(w):
        if len(w) <= 2:
            return w[0] + "*"
        return w[0] + "*" * (len(w) - 2) + w[-1]
    return " ".join(m(w) for w in (name or "?").split())


_STATUS_LABELS = {
    "pending": "⏳ ожидает решения продавца",
    "accepted": "✅ принят продавцом",
    "rejected": "❌ отклонён продавцом",
}


async def _answer_verify(message: Message, db: Database, config, raw: str):
    """Разбирает ввод: код отзыва (RSB-XXXXXXXX) или ID продавца (до 4 символов)."""
    import re as _re
    import html as _h
    t = (raw or "").strip().upper().replace(" ", "")

    kb_again = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Проверить ещё", callback_data="menu:verify")],
        [InlineKeyboardButton(text="« Меню", callback_data="menu:back")],
    ])

    # Код отзыва с карточки
    m = _re.fullmatch(r"(?:RSB-?)?([0-9A-HJKMNP-TV-Z]{8})", t)
    if m:
        review = await db.get_review_by_code("RSB-" + m.group(1))
        if not review:
            await message.answer(
                "❌ <b>Код не найден.</b>\n\n"
                "Такой отзыв этим ботом не выдавался — вероятно, карточка поддельная.",
                reply_markup=kb_again)
            return
        seller = await db.get_seller(review["seller_id"])
        shop = _h.escape(seller["shop_name"]) if seller else "?"
        pub = seller.get("pub_id") if seller else None
        stars = "★" * review.get("stars", 0) or "—"
        date = review["created_at"].strftime("%d.%m.%Y")
        status = _STATUS_LABELS.get(review.get("status"), review.get("status") or "—")
        await message.answer(
            f"✅ <b>Отзыв настоящий</b> — выдан этим ботом.\n\n"
            f"🏪 Магазин: <b>{shop}</b>" + (f" (ID <code>{pub}</code>)" if pub else "") + "\n"
            f"⭐ Оценка: {stars}\n"
            f"📅 Дата: {date}\n"
            f"👤 Покупатель: {_h.escape(_mask_name(review.get('buyer_name') or ''))}\n"
            f"📌 Статус: {status}",
            reply_markup=kb_again)
        return

    # ID продавца
    if _re.fullmatch(r"[A-Z0-9]{1,4}", t):
        seller = await db.get_seller_by_pubid(t)
        if not seller:
            await message.answer(
                f"❌ Продавец с ID <code>{_h.escape(t)}</code> в боте не найден.",
                reply_markup=kb_again)
            return
        stats = await db.get_shop_stats(seller["id"])
        ch = await db.get_seller_channel(seller["id"])
        avg = stats.get("avg_stars")
        first = stats["first_at"].strftime("%d.%m.%Y") if stats.get("first_at") else "—"
        last = stats["last_at"].strftime("%d.%m.%Y") if stats.get("last_at") else "—"
        await message.answer(
            f"🏪 <b>{_h.escape(seller['shop_name'])}</b> (ID <code>{seller['pub_id']}</code>)\n\n"
            f"📊 Отзывов выдано ботом: <b>{stats['total']}</b>\n"
            f"👥 Уникальных покупателей: <b>{stats['unique_buyers']}</b>\n"
            f"⭐ Средняя оценка: <b>{avg if avg is not None else '—'}</b>\n"
            f"📅 Первый отзыв: {first} · Последний: {last}\n"
            f"📢 Канал отзывов: {'подключён ✅' if (ch and ch['verified']) else 'не подключён'}\n\n"
            f"<i>Если продавец заявляет больше отзывов, чем выдал бот, — "
            f"это повод насторожиться. Проверяй коды на карточках.</i>",
            reply_markup=kb_again)
        return

    await message.answer(
        "🤔 Не понял формат. Пришли:\n"
        "• <b>ID продавца</b> — до 4 букв/цифр, например <code>SHIM</code>\n"
        "• или <b>код отзыва</b> с карточки, например <code>RSB-7K3MPQ92</code>")


@router.callback_query(F.data == "menu:verify")
async def cb_verify(call: CallbackQuery, state: FSMContext, config):
    await call.answer()
    if not _verify_allowed(call.from_user.id, config.ADMIN_TG_ID):
        await call.message.answer("⏳ Слишком много проверок. Попробуй через час.")
        return
    await state.set_state(VerifySG.waiting)
    await call.message.answer(
        "🔎 <b>Проверка подлинности</b>\n\n"
        "Пришли <b>ID продавца</b> (например <code>SHIM</code>) — покажу его настоящую "
        "статистику из бота.\n\n"
        "Или пришли <b>код отзыва</b> с карточки (например <code>RSB-7K3MPQ92</code>) — "
        "проверю, выдавал ли бот такой отзыв."
    )


@router.message(VerifySG.waiting, F.text)
async def verify_input(message: Message, state: FSMContext, db: Database, config):
    await state.clear()
    if not _verify_allowed(message.from_user.id, config.ADMIN_TG_ID):
        await message.answer("⏳ Слишком много проверок. Попробуй через час.")
        return
    await _answer_verify(message, db, config, message.text)


@router.message(Command("check"))
async def cmd_check(message: Message, db: Database, config):
    if not _verify_allowed(message.from_user.id, config.ADMIN_TG_ID):
        await message.answer("⏳ Слишком много проверок. Попробуй через час.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: <code>/check RSB-XXXXXXXX</code> или <code>/check SHIM</code>")
        return
    await _answer_verify(message, db, config, args[1])


@router.message(StateFilter(None), F.text.regexp(r"(?i)^rsb-?[0-9a-hjkmnp-tv-z]{8}$"))
async def bare_code_check(message: Message, db: Database, config):
    """Голый код с карточки, отправленный в личку — проверяем сразу."""
    if not _verify_allowed(message.from_user.id, config.ADMIN_TG_ID):
        await message.answer("⏳ Слишком много проверок. Попробуй через час.")
        return
    await _answer_verify(message, db, config, message.text)
