from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import logging
import asyncio
import html as _html_mod


def _esc(t) -> str:
    """Эскейп пользовательского текста для caption с parse_mode=HTML."""
    return _html_mod.escape(str(t or ""))

from db import Database
from keyboards import kb_buyer_stars, kb_templates, kb_cancel
from constants import REVIEW_MAX_LEN, REVIEW_SOFT_LEN
from services.card_generator import generate_card


async def _place_review_number(bot, db, config, seller_id: int, review_id: int,
                               channel_id: int, channel_msg_id: int, ch: dict) -> bool:
    """Ставит номер отзыву: вычисляет (умный последний), постит сообщение-номер,
    записывает в БД. Возвращает True если поставлен."""
    try:
        cache_chat = getattr(config, "CACHE_CHAT_ID", None)
        # Умный последний: если самый большой номер указывает на отзыв,
        # которого уже нет в канале — переиспользуем его номер.
        last = await db.get_last_number_entry(seller_id)
        reuse_number = None
        if last and last.get("channel_msg_id"):
            alive = await _msg_alive(bot, channel_id, last["channel_msg_id"], cache_chat)
            if not alive:
                # Последний отзыв удалён — освобождаем его номер и подчищаем его сообщение-номер
                reuse_number = last["number"]
                if last.get("number_msg_id"):
                    try:
                        await bot.delete_message(channel_id, last["number_msg_id"])
                    except Exception:
                        pass
                await db.delete_number_entry(last["id"])

        number = reuse_number if reuse_number is not None else await db.next_review_number(seller_id)

        # Текст номера в HTML — премиум-эмодзи через <tg-emoji> сохраняются
        from handlers.numbering import _render_number_html
        tpl = ch.get("numbering_template", "Отзыв №{n}")
        text = _render_number_html(tpl, number)
        try:
            sent = await bot.send_message(channel_id, text, parse_mode="HTML")
        except Exception:
            import re as _re
            plain = _re.sub(r'<tg-emoji[^>]*>|</tg-emoji>', '', text)
            plain = _re.sub(r'<[^>]+>', '', plain)
            sent = await bot.send_message(channel_id, plain)
        await db.record_review_number(seller_id, review_id, number,
                                      channel_msg_id, sent.message_id)
        return True
    except Exception as e:
        logger.error(f"Не удалось поставить номер отзыву {review_id}: {e}")
        return False


async def _msg_alive(bot, chat_id: int, message_id: int, cache_chat_id) -> bool:
    """Проверяет, существует ли сообщение в канале. Надёжно: пытаемся скопировать
    в кеш-чат и сразу удаляем копию. Если оригинал удалён — Telegram бросит ошибку.
    Если кеш-чат не задан — считаем живым (не рискуем удалять номер зря)."""
    if not cache_chat_id:
        return True
    try:
        copied = await bot.copy_message(
            chat_id=cache_chat_id, from_chat_id=chat_id, message_id=message_id)
        try:
            await bot.delete_message(cache_chat_id, copied.message_id)
        except Exception:
            pass
        return True
    except Exception as e:
        msg = str(e).lower()
        if "not found" in msg or "to copy" in msg or "message_id_invalid" in msg:
            return False
        return True  # прочие ошибки — не рискуем, считаем живым

logger = logging.getLogger(__name__)
router = Router()


class ReviewSG(StatesGroup):
    choose_template = State()
    choose_stars = State()
    enter_item = State()
    enter_text = State()
    enter_proof = State()
    ask_anon = State()
    ask_inline_btn = State()


async def start_review_flow(message: Message, seller: dict, state: FSMContext, db: Database):
    # Ранняя проверка анти-накрутки — чтобы не заставлять печатать отзыв зря
    from config import Config
    _cfg = Config()
    if message.from_user.id != _cfg.ADMIN_TG_ID:
        if await db.has_recent_review(message.from_user.id, seller["id"], hours=6):
            await message.answer(
                "⏳ Ты уже оставлял отзыв этому продавцу недавно.\n"
                "Повторный отзыв — через 6 часов после предыдущего.")
            return
        if await db.count_reviews_24h(message.from_user.id) >= 10:
            await message.answer("⛔ Дневной лимит отзывов исчерпан. Возвращайся завтра!")
            return
    await state.update_data(seller=seller, mode="buyer")

    shop = seller["shop_name"]
    seller_tag = f"@{seller['username']}" if seller.get("username") else f"tg://user?id={seller['id']}"

    await message.answer(
        f"👋 Ты оставляешь отзыв для магазина <b>{shop}</b> ({seller_tag})\n\n"
        f"Давай создадим красивую карточку!"
    )

    # Какие карточки показать клиенту — зависит от настроек продавца
    source_mode = seller.get("card_source_mode", "standard")
    allow_choice = seller["allow_template_choice"] or source_mode in ("custom", "both")

    if allow_choice:
        await state.set_state(ReviewSG.choose_template)
        customs_all = await db.list_custom_templates(seller["id"])
        allowed_ids = set(seller.get("allowed_custom_ids", []) or [])
        # Если список разрешённых задан — показываем только их, иначе все свои
        if allowed_ids:
            customs = [c for c in customs_all if c["id"] in allowed_ids]
        else:
            customs = customs_all

        # Стандартные карточки показываем только если режим это разрешает
        show_standard = source_mode in ("standard", "both")
        if source_mode == "custom" and not customs:
            # режим «только свои», но разрешённых нет — fallback на стандартные
            show_standard = True

        from keyboards import kb_templates_filtered
        await message.answer(
            "🎨 Выбери стиль карточки:",
            reply_markup=kb_templates_filtered(seller["template_id"], customs, show_standard)
        )
    else:
        await state.update_data(template_id=seller["template_id"])
        await ask_stars(message, seller, state)


@router.callback_query(ReviewSG.choose_template, F.data.startswith("tpl:"))
async def cb_choose_template(call: CallbackQuery, state: FSMContext):
    tid = call.data.split(":")[1]
    await state.update_data(template_id=tid)
    await call.answer("Шаблон выбран ✅")
    data = await state.get_data()
    await ask_stars(call.message, data["seller"], state)


async def ask_stars(message: Message, seller: dict, state: FSMContext):
    if seller["stars_mode"] == "disabled":
        await state.update_data(stars=0)
        await ask_item(message, seller, state)
    elif seller["stars_mode"] == "fixed":
        await state.update_data(stars=seller["stars_value"])
        await ask_item(message, seller, state)
    else:
        await state.set_state(ReviewSG.choose_stars)
        await message.answer(
            "⭐️ Поставь оценку магазину:",
            reply_markup=kb_buyer_stars()
        )


@router.callback_query(ReviewSG.choose_stars, F.data.startswith("review_stars:"))
async def cb_choose_stars(call: CallbackQuery, state: FSMContext):
    stars = int(call.data.split(":")[1])
    await state.update_data(stars=stars)
    await call.answer(f"{'★'*stars} — принято!")
    data = await state.get_data()
    await ask_item(call.message, data["seller"], state)


async def ask_item(message: Message, seller: dict, state: FSMContext):
    item_mode = seller["item_mode"]
    if item_mode == "fixed":
        await state.update_data(item_bought=seller["item_value"])
        await ask_text(message, state)
    elif item_mode == "hint":
        hint = seller["item_value"] or "Что именно купил?"
        await state.set_state(ReviewSG.enter_item)
        await message.answer(f"🎮 {hint}")
    else:
        await state.set_state(ReviewSG.enter_item)
        await message.answer("🎮 Что именно купил? Укажи название\n<i>Например: Грибы, Токены, Коины, Существа, Услуги</i>")


@router.message(ReviewSG.enter_item)
async def step_enter_item(message: Message, state: FSMContext):
    item = message.text.strip()
    if len(item) > 128:
        await message.answer("❌ Слишком длинно, максимум 128 символов.")
        return
    await state.update_data(item_bought=item)
    await ask_text(message, state)


async def ask_text(message: Message, state: FSMContext):
    await state.set_state(ReviewSG.enter_text)
    await message.answer(
        f"✏️ Напиши свой отзыв о продавце!\n\n"
        f"<i>Расскажи о сделке — быстро ли ответил, честная ли цена, всё ли пришло. "
        f"Максимум {REVIEW_MAX_LEN} символов.</i>"
    )


def _compress_proof(raw: bytes, max_side: int = 1000) -> bytes:
    """Сжимает фото-пруф чтобы карточка не была слишком тяжёлой для отправки."""
    try:
        import io
        from PIL import Image
        # Защита от decompression-бомб: гигантские по пикселям картинки отклоняем
        Image.MAX_IMAGE_PIXELS = 40_000_000  # ~40 Мп — с запасом для честных фото
        im = Image.open(io.BytesIO(raw))
        im.load()  # форсим декодирование здесь, чтобы поймать бомбу
        im = im.convert("RGB")
        if max(im.size) > max_side:
            im.thumbnail((max_side, max_side), Image.LANCZOS)
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=82, optimize=True)
        return out.getvalue()
    except Exception:
        return raw


async def _check_review_spike(db, bot, config, seller):
    """Детектор накрутки: >15 отзывов/час или >40/сутки продавцу → алерт админу.
    Только уведомление (не автоблок), не чаще 1 раза в 6 часов на продавца."""
    try:
        hour, day = await db.seller_review_burst(seller["id"])
        if hour <= 15 and day <= 40:
            return
        import time as _t
        skey = f"spike_alert_{seller['id']}"
        last = await db.get_setting(skey)
        now = _t.time()
        if last and now - float(last) < 6 * 3600:
            return
        await db.set_setting(skey, str(now))
        admin_id = config.ADMIN_TG_ID
        if not admin_id:
            return
        uname = f"@{seller['username']}" if seller.get("username") else "без юзернейма"
        pub = seller.get("pub_id", "?")
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🚫 Забанить продавца",
                                 callback_data=f"admin:ban_seller:{seller['id']}")
        ]])
        await bot.send_message(
            admin_id,
            f"⚠️ <b>Всплеск отзывов у продавца</b>\n\n"
            f"🏪 {seller['shop_name']} (ID <code>{pub}</code>, {uname})\n"
            f"📊 За час: <b>{hour}</b> · за сутки: <b>{day}</b>\n\n"
            f"<i>Возможна накрутка. Проверь список отзывов и уникальных покупателей.</i>",
            reply_markup=kb)
    except Exception as e:
        logger.info(f"Детектор всплеска не сработал: {e}")


async def _get_avatar_bytes(bot, user_id: int):
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file = await bot.get_file(photos.photos[0][-1].file_id)
            buf = await bot.download_file(file.file_path)
            return buf.read()
    except Exception:
        pass
    return None


async def _gate_anon(event, state: FSMContext, db: Database, bot, config,
                     text: str, entities, proofs=None):
    """Первый гейт: анонимность. Если режим 'ask' — спрашиваем клиента.
    Если 'on' — сразу анон (кнопку профиля пропускаем). Если 'off' — идём к гейту кнопки."""
    data = await state.get_data()
    seller = data["seller"]
    mode = seller.get("anon_mode", "off")

    if mode == "on":
        await state.update_data(is_anonymous=True)
        await finalize_review(event, state, db, bot, config, text=text,
                              entities=entities, proofs=proofs)
        return

    if mode == "ask":
        import base64
        await state.update_data(
            pending_text=text,
            pending_entities=[e.model_dump() for e in entities] if entities else [],
            pending_proofs_b64=[base64.b64encode(p).decode() for p in (proofs or [])],
        )
        await state.set_state(ReviewSG.ask_anon)
        from aiogram.types import CallbackQuery as _CQ
        answer = event.message.answer if isinstance(event, _CQ) else event.answer
        await answer(
            "🕵️ Хочешь оставить отзыв <b>анонимно</b>?\n\n"
            "В карточке не будет твоего имени и аватара — вместо них "
            "будет анонимный профиль.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🕵️ Да, анонимно", callback_data="anon:yes"),
                InlineKeyboardButton(text="👤 Нет, с именем", callback_data="anon:no"),
            ]])
        )
        return

    await state.update_data(is_anonymous=False)
    await _gate_inline_button(event, state, db, bot, config,
                              text=text, entities=entities, proofs=proofs)


@router.callback_query(ReviewSG.ask_anon, F.data.startswith("anon:"))
async def cb_anon_ask(call: CallbackQuery, state: FSMContext, db: Database, bot, config):
    choice = call.data.split(":")[1]
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    is_anon = (choice == "yes")
    await state.update_data(is_anonymous=is_anon)

    import base64
    from aiogram.types import MessageEntity
    data = await state.get_data()
    text = data.get("pending_text", "")
    raw_entities = data.get("pending_entities", [])
    entities = [MessageEntity(**e) for e in raw_entities] if raw_entities else []
    proofs_b64 = data.get("pending_proofs_b64") or []
    proofs = [base64.b64decode(p) for p in proofs_b64] or None

    if is_anon:
        await finalize_review(call, state, db, bot, config, text=text,
                              entities=entities, proofs=proofs)
    else:
        await _gate_inline_button(call, state, db, bot, config,
                                  text=text, entities=entities, proofs=proofs)


async def _gate_inline_button(event, state: FSMContext, db: Database, bot, config,
                               text: str, entities, proofs=None):
    """Перед финализацией: если продавец выбрал режим «спросить» — спрашиваем
    покупателя про кнопку-ссылку. Иначе сразу финализируем."""
    data = await state.get_data()
    seller = data["seller"]
    mode = seller.get("inline_button_mode", "shown")

    if mode == "ask":
        # Сохраняем всё что нужно для финализации после ответа
        import base64
        await state.update_data(
            pending_text=text,
            pending_entities=[e.model_dump() for e in entities] if entities else [],
            pending_proofs_b64=[base64.b64encode(p).decode() for p in (proofs or [])],
        )
        await state.set_state(ReviewSG.ask_inline_btn)
        from aiogram.types import CallbackQuery as _CQ
        answer = event.message.answer if isinstance(event, _CQ) else event.answer
        await answer(
            "🔘 Хочешь, чтобы в карточке для продавца была <b>кнопка-ссылка на твой профиль</b>?\n\n"
            "Так продавцу будет проще тебя найти.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Да, добавить ссылку", callback_data="inlask:yes"),
                InlineKeyboardButton(text="❌ Нет, без ссылки", callback_data="inlask:no"),
            ]])
        )
        return

    # Режимы hidden/shown — кнопка решается в finalize по mode, сразу финализируем
    await finalize_review(event, state, db, bot, config, text=text,
                          entities=entities, proofs=proofs)


@router.callback_query(ReviewSG.ask_inline_btn, F.data.startswith("inlask:"))
async def cb_inline_ask(call: CallbackQuery, state: FSMContext, db: Database, bot, config):
    choice = call.data.split(":")[1]
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await state.update_data(buyer_wants_button=(choice == "yes"))
    data = await state.get_data()
    import base64
    from aiogram.types import MessageEntity
    text = data.get("pending_text", "")
    raw_entities = data.get("pending_entities", [])
    entities = [MessageEntity(**e) for e in raw_entities] if raw_entities else []
    proofs_b64 = data.get("pending_proofs_b64") or []
    proofs = [base64.b64decode(p) for p in proofs_b64] or None
    await finalize_review(call, state, db, bot, config, text=text,
                          entities=entities, proofs=proofs)


async def finalize_review(event, state: FSMContext, db: Database, bot, config,
                          text: str, entities, proofs=None):
    """Общая финализация: генерит карточку, шлёт покупателю и продавцу.
    event может быть Message или CallbackQuery."""
    from aiogram.types import CallbackQuery as _CQ
    if isinstance(event, _CQ):
        user = event.from_user
        answer = event.message.answer
        answer_photo = event.message.answer_photo
    else:
        user = event.from_user
        answer = event.answer
        answer_photo = event.answer_photo

    data = await state.get_data()
    seller = data["seller"]

    buyer_name = user.full_name or user.first_name or "Покупатель"
    buyer_username = user.username

    # Анти-накрутка: 1 отзыв продавцу в 6 часов + суточный лимит (админ — без лимитов для тестов)
    if user.id != config.ADMIN_TG_ID:
        if await db.has_recent_review(user.id, seller["id"], hours=6):
            await answer("⏳ Ты уже оставлял отзыв этому продавцу недавно.\n"
                         "Повторный отзыв — через 6 часов после предыдущего.")
            await state.clear()
            return
        if await db.count_reviews_24h(user.id) >= 10:
            await answer("⛔ Дневной лимит отзывов исчерпан. Возвращайся завтра!")
            await state.clear()
            return

    avatar_bytes = await _get_avatar_bytes(bot, user.id)

    await answer("⏳ Генерирую карточку...")

    # Резервируем код подлинности ДО генерации — он печатается на карточке
    verify_code = await db.reserve_verify_code()

    # Анонимность: подменяем отображаемое имя и аватар (реальные — сохраняем в БД для продавца)
    is_anon = bool(data.get("is_anonymous", False))
    if is_anon:
        display_name = seller.get("anon_nickname") or "Анонимный покупатель"
        anon_av = seller.get("anon_avatar")  # base64 или None
        if anon_av:
            import base64 as _b64
            try:
                display_avatar = _b64.b64decode(anon_av)
            except Exception:
                display_avatar = None
        else:
            display_avatar = None  # генератор нарисует заглушку с инициалами/знаком
        display_initials = "?"
    else:
        display_name = buyer_name
        display_avatar = avatar_bytes
        display_initials = "".join(w[0].upper() for w in buyer_name.split() if w)[:2]

    card_data = {
        "shop_name": seller["shop_name"],
        "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID: {seller['id']}",
        "buyer_name": display_name,
        "buyer_initials": display_initials,
        "review_text": text,
        "item_bought": data.get("item_bought", ""),
        "verify_code": verify_code,
        "stars": data.get("stars", 0),
        "stars_mode": seller["stars_mode"],
        "template_id": data.get("template_id", seller["template_id"]),
        "avatar_bytes": display_avatar,
        "entities": entities or [],
        "bot": bot,
        "bot_username": config.BOT_USERNAME,
        "db": db,
        "proof_list": proofs or [],
    }

    try:
        img_bytes = await generate_card(card_data)
    except Exception as e:
        await answer(f"❌ Ошибка генерации карточки: {e}")
        return

    # Проверяем, есть ли у продавца подключённый канал — от этого зависит,
    # что советовать покупателю: слать в ЛС или ждать публикации ботом
    seller_ch = await db.get_seller_channel(seller["id"])
    seller_has_channel = bool(seller_ch and seller_ch["verified"])

    if seller_has_channel:
        await answer(
            f"✅ Вот твоя карточка отзыва!\n\n"
            f"Она отправлена продавцу <b>{seller['shop_name']}</b> — он опубликует её "
            f"в своём канале отзывов. Ничего пересылать не нужно 👍"
        )
    else:
        await answer(
            f"✅ Вот твоя карточка отзыва!\n\n"
            f"Отправь её продавцу <b>{seller['shop_name']}</b> в личные сообщения. "
            f"При пересылке можешь скрыть «переслано от...»"
        )

    stars_line = "★" * data.get("stars", 0) if data.get("stars", 0) > 0 else ""
    caption_parts = [f"<b>{_esc(seller['shop_name'])}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{_esc(text)}»</i>")
    caption_parts.append(f"\n— {_esc(display_name)}")
    review_caption = "\n".join(caption_parts)

    sent = None
    for attempt in range(3):
        try:
            sent = await answer_photo(
                BufferedInputFile(img_bytes, filename="review.png"),
                caption=review_caption
            )
            break
        except Exception as e:
            logger.warning(f"Отправка карточки покупателю не удалась (попытка {attempt+1}): {e}")
            await asyncio.sleep(1.5)
    if sent is None:
        await answer("⚠️ Карточка сгенерирована, но не отправилась из-за сети. Попробуй ещё раз.")
        return

    # Решаем показ кнопки-ссылки. Различаем ДВА места:
    #  - кнопка в ЛС продавца: показываем всегда по btn_mode (продавцу можно знать клиента)
    #  - кнопка в КАНАЛЕ (show_buyer_button в БД): при анон убираем
    btn_mode = seller.get("inline_button_mode", "shown")
    if btn_mode == "hidden":
        show_button_pm = False
    elif btn_mode == "ask":
        show_button_pm = data.get("buyer_wants_button", False)
    else:  # shown
        show_button_pm = True
    # В канале кнопки нет при анонимности
    show_button_channel = False if is_anon else show_button_pm

    file_id = sent.photo[-1].file_id if sent.photo else None
    review_row = await db.save_review(
        seller_id=seller["id"],
        buyer_id=user.id,
        buyer_name=buyer_name,
        buyer_username=buyer_username,
        review_text=text,
        item_bought=data.get("item_bought", ""),
        stars=data.get("stars", 0),
        template_used=data.get("template_id", seller["template_id"]),
        card_file_id=file_id,
        show_buyer_button=show_button_channel,
        proof_count=len(proofs or []),
        verify_code=verify_code,
        is_anonymous=is_anon,
    )
    review_id = review_row["id"]
    await state.clear()

    # Детектор накрутки — алерт админу при подозрительном всплеске
    await _check_review_spike(db, bot, config, seller)

    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={user.id}"

    ch = seller_ch
    channel_verified = seller_has_channel

    rows = []
    if show_button_pm:
        rows.append([InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)])
    if channel_verified:
        rows.append([
            InlineKeyboardButton(text="✅ Принять", callback_data=f"review:accept:{review_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{review_id}"),
        ])
    # Кнопка редактирования пруфов — только если пруфы есть (безопасность клиента)
    if proofs:
        rows.append([InlineKeyboardButton(text="🖼 Редактировать пруф",
                                          callback_data=f"proofedit:{review_id}")])
    seller_kb = InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    # Под карточкой — АНОН-НИК (как на самой карточке). Реальный автор — только пометкой.
    seller_caption = f"⭐ Новый отзыв!\n\n{review_caption}"
    if is_anon:
        real_tag = f"@{buyer_username}" if buyer_username else f"<a href='tg://user?id={user.id}'>{_esc(buyer_name)}</a>"
        seller_caption += (f"\n\n🕵️ <i>Отзыв анонимный.</i>\n"
                           f"Настоящий автор (видно только тебе): {real_tag}")

    try:
        await bot.send_photo(
            chat_id=seller["id"],
            photo=BufferedInputFile(img_bytes, filename="review.png"),
            caption=seller_caption,
            reply_markup=seller_kb,
            parse_mode="HTML",
        )
        logger.info(f"Карточка отправлена продавцу {seller['id']}")
    except Exception as e:
        logger.error(f"Ошибка отправки карточки продавцу {seller['id']}: {e}")

    # Регистрируем покупателя и показываем ему меню — чтобы он мог пользоваться ботом сам
    try:
        await db.track_user(user.id, user.username)
    except Exception:
        pass
    from keyboards import kb_main_reply
    await answer(
        "🎉 Готово! Кстати, ты тоже можешь создавать свои карточки отзывов и "
        "настраивать магазин — просто загляни в меню ниже 👇",
        reply_markup=kb_main_reply()
    )


MAX_PROOFS = 5

# Локи на пользователя: альбом приходит пачкой конкурентных апдейтов,
# без лока возможна потеря фото или двойная финализация
_PROOF_LOCKS: dict = {}


def _proof_lock(uid: int) -> asyncio.Lock:
    lock = _PROOF_LOCKS.get(uid)
    if lock is None:
        lock = _PROOF_LOCKS[uid] = asyncio.Lock()
        if len(_PROOF_LOCKS) > 1000:
            _PROOF_LOCKS.clear()
            _PROOF_LOCKS[uid] = lock
    return lock


def _kb_skip_proof():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="review:skip_proof")
    ]])


def _kb_proof_done(n: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"✅ Готово ({n} фото)", callback_data="review:proof_done")
    ]])


async def _upsert_status(message: Message, state: FSMContext, state_key: str,
                          text: str, kb=None):
    """Единое статус-сообщение: редактирует существующее; если не вышло —
    шлёт новое и снимает клавиатуру со старого (чтобы не осталось мёртвых кнопок).
    Вызывать ТОЛЬКО под _proof_lock."""
    data = await state.get_data()
    status_id = data.get(state_key)
    if status_id:
        try:
            await message.bot.edit_message_text(
                text, chat_id=message.chat.id, message_id=status_id, reply_markup=kb)
            return
        except Exception:
            pass
    sent = await message.answer(text, reply_markup=kb)
    if status_id:
        # Старое сообщение не отредактировалось — убираем с него кнопку
        try:
            await message.bot.edit_message_reply_markup(
                chat_id=message.chat.id, message_id=status_id, reply_markup=None)
        except Exception:
            pass
    await state.update_data(**{state_key: sent.message_id})


async def _add_proof_photo(message: Message, state: FSMContext, bot):
    """Скачивает фото, атомарно добавляет в пруфы и обновляет единый статус.
    Возвращает (список b64, надо_ли_продолжать) — True ровно один раз при лимите."""
    import base64
    b64 = None
    # Отсекаем слишком большие фото до скачивания (>10 МБ)
    photo = message.photo[-1]
    if photo.file_size and photo.file_size > 10 * 1024 * 1024:
        await message.answer("⚠️ Фото слишком большое (макс 10 МБ). Пришли поменьше.")
        data = await state.get_data()
        return list(data.get("proofs_b64", [])), False
    try:
        file = await bot.get_file(photo.file_id)
        buf = await bot.download_file(file.file_path)
        b64 = base64.b64encode(_compress_proof(buf.read())).decode()
    except Exception as e:
        logger.error(f"Не удалось скачать пруф: {e}")

    async with _proof_lock(message.from_user.id):
        data = await state.get_data()
        proofs = list(data.get("proofs_b64", []))
        if data.get("proofs_done") or len(proofs) >= MAX_PROOFS:
            return proofs, False
        if b64 is not None:
            proofs.append(b64)
        if len(proofs) >= MAX_PROOFS:
            # Лимит достигнут этим фото — продолжаем ровно один раз
            await state.update_data(proofs_b64=proofs, proofs_done=True)
            return proofs, True
        await state.update_data(proofs_b64=proofs)
        # Статус обновляем ПОД ЛОКОМ — иначе альбом плодит несколько сообщений
        if proofs:
            await _upsert_status(
                message, state, "proof_status_msg_id",
                f"📸 Добавлено <b>{len(proofs)}/{MAX_PROOFS}</b>. Пришли ещё или жми «Готово».",
                _kb_proof_done(len(proofs)))
        return proofs, False


async def _proceed_after_proofs(event, state: FSMContext, db: Database, bot, config):
    """Собирает текст/entities/пруфы из состояния и идёт дальше по флоу."""
    import base64
    from aiogram.types import MessageEntity
    data = await state.get_data()
    text = data.get("review_text", "")
    raw_entities = data.get("review_entities", [])
    entities = [MessageEntity(**e) for e in raw_entities] if raw_entities else []
    proofs = [base64.b64decode(p) for p in data.get("proofs_b64", [])] or None
    # Убираем статус-сообщение с кнопкой «Готово»
    status_id = data.get("proof_status_msg_id")
    if status_id:
        chat_id = event.message.chat.id if hasattr(event, "message") and event.message else event.chat.id
        try:
            await bot.delete_message(chat_id, status_id)
        except Exception:
            pass
        await state.update_data(proof_status_msg_id=None)
    await _gate_anon(event, state, db, bot, config,
                     text=text, entities=entities, proofs=proofs)


@router.message(ReviewSG.enter_text, F.photo)
async def step_enter_text_with_photo(message: Message, state: FSMContext, db: Database, bot, config):
    """Покупатель прислал фото с подписью — подпись=отзыв, фото=пруф."""
    text = (message.caption or "").strip()
    if not text:
        await message.answer("✏️ Добавь текст отзыва в подпись к фото, либо отправь сначала текст.")
        return
    if len(text) > REVIEW_MAX_LEN:
        await message.answer(f"❌ Отзыв слишком длинный ({len(text)} симв.). Максимум {REVIEW_MAX_LEN}.")
        return

    # Сохраняем текст и первый пруф, даём добрать ещё фото (до 5)
    await state.update_data(
        review_text=text,
        review_entities=[e.model_dump() for e in (message.caption_entities or [])],
    )
    await state.set_state(ReviewSG.enter_proof)
    proofs, proceed = await _add_proof_photo(message, state, bot)
    if proceed:
        await _proceed_after_proofs(message, state, db, bot, config)


@router.message(ReviewSG.enter_text)
async def step_enter_text(message: Message, state: FSMContext, db: Database, bot, config):
    text = (message.text or "").strip()

    if not text:
        await message.answer("✏️ Напиши текст отзыва.")
        return
    if len(text) > REVIEW_MAX_LEN:
        await message.answer(f"❌ Отзыв слишком длинный ({len(text)} симв.). Максимум {REVIEW_MAX_LEN}.")
        return
    if len(text) > REVIEW_SOFT_LEN:
        await message.answer(f"⚠️ Отзыв немного длинноват ({len(text)} симв.) — карточка подстроится, но лучше покороче 😊")

    # Сохраняем текст и переходим к шагу пруфа
    await state.update_data(review_text=text, review_entities=[e.model_dump() for e in (message.entities or [])])
    await state.set_state(ReviewSG.enter_proof)
    await message.answer(
        "📸 Хочешь приложить <b>фото-доказательства сделки</b>?\n\n"
        "Отправь до 5 фото — они появятся прямо в карточке отзыва. "
        "Или нажми «Пропустить».",
        reply_markup=_kb_skip_proof()
    )


@router.message(ReviewSG.enter_proof, F.photo)
async def step_enter_proof(message: Message, state: FSMContext, db: Database, bot, config):
    proofs, proceed = await _add_proof_photo(message, state, bot)
    if proceed:
        # Лимит достигнут — продолжаем автоматически (ровно один раз)
        await _proceed_after_proofs(message, state, db, bot, config)


@router.callback_query(ReviewSG.enter_proof, F.data == "review:proof_done")
async def cb_proof_done(call: CallbackQuery, state: FSMContext, db: Database, bot, config):
    await call.answer()
    async with _proof_lock(call.from_user.id):
        data = await state.get_data()
        if data.get("proofs_done"):
            return
        await state.update_data(proofs_done=True)
    # Используем call — нужен реальный from_user покупателя
    await _proceed_after_proofs(call, state, db, bot, config)


@router.message(ReviewSG.enter_proof)
async def step_enter_proof_wrong(message: Message, state: FSMContext):
    data = await state.get_data()
    n = len(data.get("proofs_b64", []))
    if n:
        # Кнопка «Готово» живёт в статус-сообщении — не плодим вторую
        await message.answer(f"📸 Отправь фото или жми «Готово» выше (добавлено {n}/{MAX_PROOFS}).")
    else:
        await message.answer("📸 Отправь именно фото, либо нажми «Пропустить».", reply_markup=_kb_skip_proof())


@router.callback_query(ReviewSG.enter_proof, F.data == "review:skip_proof")
async def cb_skip_proof(call: CallbackQuery, state: FSMContext, db: Database, bot, config):
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    # Используем call (не call.message) — нужен реальный from_user покупателя
    await _proceed_after_proofs(call, state, db, bot, config)


# ── Принять / Отклонить отзыв (публикация в канал) ────────────────────────

@router.callback_query(F.data.startswith("review:accept:"))
async def cb_review_accept(call: CallbackQuery, bot: Bot, db: Database, config):
    review_id = int(call.data.split(":", 2)[2])
    seller_id = call.from_user.id

    ch = await db.get_seller_channel(seller_id)
    if not ch or not ch["verified"]:
        await call.answer("Канал не подключён", show_alert=True)
        return

    # Достаём данные отзыва из БД
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT card_file_id, buyer_name, buyer_username, buyer_id, review_text, stars, show_buyer_button, is_anonymous FROM rvb_reviews WHERE id = $1",
            review_id
        )
    if not row or not row["card_file_id"]:
        await call.answer("Файл карточки не найден", show_alert=True)
        return

    file_id = row["card_file_id"]
    buyer_name = row["buyer_name"]
    buyer_username = row["buyer_username"]
    buyer_id = row["buyer_id"]
    is_anon = bool(row["is_anonymous"])

    # Получаем название магазина продавца (нужно и для анон-ника)
    seller = await db.get_seller(seller_id)
    shop_name = seller["shop_name"] if seller else ""

    # Анонимность: в канале показываем анон-ник, кнопку профиля убираем
    if is_anon:
        display_name = (seller.get("anon_nickname") if seller else None) or "Анонимный покупатель"
        channel_kb = None  # никакой ссылки на покупателя
    else:
        display_name = buyer_name
        show_btn = row["show_buyer_button"] if row["show_buyer_button"] is not None else True
        buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={buyer_id}"
        buyer_btn = InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)
        channel_kb = InlineKeyboardMarkup(inline_keyboard=[[buyer_btn]]) if show_btn else None

    # Строим подпись — с анон-ником при анонимности
    stars_line = "★" * row["stars"] if row["stars"] > 0 else ""
    caption_parts = [f"<b>{_esc(shop_name)}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{_esc(row['review_text'])}»</i>")
    caption_parts.append(f"\n— {_esc(display_name)}")
    caption = "\n".join(caption_parts)

    try:
        posted = await bot.send_photo(
            chat_id=ch["channel_id"],
            photo=file_id,
            caption=caption,
            reply_markup=channel_kb,
            parse_mode="HTML",
        )
    except Exception as e:
        await call.answer(f"Ошибка публикации: {e}", show_alert=True)
        return

    await db.set_review_status(review_id, "accepted")

    # ── Нумерация ──
    mode = ch.get("numbering_mode", "off")
    if mode == "auto":
        await _place_review_number(bot, db, config, seller_id, review_id,
                                   ch["channel_id"], posted.message_id, ch)
    elif mode == "ask":
        # Спрашиваем продавца в ЛС — ставить ли номер этому отзыву
        try:
            await bot.send_message(
                seller_id,
                "🔢 Добавить номер этому отзыву в канале?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Да",
                        callback_data=f"num:yes:{review_id}:{posted.message_id}"),
                    InlineKeyboardButton(text="❌ Нет", callback_data="num:no"),
                ]]))
        except Exception:
            pass

    # Убираем кнопки из текущего сообщения (у продавца в ЛС) или удаляем его
    try:
        if call.message.photo:
            await call.message.edit_reply_markup(reply_markup=channel_kb)
        else:
            await call.message.delete()
    except Exception:
        pass

    await call.answer("✅ Опубликовано в канале!")


@router.callback_query(F.data == "num:no")
async def cb_num_no(call: CallbackQuery):
    await call.answer("Ок, без номера")
    try:
        await call.message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("num:yes:"))
async def cb_num_yes(call: CallbackQuery, bot: Bot, db: Database, config):
    parts = call.data.split(":")
    review_id = int(parts[2])
    channel_msg_id = int(parts[3])
    seller_id = call.from_user.id

    ch = await db.get_seller_channel(seller_id)
    if not ch or not ch["verified"]:
        await call.answer("Канал не подключён", show_alert=True)
        try:
            await call.message.delete()
        except Exception:
            pass
        return

    # Проверяем, жив ли отзыв в канале — вдруг продавец успел удалить
    cache_chat = getattr(config, "CACHE_CHAT_ID", None)
    alive = await _msg_alive(bot, ch["channel_id"], channel_msg_id, cache_chat)
    if not alive:
        await call.answer("Отзыв уже удалён из канала", show_alert=True)
        try:
            await call.message.delete()
        except Exception:
            pass
        return

    ok = await _place_review_number(bot, db, config, seller_id, review_id,
                                    ch["channel_id"], channel_msg_id, ch)
    await call.answer("✅ Номер добавлен" if ok else "Не удалось добавить номер")
    try:
        await call.message.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("review:reject"))
async def cb_review_reject(call: CallbackQuery, bot: Bot, db: Database):
    parts = call.data.split(":", 2)
    review_id = int(parts[2]) if len(parts) > 2 else None

    if review_id:
        await db.set_review_status(review_id, "rejected")

    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("Отзыв отклонён")


# ── Редактирование пруфов продавцом (безопасность клиентов) ────────────────

class ProofEditSG(StatesGroup):
    waiting = State()


@router.callback_query(F.data.startswith("proofedit:"))
async def cb_proofedit(call: CallbackQuery, state: FSMContext, db: Database):
    review_id = int(call.data.split(":")[1])
    review = await db.get_review(review_id)
    if not review or review["seller_id"] != call.from_user.id:
        await call.answer("Недоступно", show_alert=True)
        return
    needed = review.get("proof_count") or 0
    if needed <= 0:
        await call.answer("У этого отзыва нет пруфов", show_alert=True)
        return
    await call.answer()
    await state.set_state(ProofEditSG.waiting)
    await state.update_data(pe_review_id=review_id, pe_needed=needed,
                            pe_collected=[], pe_card_msg_id=call.message.message_id,
                            pe_done=False, pe_status_msg_id=None)
    word = "фото" if needed == 1 else f"{needed} фото"
    await call.message.answer(
        f"🖼 Пришли <b>{word}</b> — они заменят текущие пруфы на карточке.\n"
        f"/cancel — отмена."
    )


@router.message(ProofEditSG.waiting, F.text == "/cancel")
async def pe_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено. Карточка не изменена.")


@router.message(ProofEditSG.waiting, F.photo)
async def pe_photo(message: Message, state: FSMContext, db: Database, bot, config):
    import base64
    b64 = None
    photo = message.photo[-1]
    if photo.file_size and photo.file_size > 10 * 1024 * 1024:
        await message.answer("⚠️ Фото слишком большое (макс 10 МБ). Пришли поменьше.")
        return
    try:
        file = await bot.get_file(photo.file_id)
        buf = await bot.download_file(file.file_path)
        b64 = base64.b64encode(_compress_proof(buf.read())).decode()
    except Exception as e:
        logger.error(f"Не удалось скачать фото для замены пруфа: {e}")
        await message.answer("⚠️ Не удалось загрузить фото, пришли ещё раз.")
        return

    async with _proof_lock(message.from_user.id):
        data = await state.get_data()
        if data.get("pe_done"):
            return
        collected = list(data.get("pe_collected", []))
        needed = data["pe_needed"]
        collected.append(b64)
        if len(collected) < needed:
            await state.update_data(pe_collected=collected)
            # Единый статус (редактируется, а не плодит сообщения при альбоме)
            await _upsert_status(
                message, state, "pe_status_msg_id",
                f"📸 <b>{len(collected)}/{needed}</b> — жду ещё.")
            return
        # Все собраны — помечаем и выходим из-под лока на генерацию
        await state.update_data(pe_collected=collected, pe_done=True)

    # Все фото собраны — перегенерируем карточку
    review_id = data["pe_review_id"]
    card_msg_id = data.get("pe_card_msg_id")
    await state.clear()
    review = await db.get_review(review_id)
    if not review:
        await message.answer("❌ Отзыв не найден.")
        return
    seller = await db.get_seller(review["seller_id"])
    if not seller:
        await message.answer("❌ Продавец не найден.")
        return

    pe_status_id = data.get("pe_status_msg_id")
    if pe_status_id:
        try:
            await bot.delete_message(message.chat.id, pe_status_id)
        except Exception:
            pass
    status = await message.answer("⏳ Перегенерирую карточку...")
    buyer_name = review["buyer_name"] or "Покупатель"
    review_is_anon = bool(review.get("is_anonymous"))

    # При анон-отзыве сохраняем анонимность и при перегенерации
    if review_is_anon:
        display_name = (seller.get("anon_nickname") if seller else None) or "Анонимный покупатель"
        display_initials = "?"
        anon_av = seller.get("anon_avatar")
        if anon_av:
            try:
                display_avatar = base64.b64decode(anon_av)
            except Exception:
                display_avatar = None
        else:
            display_avatar = None
    else:
        display_name = buyer_name
        display_initials = "".join(w[0].upper() for w in buyer_name.split() if w)[:2]
        display_avatar = await _get_avatar_bytes(bot, review["buyer_id"])

    proofs = [base64.b64decode(p) for p in collected]

    card_data = {
        "shop_name": seller["shop_name"],
        "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID: {seller['id']}",
        "buyer_name": display_name,
        "buyer_initials": display_initials,
        "review_text": review["review_text"],
        "item_bought": review.get("item_bought", ""),
        "stars": review.get("stars", 0),
        "stars_mode": seller["stars_mode"],
        "template_id": review.get("template_used") or seller["template_id"],
        "avatar_bytes": display_avatar,
        "entities": [],
        "bot": bot,
        "bot_username": config.BOT_USERNAME,
        "db": db,
        "proof_list": proofs,
        "verify_code": review.get("verify_code"),
    }
    try:
        img_bytes = await generate_card(card_data)
    except Exception as e:
        await status.edit_text(f"❌ Ошибка генерации: {e}")
        return

    # Восстанавливаем подпись и клавиатуру карточки (анон → анон-ник, без кнопки профиля)
    stars_line = "★" * review.get("stars", 0) if review.get("stars", 0) > 0 else ""
    caption_parts = [f"<b>{_esc(seller['shop_name'])}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{_esc(review['review_text'])}»</i>")
    caption_parts.append(f"\n— {_esc(display_name)}")
    review_caption = "\n".join(caption_parts)

    buyer_username = review.get("buyer_username")
    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={review['buyer_id']}"
    ch = await db.get_seller_channel(seller["id"])
    channel_verified = ch and ch["verified"]

    rows = []
    if not review_is_anon and review.get("show_buyer_button", True):
        rows.append([InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)])
    if channel_verified and review.get("status") == "pending":
        rows.append([
            InlineKeyboardButton(text="✅ Принять", callback_data=f"review:accept:{review_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{review_id}"),
        ])
    rows.append([InlineKeyboardButton(text="🖼 Редактировать пруф",
                                      callback_data=f"proofedit:{review_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    # Пробуем заменить фото прямо в исходном сообщении с карточкой
    from aiogram.types import InputMediaPhoto
    new_file_id = None
    edited = False
    if card_msg_id:
        try:
            edited_msg = await bot.edit_message_media(
                chat_id=message.chat.id, message_id=card_msg_id,
                media=InputMediaPhoto(
                    media=BufferedInputFile(img_bytes, filename="review.png"),
                    caption=f"⭐ Новый отзыв!\n\n{review_caption}",
                    parse_mode="HTML",
                ),
                reply_markup=kb,
            )
            new_file_id = edited_msg.photo[-1].file_id if edited_msg.photo else None
            edited = True
        except Exception as e:
            logger.info(f"Не удалось отредактировать карточку на месте: {e}")

    if not edited:
        # Фолбэк — шлём новую карточку
        sent = await message.answer_photo(
            BufferedInputFile(img_bytes, filename="review.png"),
            caption=f"⭐ Новый отзыв!\n\n{review_caption}",
            reply_markup=kb,
        )
        new_file_id = sent.photo[-1].file_id if sent.photo else None

    if new_file_id:
        await db.update_review_card(review_id, new_file_id)

    try:
        await status.delete()
    except Exception:
        pass
    await message.answer("✅ Пруфы заменены, карточка обновлена.")


@router.message(ProofEditSG.waiting)
async def pe_wrong(message: Message, state: FSMContext):
    data = await state.get_data()
    left = data["pe_needed"] - len(data.get("pe_collected", []))
    await message.answer(f"📸 Жду фото ({left} шт.) или /cancel.")
