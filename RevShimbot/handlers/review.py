from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import logging
import asyncio

from db import Database
from keyboards import kb_buyer_stars, kb_templates, kb_cancel
from constants import REVIEW_MAX_LEN, REVIEW_SOFT_LEN
from services.card_generator import generate_card

logger = logging.getLogger(__name__)
router = Router()


class ReviewSG(StatesGroup):
    choose_template = State()
    choose_stars = State()
    enter_item = State()
    enter_text = State()
    enter_proof = State()
    ask_inline_btn = State()


async def start_review_flow(message: Message, seller: dict, state: FSMContext, db: Database):
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
        im = Image.open(io.BytesIO(raw)).convert("RGB")
        if max(im.size) > max_side:
            im.thumbnail((max_side, max_side), Image.LANCZOS)
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=82, optimize=True)
        return out.getvalue()
    except Exception:
        return raw


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


async def _gate_inline_button(event, state: FSMContext, db: Database, bot, config,
                               text: str, entities, proof_bytes=None):
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
            pending_proof_b64=base64.b64encode(proof_bytes).decode() if proof_bytes else None,
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
                          entities=entities, proof_bytes=proof_bytes)


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
    proof_b64 = data.get("pending_proof_b64")
    proof_bytes = base64.b64decode(proof_b64) if proof_b64 else None
    await finalize_review(call, state, db, bot, config, text=text,
                          entities=entities, proof_bytes=proof_bytes)


async def finalize_review(event, state: FSMContext, db: Database, bot, config,
                          text: str, entities, proof_bytes=None):
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

    avatar_bytes = await _get_avatar_bytes(bot, user.id)

    await answer("⏳ Генерирую карточку...")

    card_data = {
        "shop_name": seller["shop_name"],
        "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID: {seller['id']}",
        "buyer_name": buyer_name,
        "buyer_initials": "".join(w[0].upper() for w in buyer_name.split() if w)[:2],
        "review_text": text,
        "item_bought": data.get("item_bought", ""),
        "stars": data.get("stars", 0),
        "stars_mode": seller["stars_mode"],
        "template_id": data.get("template_id", seller["template_id"]),
        "avatar_bytes": avatar_bytes,
        "entities": entities or [],
        "bot": bot,
        "bot_username": config.BOT_USERNAME,
        "db": db,
        "proof_bytes": proof_bytes,
    }

    try:
        img_bytes = await generate_card(card_data)
    except Exception as e:
        await answer(f"❌ Ошибка генерации карточки: {e}")
        return

    await answer(
        f"✅ Вот твоя карточка отзыва!\n\n"
        f"Отправь её продавцу <b>{seller['shop_name']}</b> в личные сообщения. "
        f"При пересылке можешь скрыть «переслано от...»"
    )

    stars_line = "★" * data.get("stars", 0) if data.get("stars", 0) > 0 else ""
    caption_parts = [f"<b>{seller['shop_name']}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{text}»</i>")
    caption_parts.append(f"\n— {buyer_name}")
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

    # Решаем, показывать ли кнопку-ссылку на покупателя (нужно ДО сохранения)
    btn_mode = seller.get("inline_button_mode", "shown")
    if btn_mode == "hidden":
        show_button = False
    elif btn_mode == "ask":
        show_button = data.get("buyer_wants_button", False)
    else:  # shown
        show_button = True

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
        show_buyer_button=show_button,
    )
    review_id = review_row["id"]
    await state.clear()

    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={user.id}"

    ch = await db.get_seller_channel(seller["id"])
    channel_verified = ch and ch["verified"]

    rows = []
    if show_button:
        rows.append([InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)])
    if channel_verified:
        rows.append([
            InlineKeyboardButton(text="✅ Принять", callback_data=f"review:accept:{review_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{review_id}"),
        ])
    seller_kb = InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    try:
        await bot.send_photo(
            chat_id=seller["id"],
            photo=BufferedInputFile(img_bytes, filename="review.png"),
            caption=(f"⭐ Новый отзыв!\n\n{review_caption}"),
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


def _kb_skip_proof():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="review:skip_proof")
    ]])


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

    # Скачиваем фото-пруф (самое большое)
    proof_bytes = None
    try:
        file = await bot.get_file(message.photo[-1].file_id)
        buf = await bot.download_file(file.file_path)
        proof_bytes = _compress_proof(buf.read())
    except Exception as e:
        logger.error(f"Не удалось скачать пруф: {e}")

    await _gate_inline_button(message, state, db, bot, config,
                          text=text, entities=message.caption_entities, proof_bytes=proof_bytes)


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
        "📸 Хочешь приложить <b>фото-доказательство сделки</b>?\n\n"
        "Отправь фото — оно появится прямо в карточке отзыва. "
        "Или нажми «Пропустить».",
        reply_markup=_kb_skip_proof()
    )


@router.message(ReviewSG.enter_proof, F.photo)
async def step_enter_proof(message: Message, state: FSMContext, db: Database, bot, config):
    data = await state.get_data()
    text = data.get("review_text", "")

    proof_bytes = None
    try:
        file = await bot.get_file(message.photo[-1].file_id)
        buf = await bot.download_file(file.file_path)
        proof_bytes = _compress_proof(buf.read())
    except Exception as e:
        logger.error(f"Не удалось скачать пруф: {e}")

    # Восстанавливаем entities
    from aiogram.types import MessageEntity
    raw_entities = data.get("review_entities", [])
    entities = [MessageEntity(**e) for e in raw_entities] if raw_entities else []

    await _gate_inline_button(message, state, db, bot, config,
                          text=text, entities=entities, proof_bytes=proof_bytes)


@router.message(ReviewSG.enter_proof)
async def step_enter_proof_wrong(message: Message, state: FSMContext):
    await message.answer("📸 Отправь именно фото, либо нажми «Пропустить».", reply_markup=_kb_skip_proof())


@router.callback_query(ReviewSG.enter_proof, F.data == "review:skip_proof")
async def cb_skip_proof(call: CallbackQuery, state: FSMContext, db: Database, bot, config):
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    data = await state.get_data()
    text = data.get("review_text", "")
    from aiogram.types import MessageEntity
    raw_entities = data.get("review_entities", [])
    entities = [MessageEntity(**e) for e in raw_entities] if raw_entities else []
    # Используем call (не call.message) — нужен реальный from_user покупателя
    await _gate_inline_button(call, state, db, bot, config,
                          text=text, entities=entities, proof_bytes=None)


# ── Принять / Отклонить отзыв (публикация в канал) ────────────────────────

@router.callback_query(F.data.startswith("review:accept:"))
async def cb_review_accept(call: CallbackQuery, bot: Bot, db: Database):
    review_id = int(call.data.split(":", 2)[2])
    seller_id = call.from_user.id

    ch = await db.get_seller_channel(seller_id)
    if not ch or not ch["verified"]:
        await call.answer("Канал не подключён", show_alert=True)
        return

    # Достаём данные отзыва из БД
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT card_file_id, buyer_name, buyer_username, buyer_id, review_text, stars, show_buyer_button FROM rvb_reviews WHERE id = $1",
            review_id
        )
    if not row or not row["card_file_id"]:
        await call.answer("Файл карточки не найден", show_alert=True)
        return

    file_id = row["card_file_id"]
    buyer_name = row["buyer_name"]
    buyer_username = row["buyer_username"]
    buyer_id = row["buyer_id"]
    # Уважаем выбор покупателя: показывать ли ссылку на него
    show_btn = row["show_buyer_button"] if row["show_buyer_button"] is not None else True
    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={buyer_id}"
    buyer_btn = InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)
    channel_kb = InlineKeyboardMarkup(inline_keyboard=[[buyer_btn]]) if show_btn else None

    # Получаем название магазина продавца
    seller = await db.get_seller(seller_id)
    shop_name = seller["shop_name"] if seller else ""

    # Строим подпись идентично основному флоу
    stars_line = "★" * row["stars"] if row["stars"] > 0 else ""
    caption_parts = [f"<b>{shop_name}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{row['review_text']}»</i>")
    caption_parts.append(f"\n— {buyer_name}")
    caption = "\n".join(caption_parts)

    try:
        await bot.send_photo(
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

    # Убираем кнопки из текущего сообщения или удаляем его
    try:
        if call.message.photo:
            new_kb = InlineKeyboardMarkup(inline_keyboard=[[buyer_btn]]) if show_btn else None
            await call.message.edit_reply_markup(reply_markup=new_kb)
        else:
            await call.message.delete()
    except Exception:
        pass

    await call.answer("✅ Опубликовано в канале!")


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
