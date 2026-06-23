from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import logging

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


async def start_review_flow(message: Message, seller: dict, state: FSMContext, db: Database):
    await state.update_data(seller=seller, mode="buyer")

    shop = seller["shop_name"]
    seller_tag = f"@{seller['username']}" if seller.get("username") else f"tg://user?id={seller['id']}"

    await message.answer(
        f"👋 Ты оставляешь отзыв для магазина <b>{shop}</b> ({seller_tag})\n\n"
        f"Давай создадим красивую карточку!"
    )

    if seller["allow_template_choice"]:
        await state.set_state(ReviewSG.choose_template)
        # Показываем кастомные шаблоны ПРОДАВЦА — ведь отзыв для его магазина
        customs = await db.list_custom_templates(seller["id"])
        await message.answer(
            "🎨 Выбери стиль карточки:",
            reply_markup=kb_templates(seller["template_id"], customs)
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


@router.message(ReviewSG.enter_text)
async def step_enter_text(message: Message, state: FSMContext, db: Database, bot, config):
    text = message.text.strip()

    if len(text) > REVIEW_MAX_LEN:
        await message.answer(
            f"❌ Отзыв слишком длинный ({len(text)} симв.). Максимум {REVIEW_MAX_LEN}."
        )
        return

    if len(text) > REVIEW_SOFT_LEN:
        await message.answer(
            f"⚠️ Отзыв немного длинноват ({len(text)} симв.) — карточка подстроится, но лучше покороче 😊"
        )

    data = await state.get_data()
    seller = data["seller"]

    buyer_name = message.from_user.full_name or message.from_user.first_name or "Покупатель"
    buyer_username = message.from_user.username

    # Пробуем получить аватарку
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
        "entities": message.entities or [],
        "bot": bot,
        "bot_username": config.BOT_USERNAME,
        "db": db,
    }

    try:
        img_bytes = await generate_card(card_data)
    except Exception as e:
        await message.answer(f"❌ Ошибка генерации карточки: {e}")
        return

    # Сначала отдельным сообщением — подсказка
    await message.answer(
        f"✅ Вот твоя карточка отзыва!\n\n"
        f"Отправь её продавцу <b>{seller['shop_name']}</b> в личные сообщения. "
        f"При пересылке можешь скрыть «переслано от...»"
    )

    # Затем карточка с дубликатом отзыва в подписи (для красивой пересылки)
    stars_line = "★" * data.get("stars", 0) if data.get("stars", 0) > 0 else ""
    caption_parts = [f"<b>{seller['shop_name']}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{text}»</i>")
    caption_parts.append(f"\n— {buyer_name}")
    review_caption = "\n".join(caption_parts)

    sent = await message.answer_photo(
        BufferedInputFile(img_bytes, filename="review.png"),
        caption=review_caption
    )

    # Сохраняем отзыв в БД
    file_id = sent.photo[-1].file_id if sent.photo else None
    review_row = await db.save_review(
        seller_id=seller["id"],
        buyer_id=message.from_user.id,
        buyer_name=buyer_name,
        buyer_username=buyer_username,
        review_text=text,
        item_bought=data.get("item_bought", ""),
        stars=data.get("stars", 0),
        template_used=data.get("template_id", seller["template_id"]),
        card_file_id=file_id,
    )
    review_id = review_row["id"]
    await state.clear()

    # Отправляем карточку продавцу автоматически
    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={message.from_user.id}"

    ch = await db.get_seller_channel(seller["id"])
    channel_verified = ch and ch["verified"]

    buyer_btn = InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)
    if channel_verified:
        seller_kb = InlineKeyboardMarkup(inline_keyboard=[
            [buyer_btn],
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"review:accept:{review_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{review_id}"),
            ]
        ])
    else:
        seller_kb = InlineKeyboardMarkup(inline_keyboard=[[buyer_btn]])

    try:
        await bot.send_photo(
            chat_id=seller["id"],
            photo=BufferedInputFile(img_bytes, filename="review.png"),
            caption=(
                f"⭐ Новый отзыв!\n\n"
                f"{review_caption}"
            ),
            reply_markup=seller_kb,
            parse_mode="HTML",
        )
        logger.info(f"Карточка отправлена продавцу {seller['id']}")
    except Exception as e:
        logger.error(f"Ошибка отправки карточки продавцу {seller['id']}: {e}")


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
            "SELECT card_file_id, buyer_name, buyer_username, buyer_id, review_text, stars FROM rvb_reviews WHERE id = $1",
            review_id
        )
    if not row or not row["card_file_id"]:
        await call.answer("Файл карточки не найден", show_alert=True)
        return

    file_id = row["card_file_id"]
    buyer_name = row["buyer_name"]
    buyer_username = row["buyer_username"]
    buyer_id = row["buyer_id"]
    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={buyer_id}"
    buyer_btn = InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)
    channel_kb = InlineKeyboardMarkup(inline_keyboard=[[buyer_btn]])

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
            new_kb = InlineKeyboardMarkup(inline_keyboard=[[buyer_btn]])
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
