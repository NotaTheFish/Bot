import hashlib
import logging
from aiogram import Router
from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedPhoto,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
)

from db import Database
from services.card_generator import generate_card

router = Router()
logger = logging.getLogger(__name__)


async def get_or_generate_card(
    query: InlineQuery,
    seller: dict,
    review_text: str,
    bot,
    config,
) -> str | None:
    """
    Генерирует карточку, загружает в CACHE_CHAT_ID и возвращает file_id.
    Если карточка уже была сгенерирована для этого текста — возвращает
    закешированный file_id из БД (TODO: можно добавить кеш в памяти).
    """
    if not config.CACHE_CHAT_ID:
        return None

    buyer_name = query.from_user.full_name or query.from_user.first_name or "Трейдер"
    buyer_initials = "".join(w[0].upper() for w in buyer_name.split() if w)[:2]

    # Аватарку в inline не берём — слишком медленно для живого запроса
    card_data = {
        "shop_name": seller["shop_name"],
        "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID:{seller['id']}",
        "buyer_name": buyer_name,
        "buyer_initials": buyer_initials,
        "review_text": review_text,
        "item_bought": seller["item_value"] if seller["item_mode"] == "fixed" else "",
        "stars": seller["stars_value"] if seller["stars_mode"] == "fixed" else 5,
        "stars_mode": seller["stars_mode"],
        "template_id": seller["template_id"],
        "avatar_bytes": None,
    }

    try:
        img_bytes = await generate_card(card_data)
    except Exception as e:
        logger.error(f"Card generation failed in inline: {e}")
        return None

    try:
        sent = await bot.send_photo(
            chat_id=config.CACHE_CHAT_ID,
            photo=BufferedInputFile(img_bytes, filename="review_cache.png"),
        )
        return sent.photo[-1].file_id
    except Exception as e:
        logger.error(f"Failed to upload card to cache chat: {e}")
        return None


@router.inline_query()
async def inline_review(query: InlineQuery, db: Database, bot, config):
    text = query.query.strip()

    if not text or len(text) < 3:
        await query.answer(
            results=[],
            switch_pm_text="✏️ Напиши текст отзыва после @бота",
            switch_pm_parameter="inline_help",
            cache_time=1,
        )
        return

    # Ищем продавца по chat — в inline нет прямого доступа к chat_id собеседника,
    # поэтому ищем по username чата если он есть в query.chat_type
    # Альтернатива: передаём seller_id через текст запроса: "123456 Отличная сделка!"
    seller = None
    parts = text.split(None, 1)

    if parts[0].startswith("seller_") and parts[0][7:].isdigit():
        # Формат из inline-кнопки: "seller_123456 текст отзыва"
        seller_id = int(parts[0][7:])
        review_text = parts[1].strip() if len(parts) > 1 else ""
        seller = await db.get_seller(seller_id)
    elif parts[0].isdigit():
        # Формат ручной: "123456 текст отзыва"
        seller_id = int(parts[0])
        review_text = parts[1].strip() if len(parts) > 1 else ""
        seller = await db.get_seller(seller_id)
    else:
        review_text = text

    result_id = hashlib.md5(f"{query.from_user.id}:{text}".encode()).hexdigest()

    if seller and config.CACHE_CHAT_ID and review_text:
        # Генерируем красивую карточку
        file_id = await get_or_generate_card(query, seller, review_text, bot, config)

        if file_id:
            buyer_name = query.from_user.full_name or "Трейдер"
            stars = "★" * (seller["stars_value"] if seller["stars_mode"] == "fixed" else 5)
            caption = (
                f"<b>{seller['shop_name']}</b>\n"
                f"{stars}\n\n"
                f"<i>«{review_text}»</i>\n\n"
                f"— {buyer_name}"
            )
            await query.answer(
                results=[
                    InlineQueryResultCachedPhoto(
                        id=result_id,
                        photo_file_id=file_id,
                        caption=caption,
                        parse_mode="HTML",
                        title=f"⭐️ Отзыв для {seller['shop_name']}",
                        description=review_text[:100],
                    )
                ],
                cache_time=30,
            )
            return

    # Фолбэк — текстовый отзыв если нет seller или CACHE_CHAT_ID
    buyer_name = query.from_user.full_name or "Трейдер"
    await query.answer(
        results=[
            InlineQueryResultArticle(
                id=result_id,
                title="📝 Отправить текстовый отзыв",
                description=text[:100],
                input_message_content=InputTextMessageContent(
                    message_text=(
                        f"⭐️⭐️⭐️⭐️⭐️\n\n"
                        f"<i>«{text}»</i>\n\n"
                        f"— {buyer_name}"
                    ),
                    parse_mode="HTML",
                ),
            )
        ],
        switch_pm_text="🎨 Получить красивую карточку",
        switch_pm_parameter="inline_card",
        cache_time=1,
    )
