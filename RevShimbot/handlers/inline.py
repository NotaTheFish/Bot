from aiogram import Router
from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent
import hashlib

from db import Database

router = Router()


@router.inline_query()
async def inline_review(query: InlineQuery, db: Database):
    text = query.query.strip()
    if not text:
        await query.answer(
            results=[],
            switch_pm_text="Напиши текст отзыва после @бота",
            switch_pm_parameter="inline_help",
            cache_time=1
        )
        return

    # Пробуем найти продавца по chat_id (если бот знает чат)
    # В inline mode нет прямого доступа к chat_id собеседника,
    # поэтому предлагаем универсальную карточку с текстом
    result_id = hashlib.md5(text.encode()).hexdigest()

    await query.answer(
        results=[
            InlineQueryResultArticle(
                id=result_id,
                title="📝 Отправить отзыв",
                description=text[:100],
                input_message_content=InputTextMessageContent(
                    message_text=(
                        f"⭐️⭐️⭐️⭐️⭐️\n\n"
                        f"<i>«{text}»</i>\n\n"
                        f"— {query.from_user.full_name}"
                    ),
                    parse_mode="HTML"
                ),
                thumb_url="https://i.imgur.com/placeholder.png"
            )
        ],
        cache_time=1
    )
