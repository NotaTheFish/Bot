import hashlib
import logging
from aiogram import Router
from aiogram.types import (
    InlineQuery,
    InlineQueryResultCachedPhoto,
    InlineQueryResultArticle,
    InputTextMessageContent,
    BufferedInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
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
    db,
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
        "bot_username": config.BOT_USERNAME,
        "db": db,
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
    import re as _re
    text = query.query.strip()
    buyer_name = query.from_user.full_name or "Трейдер"

    parts = text.split(None, 1)
    first = parts[0] if parts else ""

    # Распознаём pub_id продавца: строго 4 заглавных буквы/цифры
    pub_id_match = bool(_re.fullmatch(r"[A-Z0-9]{4}", first))
    seller = None
    review_text = ""

    if pub_id_match:
        seller = await db.get_seller_by_pubid(first)
        review_text = parts[1].strip() if len(parts) > 1 else ""
    elif first.startswith("seller_"):
        # Обратная совместимость со старыми ссылками
        raw = first[7:]
        if raw.isdigit():
            seller = await db.get_seller(int(raw))
        else:
            seller = await db.get_seller_by_pubid(raw.upper())
        review_text = parts[1].strip() if len(parts) > 1 else ""
    else:
        review_text = text

    result_id = hashlib.md5(f"{query.from_user.id}:{text}".encode()).hexdigest()

    # ── СЛУЧАЙ 1: pub_id БЕЗ текста → приглашение «Написать отзыв» ──────────
    if seller and not review_text:
        ref_link = f"https://t.me/{config.BOT_USERNAME}?start=seller_{seller['pub_id']}"

        if config.CACHE_CHAT_ID:
            # Генерируем актуальную карточку-превью шаблона продавца
            preview_data = {
                "shop_name": seller["shop_name"],
                "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID:{seller['pub_id']}",
                "buyer_name": "Трейдер",
                "buyer_initials": "ТР",
                "review_text": "Здесь появится твой отзыв о продавце ✍️",
                "item_bought": seller["item_value"] if seller["item_mode"] == "fixed" else "",
                "stars": seller["stars_value"] if seller["stars_mode"] == "fixed" else 5,
                "stars_mode": seller["stars_mode"],
                "template_id": seller["template_id"],
                "avatar_bytes": None,
                "bot_username": config.BOT_USERNAME,
                "db": db,
            }
            try:
                img = await generate_card(preview_data)
                sent = await bot.send_photo(
                    chat_id=config.CACHE_CHAT_ID,
                    photo=BufferedInputFile(img, filename="invite.png"),
                )
                file_id = sent.photo[-1].file_id
            except Exception as e:
                logger.error(f"Invite card failed: {e}")
                file_id = None

            if file_id:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✍️ Написать отзыв в боте", url=ref_link)
                ]])
                caption = (
                    f"⭐️ <b>{seller['shop_name']}</b> просит оставить отзыв!\n\n"
                    f"Нажми кнопку ниже, чтобы написать красивый отзыв.\n\n"
                    f"💡 <i>Быстрее: напиши прямо в чате "
                    f"<code>@{config.BOT_USERNAME} {seller['pub_id']} твой отзыв</code> "
                    f"и нажми на всплывающую картинку.</i>"
                )
                await query.answer(
                    results=[
                        InlineQueryResultCachedPhoto(
                            id=result_id,
                            photo_file_id=file_id,
                            caption=caption,
                            parse_mode="HTML",
                            reply_markup=kb,
                            title=f"📨 Пригласить оставить отзыв — {seller['shop_name']}",
                            description="Отправь приглашение оставить отзыв",
                        )
                    ],
                    cache_time=5,
                )
                return

        # Фолбэк без CACHE_CHAT_ID — просто текст с кнопкой
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✍️ Написать отзыв в боте", url=ref_link)
        ]])
        await query.answer(
            results=[
                InlineQueryResultArticle(
                    id=result_id,
                    title=f"📨 Пригласить — {seller['shop_name']}",
                    description="Отправь приглашение оставить отзыв",
                    reply_markup=kb,
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            f"⭐️ <b>{seller['shop_name']}</b> просит оставить отзыв!\n\n"
                            f"💡 Быстрее: <code>@{config.BOT_USERNAME} {seller['pub_id']} твой отзыв</code>"
                        ),
                        parse_mode="HTML",
                    ),
                )
            ],
            cache_time=5,
        )
        return

    # ── СЛУЧАЙ 2: pub_id + текст → карточка отзыва по шаблону продавца ──────
    if seller and config.CACHE_CHAT_ID and review_text:
        file_id = await get_or_generate_card(query, seller, review_text, bot, config, db)
        if file_id:
            stars = "★" * (seller["stars_value"] if seller["stars_mode"] == "fixed" else 5)
            caption = (
                f"<b>{seller['shop_name']}</b>\n{stars}\n\n"
                f"<i>«{review_text}»</i>\n\n— {buyer_name}"
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

    # ── СЛУЧАЙ 3: нет pub_id, но есть клиентский шаблон ────────────────────
    if review_text and len(review_text) >= 3:
        client_tpl = await db.get_client_template(query.from_user.id)
        if client_tpl and config.CACHE_CHAT_ID:
            fake_seller = {
                "shop_name": buyer_name,
                "username": query.from_user.username,
                "id": query.from_user.id,
                "pub_id": None,
                "template_id": client_tpl["template_id"],
                "stars_mode": "buyer_choice", "stars_value": 5,
                "item_mode": "free", "item_value": "",
            }
            file_id = await get_or_generate_card(query, fake_seller, review_text, bot, config, db)
            if file_id:
                await query.answer(
                    results=[InlineQueryResultCachedPhoto(
                        id=result_id, photo_file_id=file_id,
                        caption=f"<i>«{review_text}»</i>\n\n— {buyer_name}",
                        parse_mode="HTML", title="⭐️ Отправить карточку отзыва",
                        description=review_text[:100],
                    )],
                    cache_time=30,
                )
                return

        # ── СЛУЧАЙ 4: нет шаблона → стабильная рандомная карточка ──────────
        if config.CACHE_CHAT_ID:
            STANDARD = ["classic_gold", "retro_paper", "dark_slate", "clean_white", "sketch_paper"]
            chosen = STANDARD[query.from_user.id % len(STANDARD)]
            fake_seller = {
                "shop_name": buyer_name, "username": query.from_user.username,
                "id": query.from_user.id, "pub_id": None, "template_id": chosen,
                "stars_mode": "buyer_choice", "stars_value": 5,
                "item_mode": "free", "item_value": "",
            }
            file_id = await get_or_generate_card(query, fake_seller, review_text, bot, config, db)
            if file_id:
                await query.answer(
                    results=[InlineQueryResultCachedPhoto(
                        id=result_id, photo_file_id=file_id,
                        caption=f"<i>«{review_text}»</i>\n\n— {buyer_name}",
                        parse_mode="HTML", title="⭐️ Отправить карточку отзыва",
                        description=review_text[:100],
                    )],
                    cache_time=30,
                )
                return

    # ── СЛУЧАЙ 5: совсем пусто или нет CACHE → подсказка ───────────────────
    await query.answer(
        results=[],
        switch_pm_text="✏️ Открыть бота чтобы оставить отзыв",
        switch_pm_parameter="inline_help",
        cache_time=1,
    )
