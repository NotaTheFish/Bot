import hashlib
import logging
import asyncio
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

# Кеш сгенерированных карточек в памяти: ключ -> file_id.
# Инлайн-запрос должен ответить за ~10 сек, а рендер Playwright медленный,
# поэтому одинаковые/повторные запросы берём из кеша мгновенно.
_INLINE_CARD_CACHE: dict[str, str] = {}
_INLINE_CACHE_MAX = 500

# Ожидающие инлайн-отзывы для уведомления продавца (когда покупатель реально отправит)
_PENDING_INLINE: dict[str, dict] = {}
_PENDING_MAX = 500

# Дебаунс инлайна: рендерим только когда юзер перестал печатать
_LAST_INLINE_Q: dict[int, str] = {}
_DEBOUNCE_SEC = 1.3

import html as _html_mod


def _esc(t) -> str:
    return _html_mod.escape(str(t or ""))


def _cache_key(seller: dict, review_text: str) -> str:
    raw = f"{seller.get('template_id')}|{seller.get('inline_template_id')}|{seller.get('shop_name')}|{seller.get('stars_mode')}|{seller.get('stars_value')}|{seller.get('item_mode')}|{seller.get('item_value')}|{review_text}"
    return hashlib.sha256(raw.encode()).hexdigest()


async def _safe_answer(query: InlineQuery, **kwargs):
    """Отвечает на инлайн-запрос, гася ошибку устаревшего запроса."""
    try:
        await query.answer(**kwargs)
    except Exception as e:
        # query is too old / invalid — пользователь уже изменил текст, это норма
        logger.info(f"Inline answer skipped (stale query): {e}")


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

    # Кеш в памяти — если такую же карточку уже рендерили, отдаём мгновенно
    ckey = _cache_key(seller, review_text)
    cached = _INLINE_CARD_CACHE.get(ckey)
    if cached:
        return cached

    buyer_name = query.from_user.full_name or query.from_user.first_name or "Трейдер"
    buyer_initials = "".join(w[0].upper() for w in buyer_name.split() if w)[:2]

    # Аватарку в inline не берём — слишком медленно для живого запроса
    inline_tpl = seller.get("inline_template_id") or seller["template_id"]
    card_data = {
        "shop_name": seller["shop_name"],
        "seller_tag": f"@{seller['username']}" if seller.get("username") else f"ID:{seller['id']}",
        "buyer_name": buyer_name,
        "buyer_initials": buyer_initials,
        "review_text": review_text,
        "item_bought": seller["item_value"] if seller["item_mode"] == "fixed" else "",
        "stars": seller["stars_value"] if seller["stars_mode"] == "fixed" else 5,
        "stars_mode": seller["stars_mode"],
        "template_id": inline_tpl,
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
        fid = sent.photo[-1].file_id
        # Чистим кеш-чат: file_id остаётся валидным после удаления сообщения
        try:
            await bot.delete_message(config.CACHE_CHAT_ID, sent.message_id)
        except Exception:
            pass
        # Сохраняем в кеш (с простым ограничением размера)
        if len(_INLINE_CARD_CACHE) >= _INLINE_CACHE_MAX:
            _INLINE_CARD_CACHE.clear()
        _INLINE_CARD_CACHE[ckey] = fid
        return fid
    except Exception as e:
        logger.error(f"Failed to upload card to cache chat: {e}")
        return None


@router.inline_query()
async def inline_review(query: InlineQuery, db: Database, bot, config):
    import re as _re
    raw = query.query
    text = raw.strip()

    # Если запрос пустой (юзер просто написал @username без текста) —
    # не показываем инлайн-панель, чтобы можно было отправить @username как текст.
    if not text:
        await _safe_answer(query, results=[], cache_time=1)
        return

    # Дебаунс: Telegram шлёт запрос почти на каждый символ — ждём паузу в наборе.
    # Рендерим только самый свежий запрос, устаревшие молча умирают.
    _uid = query.from_user.id
    _LAST_INLINE_Q[_uid] = query.id
    if len(_LAST_INLINE_Q) > 2000:
        _LAST_INLINE_Q.clear()
        _LAST_INLINE_Q[_uid] = query.id
    await asyncio.sleep(_DEBOUNCE_SEC)
    if _LAST_INLINE_Q.get(_uid) != query.id:
        return

    buyer_name = query.from_user.full_name or "Трейдер"

    parts = text.split(None, 1)
    first = parts[0] if parts else ""

    # Распознаём pub_id продавца: 4 буквы/цифры, регистр не важен (shim = SHIM)
    pub_id_match = bool(_re.fullmatch(r"[A-Za-z0-9]{4}", first))
    seller = None
    review_text = ""

    if pub_id_match:
        seller = await db.get_seller_by_pubid(first.upper())
        if seller:
            review_text = parts[1].strip() if len(parts) > 1 else ""
        else:
            # ID не существует (например продавец его сменил) —
            # всё слово становится частью отзыва: "SHIM спасибо" → отзыв целиком
            review_text = text
    elif first.startswith("seller_"):
        # Обратная совместимость со старыми ссылками
        raw = first[7:]
        if raw.isdigit():
            seller = await db.get_seller(int(raw))
        else:
            seller = await db.get_seller_by_pubid(raw.upper())
        if seller:
            review_text = parts[1].strip() if len(parts) > 1 else ""
        else:
            review_text = text
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
                "template_id": seller.get("inline_template_id") or seller["template_id"],
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
                try:
                    await bot.delete_message(config.CACHE_CHAT_ID, sent.message_id)
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Invite card failed: {e}")
                file_id = None

            if file_id:
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✍️ Написать отзыв в боте", url=ref_link)
                ]])
                caption = (
                    f"⭐️ <b>{_esc(seller['shop_name'])}</b> просит оставить отзыв!\n\n"
                    f"Нажми кнопку ниже, чтобы написать красивый отзыв.\n\n"
                    f"💡 <i>Быстрее: напиши прямо в чате "
                    f"<code>@{config.BOT_USERNAME} {seller['pub_id']} твой отзыв</code> "
                    f"и нажми на всплывающую картинку.</i>"
                )
                await _safe_answer(query,
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
        await _safe_answer(query,
            results=[
                InlineQueryResultArticle(
                    id=result_id,
                    title=f"📨 Пригласить — {seller['shop_name']}",
                    description="Отправь приглашение оставить отзыв",
                    reply_markup=kb,
                    input_message_content=InputTextMessageContent(
                        message_text=(
                            f"⭐️ <b>{_esc(seller['shop_name'])}</b> просит оставить отзыв!\n\n"
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
                f"<b>{_esc(seller['shop_name'])}</b>\n{stars}\n\n"
                f"<i>«{_esc(review_text)}»</i>\n\n— {_esc(buyer_name)}"
            )
            # Кнопка-ссылка на покупателя, если продавец её включил для инлайн
            inline_kb = None
            if seller.get("inline_button_show", True):
                buyer_uname = query.from_user.username
                buyer_url = f"https://t.me/{buyer_uname}" if buyer_uname else f"tg://user?id={query.from_user.id}"
                inline_kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)
                ]])

            # Если продавец хочет уведомления — регистрируем данные для chosen_inline_result
            r_id = result_id
            if seller.get("inline_notify_seller", False):
                import time as _time
                r_id = f"notify:{result_id}"
                _PENDING_INLINE[r_id] = {
                    "seller_id": seller["id"],
                    "buyer_id": query.from_user.id,
                    "buyer_name": buyer_name,
                    "buyer_username": query.from_user.username,
                    "review_text": review_text,
                    "stars": seller["stars_value"] if seller["stars_mode"] == "fixed" else 5,
                    "item_bought": seller["item_value"] if seller["item_mode"] == "fixed" else "",
                    "file_id": file_id,
                    "template_id": seller.get("inline_template_id") or seller["template_id"],
                    "ts": _time.time(),
                }
                if len(_PENDING_INLINE) > _PENDING_MAX:
                    for k in sorted(_PENDING_INLINE, key=lambda x: _PENDING_INLINE[x]["ts"])[:100]:
                        _PENDING_INLINE.pop(k, None)

            await _safe_answer(query,
                results=[
                    InlineQueryResultCachedPhoto(
                        id=r_id,
                        photo_file_id=file_id,
                        caption=caption,
                        parse_mode="HTML",
                        reply_markup=inline_kb,
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
                await _safe_answer(query,
                    results=[InlineQueryResultCachedPhoto(
                        id=result_id, photo_file_id=file_id,
                        caption=f"<i>«{_esc(review_text)}»</i>\n\n— {_esc(buyer_name)}",
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
                await _safe_answer(query,
                    results=[InlineQueryResultCachedPhoto(
                        id=result_id, photo_file_id=file_id,
                        caption=f"<i>«{_esc(review_text)}»</i>\n\n— {_esc(buyer_name)}",
                        parse_mode="HTML", title="⭐️ Отправить карточку отзыва",
                        description=review_text[:100],
                    )],
                    cache_time=30,
                )
                return

    # ── СЛУЧАЙ 5: совсем пусто или нет CACHE → подсказка ───────────────────
    await _safe_answer(query,
        results=[],
        switch_pm_text="✏️ Открыть бота чтобы оставить отзыв",
        switch_pm_parameter="inline_help",
        cache_time=1,
    )


@router.chosen_inline_result()
async def on_chosen_inline_result(chosen, bot, db: Database, config):
    """Срабатывает когда покупатель реально отправил инлайн-карточку.
    Если продавец включил уведомления — шлём ему карточку в ЛС с кнопками."""
    result_id = chosen.result_id
    if not result_id.startswith("notify:"):
        return
    pending = _PENDING_INLINE.pop(result_id, None)
    if not pending:
        return

    seller_id = pending["seller_id"]
    buyer_name = pending["buyer_name"]
    buyer_username = pending.get("buyer_username")
    buyer_id = pending["buyer_id"]
    review_text = pending["review_text"]
    stars = pending["stars"]
    file_id = pending["file_id"]

    # Анти-накрутка: те же лимиты, что и в основном флоу (админ — без лимитов)
    if buyer_id != config.ADMIN_TG_ID:
        if await db.has_recent_review(buyer_id, seller_id, hours=6):
            logger.info(f"Инлайн-отзыв {buyer_id}->{seller_id} отклонён: дедуп 6ч")
            return
        if await db.count_reviews_24h(buyer_id) >= 10:
            logger.info(f"Инлайн-отзыв {buyer_id} отклонён: суточный лимит")
            return

    # Сохраняем отзыв в БД — чтобы работали принять/отклонить
    try:
        seller = await db.get_seller(seller_id)
        show_btn = seller.get("inline_button_show", True) if seller else True
        review_row = await db.save_review(
            seller_id=seller_id,
            buyer_id=buyer_id,
            buyer_name=buyer_name,
            buyer_username=buyer_username,
            review_text=review_text,
            item_bought=pending.get("item_bought", ""),
            stars=stars,
            template_used=pending.get("template_id", "classic_gold"),
            card_file_id=file_id,
            show_buyer_button=show_btn,
        )
        review_id = review_row["id"]
    except Exception as e:
        logger.error(f"Не удалось сохранить инлайн-отзыв: {e}")
        return

    stars_line = "★" * stars if stars > 0 else ""
    caption_parts = [f"<b>{_esc(seller['shop_name'] if seller else '')}</b>"]
    if stars_line:
        caption_parts.append(stars_line)
    caption_parts.append(f"\n<i>«{_esc(review_text)}»</i>")
    caption_parts.append(f"\n— {_esc(buyer_name)}")
    caption = "\n".join(caption_parts)

    buyer_url = f"https://t.me/{buyer_username}" if buyer_username else f"tg://user?id={buyer_id}"

    ch = await db.get_seller_channel(seller_id)
    channel_verified = ch and ch["verified"]

    rows = []
    if show_btn:
        rows.append([InlineKeyboardButton(text=f"👤 {buyer_name}", url=buyer_url)])
    if channel_verified:
        rows.append([
            InlineKeyboardButton(text="✅ Принять", callback_data=f"review:accept:{review_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{review_id}"),
        ])
    seller_kb = InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    try:
        await bot.send_photo(
            chat_id=seller_id,
            photo=file_id,
            caption=f"⭐ Новый отзыв (через чат)!\n\n{caption}",
            reply_markup=seller_kb,
            parse_mode="HTML",
        )
        logger.info(f"Инлайн-отзыв отправлен продавцу {seller_id}")
    except Exception as e:
        logger.error(f"Ошибка отправки инлайн-отзыва продавцу {seller_id}: {e}")
