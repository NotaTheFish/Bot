import asyncio
from aiogram import Router, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InputMediaPhoto, InputMediaVideo
from redis.asyncio import Redis

from config import ADMIN_IDS, FLOOD_LIMIT_SECONDS
from states import UserSuggest
from db.queries import (
    create_submission, add_content, get_submission,
    get_submission_content, save_admin_msg, save_source_messages
)
from keyboards.user_kb import main_menu_kb, confirm_kb
from keyboards.admin_kb import submission_admin_kb
from utils.helpers import user_mention, build_media_input, submission_header
from utils.media_group import add_to_media_group
from utils.logger import log

router = Router()

REDIS_FLOOD_KEY = "sgb:flood:{user_id}"
# Временное хранилище контента до создания submission (ключ: user_id)
PENDING_CONTENT_KEY = "sgb:pending:{user_id}"


# ─── Шаг 1: кнопка "Предложить" ──────────────────────────────────────────────

@router.message(F.text == "📰 Предложить новость или работу")
async def suggest_start(message: Message, state: FSMContext, redis: Redis) -> None:
    user_id = message.from_user.id

    # Проверка флуда
    key = REDIS_FLOOD_KEY.format(user_id=user_id)
    ttl = await redis.ttl(key)
    if ttl > 0:
        return  # обрабатывается middleware — здесь просто выходим

    await state.set_state(UserSuggest.waiting_content)
    await state.update_data(items=[], media_group_ids=set())
    await message.answer(
        "📝 Опишите что хотите предложить.\n\n"
        "Можно отправить текст, фото, видео — или всё вместе.\n"
        "Когда закончите — нажмите <b>Готово</b> или отправьте всё сразу.",
        parse_mode="HTML",
    )


# ─── Шаг 2: принимаем контент ────────────────────────────────────────────────

async def _handle_content(message: Message, state: FSMContext, redis: Redis, item: dict) -> None:
    """Общая логика добавления одного элемента контента в стейт."""
    data = await state.get_data()
    items: list = data.get("items", [])

    if not items and not item.get("media_group_id"):
        # Одиночный элемент — сразу показываем превью
        items.append(item)
        await state.update_data(items=items)
        await _show_preview(message, items)
        await state.set_state(UserSuggest.confirming)
        return

    items.append(item)
    await state.update_data(items=items)

    if item.get("media_group_id"):
        # Медиагруппа — ждём буфер
        mg_id = item["media_group_id"]
        result = await add_to_media_group(redis, mg_id, item)
        if result is None:
            return  # ещё не все фото пришли
        # Финализируем — обновляем items из буфера
        await state.update_data(items=result)
        await _show_preview(message, result)
        await state.set_state(UserSuggest.confirming)


async def _show_preview(message: Message, items: list) -> None:
    """Показывает превью контента и кнопки подтверждения."""
    photos = [i for i in items if i["type"] in ("photo", "video")]
    text_item = next((i for i in items if i["type"] == "text"), None)
    caption = text_item["text"] if text_item else (photos[0].get("caption") if photos else None)

    await message.answer("👀 <b>Предпросмотр вашей заявки:</b>", parse_mode="HTML")

    if photos:
        if len(photos) == 1:
            if photos[0]["type"] == "photo":
                await message.answer_photo(
                    photo=photos[0]["file_id"],
                    caption=caption,
                    parse_mode="HTML",
                )
            else:
                await message.answer_video(
                    video=photos[0]["file_id"],
                    caption=caption,
                    parse_mode="HTML",
                )
        else:
            media = []
            for i, p in enumerate(photos):
                cap = caption if i == 0 else None
                if p["type"] == "photo":
                    media.append(InputMediaPhoto(media=p["file_id"], caption=cap, parse_mode="HTML"))
                else:
                    media.append(InputMediaVideo(media=p["file_id"], caption=cap, parse_mode="HTML"))
            await message.answer_media_group(media)
    elif caption:
        await message.answer(caption, parse_mode="HTML")

    await message.answer(
        "Всё верно? Отправляем?",
        reply_markup=confirm_kb(),
    )


@router.message(UserSuggest.waiting_content, F.photo)
async def handle_photo(message: Message, state: FSMContext, redis: Redis) -> None:
    item = {
        "type": "photo",
        "file_id": message.photo[-1].file_id,
        "caption": message.caption,
        "media_group_id": message.media_group_id,
        "sort_order": message.message_id,
        "message_id": message.message_id,
        "chat_id": message.chat.id,
    }
    await _handle_content(message, state, redis, item)


@router.message(UserSuggest.waiting_content, F.video)
async def handle_video(message: Message, state: FSMContext, redis: Redis) -> None:
    item = {
        "type": "video",
        "file_id": message.video.file_id,
        "caption": message.caption,
        "media_group_id": message.media_group_id,
        "sort_order": message.message_id,
        "message_id": message.message_id,
        "chat_id": message.chat.id,
    }
    await _handle_content(message, state, redis, item)


@router.message(UserSuggest.waiting_content, F.text)
async def handle_text(message: Message, state: FSMContext, redis: Redis) -> None:
    item = {
        "type": "text",
        "text": message.text,
        "media_group_id": None,
        "sort_order": 0,
        "message_id": message.message_id,
        "chat_id": message.chat.id,
    }
    await _handle_content(message, state, redis, item)


# ─── Шаг 3: подтверждение ────────────────────────────────────────────────────

@router.callback_query(UserSuggest.confirming, F.data == "user:confirm")
async def confirm_submission(callback: CallbackQuery, state: FSMContext, bot: Bot, redis: Redis) -> None:
    await callback.answer()
    data = await state.get_data()
    items: list = data.get("items", [])

    user = callback.from_user
    user_id = user.id

    # Создаём submission в БД
    sub_id = await create_submission(user_id)

    source_chat_id = None
    source_message_ids = []
    for i, item in enumerate(items):
        if item["type"] == "text":
            await add_content(sub_id, "text", None, item["text"], i)
        elif item["type"] in ("photo", "video"):
            await add_content(sub_id, item["type"], item["file_id"], item.get("caption"), i)
        if item.get("chat_id"):
            source_chat_id = item["chat_id"]
        if item.get("message_id"):
            source_message_ids.append(item["message_id"])

    if source_chat_id and source_message_ids:
        await save_source_messages(sub_id, source_chat_id, source_message_ids)

    # Ставим флуд-блок
    flood_key = REDIS_FLOOD_KEY.format(user_id=user_id)
    await redis.set(flood_key, "1", ex=FLOOD_LIMIT_SECONDS)

    await state.clear()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "✅ <b>Спасибо за предложку!</b>\nМы рассмотрим её в ближайшее время.",
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )

    # Рассылаем всем админам
    username = user.username
    first_name = user.first_name
    header = submission_header(sub_id, user_id, username, first_name)

    for admin_id in ADMIN_IDS:
        try:
            sent = await _send_submission_to_admin(bot, admin_id, sub_id, items, header, user_id)
            if sent:
                await save_admin_msg(sub_id, sent.message_id, admin_id)
        except Exception:
            pass

    await log(bot, f"📨 Новая заявка <b>#{sub_id}</b> от {user_mention(user_id, username, first_name)}")


async def _send_submission_to_admin(
    bot: Bot, admin_id: int, sub_id: int, items: list, header: str, user_id: int
) -> Message | None:
    """Отправляет заявку одному админу, возвращает сообщение с кнопками."""
    photos = [i for i in items if i["type"] in ("photo", "video")]
    text_item = next((i for i in items if i["type"] == "text"), None)
    caption = text_item["text"] if text_item else (photos[0].get("caption") if photos else "")

    kb = submission_admin_kb(sub_id, user_id)
    full_caption = header + (caption or "")

    if photos:
        if len(photos) == 1:
            if photos[0]["type"] == "photo":
                await bot.send_photo(admin_id, photos[0]["file_id"], caption=full_caption,
                                     parse_mode="HTML")
            else:
                await bot.send_video(admin_id, photos[0]["file_id"], caption=full_caption,
                                     parse_mode="HTML")
        else:
            media = []
            for i, p in enumerate(photos):
                cap = full_caption if i == 0 else None
                if p["type"] == "photo":
                    media.append(InputMediaPhoto(media=p["file_id"], caption=cap, parse_mode="HTML"))
                else:
                    media.append(InputMediaVideo(media=p["file_id"], caption=cap, parse_mode="HTML"))
            await bot.send_media_group(admin_id, media)

        # Отправляем кнопки отдельным сообщением после медиагруппы
        sent = await bot.send_message(
            admin_id,
            f"⬆️ Заявка <b>#{sub_id}</b> — выберите действие:",
            reply_markup=kb,
            parse_mode="HTML",
        )
        return sent
    else:
        sent = await bot.send_message(
            admin_id,
            full_caption,
            reply_markup=kb,
            parse_mode="HTML",
        )
        return sent


@router.callback_query(UserSuggest.confirming, F.data == "user:cancel")
async def cancel_submission(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.clear()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "❌ Отменено. Можешь начать заново в любой момент.",
        reply_markup=main_menu_kb(),
    )
