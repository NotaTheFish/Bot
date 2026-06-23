import secrets
import logging
from aiogram import Router, F, Bot
from aiogram.types import (
    CallbackQuery, Message, ChatMemberUpdated,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import ChatMemberUpdatedFilter, IS_NOT_MEMBER, ADMINISTRATOR
from aiogram.fsm.context import FSMContext

from db import Database

logger = logging.getLogger(__name__)
router = Router()


def _gen_key() -> str:
    return "!" + secrets.token_hex(4)


def _add_kb(bot_username: str, seller_id: int) -> InlineKeyboardMarkup:
    """Кнопка-диплинк для добавления бота в канал с нужными правами."""
    deep = f"addchannel_{seller_id}"
    url = (
        f"https://t.me/{bot_username}?startchannel={deep}"
        f"&admin=post_messages+delete_messages"
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Добавить бота в канал", url=url)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="channel:cancel")],
    ])


# ── 1. Продавец нажал «Канал отзывов» ─────────────────────────────────────

@router.callback_query(F.data == "channel:manage")
async def cb_channel_manage(call: CallbackQuery, db: Database, config):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    await call.answer()

    ch = await db.get_seller_channel(call.from_user.id)

    if ch and ch["verified"]:
        await call.message.answer(
            f"📢 <b>Канал отзывов подключён</b>\n\n"
            f"Канал: <b>{ch['channel_title']}</b>\n\n"
            f"Хочешь отключить канал?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗑 Отключить канал", callback_data="channel:remove")],
                [InlineKeyboardButton(text="« Назад", callback_data="menu:back")],
            ])
        )
        return

    await call.message.answer(
        "📢 <b>Подключение канала отзывов</b>\n\n"
        "Нажми кнопку ниже — Telegram предложит выбрать канал и выдать боту права администратора.\n\n"
        "⚠️ Ты должен быть <b>владельцем</b> этого канала.",
        reply_markup=_add_kb(config.BOT_USERNAME, call.from_user.id)
    )


# ── 2. Бот добавлен в канал (my_chat_member апдейт) ───────────────────────

@router.my_chat_member(
    ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER >> ADMINISTRATOR)
)
async def on_bot_added_to_channel(event: ChatMemberUpdated, bot: Bot, db: Database, config):
    logger.info(f"my_chat_member сработал: chat.type={event.chat.type}, chat.id={event.chat.id}")

    if event.chat.type != "channel":
        logger.info(f"Пропуск: не канал (type={event.chat.type})")
        return

    channel_id = event.chat.id
    channel_title = event.chat.title or "Без названия"
    logger.info(f"Бот добавлен в канал: {channel_id} ({channel_title})")

    try:
        admins = await bot.get_chat_administrators(channel_id)
        logger.info(f"Админы канала: {[(a.user.id, a.status) for a in admins]}")
    except Exception as e:
        logger.warning(f"Не удалось получить админов канала {channel_id}: {e}")
        return

    creator = next((a for a in admins if a.status == "creator"), None)
    logger.info(f"Creator: {creator.user.id if creator else None}")
    if not creator:
        logger.info(f"Канал {channel_id}: создатель не найден среди админов")
        return

    seller_id = creator.user.id
    seller = await db.get_seller(seller_id)
    logger.info(f"Seller найден: {seller is not None}, seller_id={seller_id}")
    if not seller:
        logger.info(f"Канал {channel_id}: создатель {seller_id} не является продавцом бота")
        return

    key = _gen_key()
    await db.set_seller_channel(seller_id, channel_id, channel_title, key)
    logger.info(f"Канал сохранён, ключ={key}, отправляю продавцу {seller_id}")

    try:
        await bot.send_message(
            seller_id,
            f"✅ Бот добавлен в канал <b>{channel_title}</b>!\n\n"
            f"Для подтверждения владения каналом напиши в нём следующее сообщение "
            f"от своего аккаунта (ты должен быть <b>владельцем</b>):\n\n"
            f"<code>{key}</code>\n\n"
            f"⚠️ Бот автоматически удалит это сообщение после проверки."
        )
        logger.info(f"Ключ отправлен продавцу {seller_id}")
    except Exception as e:
        logger.warning(f"Не удалось написать продавцу {seller_id}: {e}")


# ── 3. Ключ написан в канале ───────────────────────────────────────────────

@router.channel_post()
async def on_channel_post(message: Message, bot: Bot, db: Database):
    text = (message.text or "").strip()
    if not text.startswith("!"):
        return

    ch = await db.get_pending_channel_by_key(text)
    if not ch:
        return

    channel_id = message.chat.id
    seller_id = ch["seller_id"]

    # Проверяем что автор поста — владелец канала
    # В канале message.from_user = None (анонимный пост от канала).
    # Единственный способ убедиться что ключ написал именно продавец —
    # проверить что seller_id является creator этого канала.
    try:
        member = await bot.get_chat_member(channel_id, seller_id)
    except Exception as e:
        logger.warning(f"Не удалось проверить статус {seller_id} в {channel_id}: {e}")
        return

    if member.status != "creator":
        # Кто-то чужой написал ключ — игнорируем, не удаляем
        return

    # Удаляем ключ из канала
    try:
        await bot.delete_message(channel_id, message.message_id)
    except Exception:
        pass

    # Верифицируем
    await db.verify_seller_channel(channel_id)

    try:
        await bot.send_message(
            seller_id,
            f"🎉 <b>Канал успешно подключён!</b>\n\n"
            f"Теперь при каждом новом отзыве ты сможешь публиковать его в <b>{message.chat.title}</b> "
            f"одним нажатием кнопки «✅ Принять»."
        )
    except Exception:
        pass


# ── 4. Отключить канал ────────────────────────────────────────────────────

@router.callback_query(F.data == "channel:remove")
async def cb_channel_remove(call: CallbackQuery, db: Database):
    await db.delete_seller_channel(call.from_user.id)
    await call.answer("Канал отключён", show_alert=True)
    await call.message.delete()


@router.callback_query(F.data == "channel:cancel")
async def cb_channel_cancel(call: CallbackQuery):
    await call.answer()
    await call.message.delete()
