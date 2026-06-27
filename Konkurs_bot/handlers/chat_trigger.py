import re
import logging
from aiogram import Router, Bot
from aiogram.types import Message

from database import db
from keyboards.kb import channels_kb

logger = logging.getLogger(__name__)
router = Router()

# Matches !anykey! at the start or anywhere in a message
TRIGGER_RE = re.compile(r'!([a-zA-Zа-яА-ЯёЁ0-9_-]+)!', re.IGNORECASE)


@router.message()
async def chat_key_trigger(message: Message, bot: Bot):
    """Handles !key! trigger in group chats."""
    if message.chat.type == "private":
        return

    text = message.text or message.caption or ""
    match = TRIGGER_RE.search(text)
    if not match:
        return

    key = match.group(1).lower()
    g = await db.get_giveaway_by_key(key)
    if not g:
        return

    channels = await db.get_channels(g['id'])

    # Fix missing chat_title — fetch from Telegram if stored as ID
    fixed_channels = []
    for ch in channels:
        title = ch.get('chat_title', '')
        if not title or title.lstrip('-').isdigit():
            try:
                chat_info = await bot.get_chat(ch['chat_id'])
                title = chat_info.title or str(ch['chat_id'])
                await db.update_channel_title(ch['id'], title)
            except Exception:
                title = str(ch['chat_id'])
        fixed_channels.append({**ch, 'chat_title': title})

    kb = channels_kb(fixed_channels, g['id'])

    try:
        await message.answer(g['announcement'], reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to send giveaway announcement: {e}")
