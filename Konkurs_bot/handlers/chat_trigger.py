import re
import logging
from aiogram import Router, Bot
from aiogram.types import Message

from database import db
from keyboards.kb import channels_kb
from services.giveaway_service import prepare_channels, send_html_smart

logger = logging.getLogger(__name__)
router = Router()

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

    channels = await prepare_channels(bot, g['id'])
    kb = channels_kb(channels, g['id'])

    try:
        sent = await send_html_smart(bot, message.chat.id, g['announcement'], reply_markup=kb)
        await db.add_published_message(g['id'], sent.chat.id, sent.message_id)
    except Exception as e:
        logger.error(f"Failed to send giveaway announcement: {e}")
