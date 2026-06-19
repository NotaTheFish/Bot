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
    # Only work in groups/supergroups/channels, not private
    if message.chat.type == "private":
        return

    text = message.text or message.caption or ""
    match = TRIGGER_RE.search(text)
    if not match:
        return

    key = match.group(1).lower()
    g = await db.get_giveaway_by_key(key)
    if not g:
        return  # Key not found — silently ignore

    channels = await db.get_channels(g['id'])
    prizes = await db.get_prizes(g['id'])

    def prize_icon(n):
        icons = {1: "🥇", 2: "🥈", 3: "🥉"}
        return icons.get(n, f"#{n}")

    prizes_text = "\n".join(
        f"{prize_icon(p['place'])} {p['place']} место — {p['description']}"
        for p in prizes
    )

    announcement_text = (
        f"{g['announcement']}\n\n"
        f"🏆 <b>Призы:</b>\n{prizes_text}"
    )

    kb = channels_kb(channels, g['id'])

    try:
        await message.answer(announcement_text, reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to send giveaway announcement: {e}")
