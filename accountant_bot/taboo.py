from __future__ import annotations

import logging
from typing import Any

from .config import Settings

logger = logging.getLogger(__name__)

_taboo_chat_ids: set[int] = set()


def load_taboo(settings: Settings) -> set[int]:
    global _taboo_chat_ids
    _taboo_chat_ids = {int(chat_id) for chat_id in (settings.TABOO_CHAT_IDS or [])}
    return set(_taboo_chat_ids)


def is_taboo(chat_id: int) -> bool:
    return int(chat_id) in _taboo_chat_ids


async def safe_send_message(bot: Any, chat_id: int, text: str, **kwargs: Any) -> Any | None:
    if is_taboo(chat_id):
        logger.warning("Blocked send_message to taboo chat_id=%s", chat_id)
        return None
    return await bot.send_message(chat_id=chat_id, text=text, **kwargs)


async def safe_send_document(bot: Any, chat_id: int, document: Any, **kwargs: Any) -> Any | None:
    if is_taboo(chat_id):
        logger.warning("Blocked send_document to taboo chat_id=%s", chat_id)
        return None
    return await bot.send_document(chat_id=chat_id, document=document, **kwargs)