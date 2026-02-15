from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

logger = logging.getLogger(__name__)

_MIGRATED_SUPERGROUP_RE = re.compile(r"migrated to supergroup id\s*(-?\d+)", re.IGNORECASE)


def _normalize_supergroup_chat_id(raw_chat_id: Any) -> int | None:
    try:
        value = int(raw_chat_id)
    except (TypeError, ValueError):
        return None

    # Для супергрупп/каналов в нашем коде используем Bot API стиль: -100...
    if value > 0:
        return int(f"-100{value}")
    return value


def extract_migrated_chat_id(exc: BaseException) -> int | None:
    """Пытается вытащить новый chat_id из RPC-ошибки миграции чата."""
    # 1) Популярные атрибуты у исключений Telethon (если есть)
    for attr_name in ("new_chat_id", "migrate_to", "chat_id", "value"):
        if not hasattr(exc, attr_name):
            continue
        normalized = _normalize_supergroup_chat_id(getattr(exc, attr_name))
        if normalized is not None and str(normalized).startswith("-100"):
            return normalized

    # 2) Fallback: разбираем текст ошибки
    text = str(exc)
    match = _MIGRATED_SUPERGROUP_RE.search(text)
    if not match:
        return None
    return _normalize_supergroup_chat_id(match.group(1))


async def forward_post(
    client: TelegramClient,
    source_chat_id: int,
    source_message_ids: list[int],
    target_chat_id: int,
    min_delay: int,
    max_delay: int,
) -> int:
    await asyncio.sleep(random.randint(min_delay, max_delay))

    retry_after_migration = True

    while True:
        try:
            sent = await client.forward_messages(
                entity=target_chat_id,
                messages=source_message_ids,
                from_peer=source_chat_id,
            )
            if isinstance(sent, list):
                return int(sent[-1].id)
            return int(sent.id)
        except FloodWaitError as exc:
            logger.warning("FloodWaitError chat=%s wait=%s", target_chat_id, exc.seconds)
            await asyncio.sleep(exc.seconds)
        except RPCError as exc:
            migrated_chat_id = extract_migrated_chat_id(exc)
            if retry_after_migration and migrated_chat_id is not None and migrated_chat_id != target_chat_id:
                logger.warning(
                    "Target chat migrated %s -> %s. Retrying forward once.",
                    target_chat_id,
                    migrated_chat_id,
                )
                target_chat_id = migrated_chat_id
                retry_after_migration = False
                continue
            raise