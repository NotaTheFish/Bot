from __future__ import annotations

import asyncio
import logging
import random

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

logger = logging.getLogger(__name__)


async def forward_post(
    client: TelegramClient,
    source_chat_id: int,
    source_message_ids: list[int],
    target_chat_id: int,
    min_delay: int,
    max_delay: int,
) -> int:
    await asyncio.sleep(random.randint(min_delay, max_delay))

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
        except RPCError:
            raise