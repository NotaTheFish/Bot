from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Any, Optional, Sequence, Union

from telethon import TelegramClient
from telethon.errors import FloodWaitError, MessageIdInvalidError, RPCError
from telethon.tl.types import TypeInputPeer

logger = logging.getLogger(__name__)


class SourceMessageMissingError(RuntimeError):
    """Storage source message(s) are missing or no longer accessible."""


# В логах/ошибках Telethon часто встречаются такие формулировки миграции
# Примеры:
# - "The group has been migrated to a supergroup with id -100123..."
# - "... migrated to supergroup id -100123..."
_MIGRATED_TO_SUPERGROUP_RE = re.compile(
    r"(?:migrated to (?:a )?supergroup(?: with id)?|migrated to supergroup id)\s*(-?\d+)",
    re.IGNORECASE,
)


def _normalize_supergroup_chat_id(raw_chat_id: Any) -> Optional[int]:
    """
    Приводим id к ожидаемому виду.
    Если пришёл положительный id, пробуем сделать bot-api стиль -100...
    (Иногда Telethon может отдавать/логировать id по-разному.)
    """
    try:
        value = int(raw_chat_id)
    except (TypeError, ValueError):
        return None

    # Если это "положительный" id (редко в исключениях), приводим к -100...
    if value > 0:
        return int(f"-100{value}")
    return value


def extract_migrated_chat_id(exc: BaseException) -> Optional[int]:
    """
    Пытается вытащить новый chat_id из RPC-ошибки миграции чата.
    Возвращает -100... если удалось, иначе None.
    """
    # 1) Иногда Telethon-исключения имеют полезные атрибуты
    for attr_name in ("new_chat_id", "migrate_to", "chat_id", "value"):
        if hasattr(exc, attr_name):
            normalized = _normalize_supergroup_chat_id(getattr(exc, attr_name))
            if normalized is not None and str(normalized).startswith("-100"):
                return normalized

    # 2) Фолбэк: парсим текст исключения
    text = str(exc) or ""
    m = _MIGRATED_TO_SUPERGROUP_RE.search(text)
    if not m:
        return None

    normalized = _normalize_supergroup_chat_id(m.group(1))
    if normalized is not None and str(normalized).startswith("-100"):
        return normalized
    return None


def _is_forward_restricted_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    markers = (
        "chatforwardsrestricted",
        "forwards restricted",
        "forward forbidden",
        "chat_send_media_forbidden",
        "chatsendmediaforbidden",
    )
    return any(marker in text for marker in markers)


async def _copy_messages_as_text(
    client: TelegramClient,
    *,
    source_chat_id: int,
    source_message_id: Union[int, Sequence[int]],
    target: Union[int, TypeInputPeer],
) -> int:
    message_ids = (
        [int(source_message_id)]
        if isinstance(source_message_id, int)
        else [int(message_id) for message_id in source_message_id]
    )
    source_messages = await client.get_messages(source_chat_id, ids=message_ids)
    if not source_messages:
        raise RuntimeError("Cannot copy message: source message not found")

    if not isinstance(source_messages, list):
        source_messages = [source_messages]

    last_sent = None
    for source_message in source_messages:
        text = getattr(source_message, "raw_text", None) or getattr(source_message, "message", None) or ""
        sent = await client.send_message(
            entity=target,
            message=text if text else " ",
            buttons=getattr(source_message, "buttons", None),
        )
        last_sent = sent

    if last_sent is None:
        raise RuntimeError("Cannot copy message: no messages were sent")
    return int(last_sent.id)


async def forward_post(
    client: TelegramClient,
    source_chat_id: int,
    source_message_id: Union[int, Sequence[int]],
    target: Union[int, TypeInputPeer],
    min_delay: int,
    max_delay: int,
) -> int:
    """
    ТВОЯ исходная логика сохранена:
    - рандомная задержка перед отправкой
    - ретраи при FloodWaitError
    - пересылка через forward_messages

    Возвращает id последнего отправленного сообщения (если список — последнего).
    """
    await asyncio.sleep(random.randint(int(min_delay), int(max_delay)))

    while True:
        try:
            sent = await client.forward_messages(
                entity=target,
                messages=source_message_id,  # int или list[int]
                from_peer=source_chat_id,
            )

            # Telethon может вернуть Message или list[Message]
            if isinstance(sent, list):
                if not sent:
                    raise RuntimeError("forward_messages returned empty list")
                return int(sent[-1].id)
            return int(sent.id)

        except FloodWaitError as exc:
            logger.warning("FloodWaitError target=%s wait=%s", target, exc.seconds)
            await asyncio.sleep(int(exc.seconds))

        except MessageIdInvalidError as exc:
            raise SourceMessageMissingError("source_message_missing") from exc

        except RPCError as exc:
            if _is_forward_restricted_error(exc):
                logger.warning(
                    "Forward restricted chat=%s source_chat=%s. Fallback to text copy",
                    target,
                    source_chat_id,
                )
                return await _copy_messages_as_text(
                    client,
                    source_chat_id=source_chat_id,
                    source_message_id=source_message_id,
                    target=target,
                )
            # Ничего не скрываем — пусть воркер решает, что делать дальше
            raise


async def send_post_to_chat(
    client: TelegramClient,
    *,
    source_chat_id: int,
    source_message_ids: Sequence[int],
    target: Union[int, TypeInputPeer],
    min_delay: int,
    max_delay: int,
) -> int:
    """
    Совместимость с новой версией worker.py:
    - ожидает список message_ids (например storage_message_ids)
    - внутри использует forward_post(), чтобы сохранить твою логику задержек/FloodWait
    """
    if not source_message_ids:
        raise ValueError("source_message_ids is empty")

    # forward_post уже поддерживает и int, и list[int]
    return await forward_post(
        client=client,
        source_chat_id=source_chat_id,
        source_message_id=list(source_message_ids) if len(source_message_ids) > 1 else int(source_message_ids[0]),
        target=target,
        min_delay=min_delay,
        max_delay=max_delay,
    )
