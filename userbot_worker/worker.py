import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Message

from .config import Settings
from .db import (
    get_pending_tasks,
    mark_task_done,
    mark_task_error,
    update_task_progress,
)
from .sender import send_post_to_chat


logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _chunked(seq: List[int], n: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


async def _resolve_targets_from_dialogs(
    client: TelegramClient,
    excluded_ids: Set[int],
) -> List[int]:
    """
    Собираем target_chat_ids автоматически из диалогов Telethon:
    - только группы/каналы
    - исключаем storage и blacklist
    """
    targets: List[int] = []

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        chat_id = dialog.id  # у Telethon тут уже правильный id (может быть отрицательным)
        if chat_id in excluded_ids:
            continue

        # Берём группы/каналы (лички и ботов не трогаем)
        is_group_or_channel = getattr(entity, "megagroup", False) or getattr(entity, "broadcast", False) or dialog.is_group or dialog.is_channel
        if not is_group_or_channel:
            continue

        # Telethon: диалог может быть “read-only”, но точной проверки прав тут нет без отдельного запроса.
        # Оставляем как было: пробуем отправить — если нельзя, поймаем ошибку на send.
        targets.append(chat_id)

    return targets


async def run_worker(settings: Settings) -> None:
    """
    Основной цикл:
    - берём pending задачи из БД
    - определяем target чаты
    - рассылаем с рандомной задержкой
    - пишем прогресс/ошибки в БД
    """
    logger.info("Worker started. poll=%ss", settings.worker_poll_seconds)

    async with TelegramClient(
        settings.telethon_session,
        settings.telegram_api_id,
        settings.telegram_api_hash,
    ) as client:
        while True:
            try:
                tasks = await get_pending_tasks(settings.database_url, now=_now_utc())
            except Exception:
                logger.exception("DB: failed to fetch pending tasks")
                await asyncio.sleep(settings.worker_poll_seconds)
                continue

            if not tasks:
                await asyncio.sleep(settings.worker_poll_seconds)
                continue

            for task in tasks:
                # 1) Определяем куда слать
                try:
                    if task.target_chat_ids and len(task.target_chat_ids) > 0:
                        target_ids = list(task.target_chat_ids)
                    else:
                        excluded: Set[int] = set()
                        excluded.add(int(settings.storage_chat_id))
                        excluded |= set(getattr(settings, "blacklist_chat_ids", set()) or set())

                        target_ids = await _resolve_targets_from_dialogs(client, excluded)

                    # Нечего делать
                    if not target_ids:
                        await mark_task_error(
                            settings.database_url,
                            task_id=task.id,
                            error="No target chats resolved (empty TARGET_CHAT_IDS and no dialogs matched).",
                        )
                        continue

                except Exception:
                    logger.exception("Task %s: failed to resolve targets", task.id)
                    await mark_task_error(
                        settings.database_url,
                        task_id=task.id,
                        error="Failed to resolve target chats.",
                    )
                    continue

                # 2) Рассылка
                sent = 0
                errors = 0
                last_error: Optional[str] = None

                for chat_id in target_ids:
                    # защита от “слишком частых” отправок
                    delay = random.randint(settings.min_seconds_between_chats, settings.max_seconds_between_chats)
                    await asyncio.sleep(delay)

                    try:
                        await send_post_to_chat(
                            client=client,
                            database_url=settings.database_url,
                            task=task,
                            chat_id=int(chat_id),
                        )
                        sent += 1
                        await update_task_progress(
                            settings.database_url,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=None,
                        )

                    except FloodWaitError as e:
                        # FloodWait — надо ждать e.seconds
                        wait_s = int(getattr(e, "seconds", 0) or 0)
                        last_error = f"FloodWait {wait_s}s"
                        errors += 1
                        logger.warning("Task %s: FloodWait for %ss", task.id, wait_s)
                        await update_task_progress(
                            settings.database_url,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=last_error,
                        )
                        await asyncio.sleep(max(wait_s, 1))

                    except RPCError as e:
                        # Например: чат апгрейднут в супергруппу, нет прав писать, etc.
                        last_error = str(e)
                        errors += 1
                        logger.warning("Task %s: Failed to send to chat %s: %s", task.id, chat_id, last_error)
                        await update_task_progress(
                            settings.database_url,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=last_error,
                        )
                        continue

                    except Exception as e:
                        last_error = repr(e)
                        errors += 1
                        logger.exception("Task %s: unexpected error for chat %s", task.id, chat_id)
                        await update_task_progress(
                            settings.database_url,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=last_error,
                        )
                        continue

                # 3) Финализация задачи
                try:
                    # если были ошибки — всё равно завершаем как done (как и раньше),
                    # либо можно менять на partial — это уже на твоё усмотрение в tasks/db логике
                    await mark_task_done(
                        settings.database_url,
                        task_id=task.id,
                        sent_count=sent,
                        error_count=errors,
                        last_error=last_error,
                    )
                    logger.info("Task %s done. sent=%s errors=%s", task.id, sent, errors)
                except Exception:
                    logger.exception("Task %s: failed to mark done", task.id)
                    await mark_task_error(
                        settings.database_url,
                        task_id=task.id,
                        error="Failed to finalize task (mark done).",
                    )

            await asyncio.sleep(settings.worker_poll_seconds)
