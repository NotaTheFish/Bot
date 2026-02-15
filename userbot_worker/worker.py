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
from .sender import extract_migrated_chat_id, send_post_to_chat
from .tasks import remap_chat_id_everywhere


logger = logging.getLogger(__name__)


async def _apply_chat_migration(
    *,
    settings: Settings,
    pool,
    task,
    old_chat_id: int,
    new_chat_id: int,
    is_storage_chat: bool,
) -> None:
    await remap_chat_id_everywhere(
        pool,
        old_chat_id=old_chat_id,
        new_chat_id=new_chat_id,
        is_storage_chat=is_storage_chat,
    )

    if is_storage_chat:
        task.storage_chat_id = new_chat_id
        try:
            object.__setattr__(settings, "storage_chat_id", new_chat_id)
        except Exception:
            logger.debug("Failed to update settings.storage_chat_id in-memory", exc_info=True)
    elif task.target_chat_ids is not None:
        task.target_chat_ids = [new_chat_id if value == old_chat_id else value for value in task.target_chat_ids]


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


async def run_worker(settings: Settings, pool, client: TelegramClient, stop_event: asyncio.Event) -> None:
    """
    Основной цикл:
    - берём pending задачи из БД
    - определяем target чаты
    - рассылаем с рандомной задержкой
    - пишем прогресс/ошибки в БД
    """
    logger.info("Worker started. poll=%ss", settings.worker_poll_seconds)

    while not stop_event.is_set():
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
                delay = random.randint(settings.min_seconds_between_chats, settings.max_seconds_between_chats)
                await asyncio.sleep(delay)

                current_chat_id = int(chat_id)
                retried_after_migration = False

                try:
                    while True:
                        try:
                            await send_post_to_chat(
                                client=client,
                                database_url=settings.database_url,
                                task=task,
                                chat_id=current_chat_id,
                            )
                            sent += 1
                            await update_task_progress(
                                settings.database_url,
                                task_id=task.id,
                                sent_count=sent,
                                error_count=errors,
                                last_error=None,
                            )
                            break
                        except RPCError as e:
                            migrated_chat_id = extract_migrated_chat_id(e)
                            if retried_after_migration or migrated_chat_id is None:
                                raise

                            error_text = str(e)
                            storage_id_text = str(task.storage_chat_id)
                            current_id_text = str(current_chat_id)
                            is_storage_migration = storage_id_text in error_text and current_id_text not in error_text
                            old_chat_id = task.storage_chat_id if is_storage_migration else current_chat_id

                            await _apply_chat_migration(
                                settings=settings,
                                pool=pool,
                                task=task,
                                old_chat_id=int(old_chat_id),
                                new_chat_id=int(migrated_chat_id),
                                is_storage_chat=is_storage_migration,
                            )

                            if is_storage_migration:
                                logger.warning(
                                    "Task %s: storage chat migrated %s -> %s. Retrying once for target %s",
                                    task.id,
                                    old_chat_id,
                                    migrated_chat_id,
                                    current_chat_id,
                                )
                            else:
                                logger.warning(
                                    "Task %s: target chat migrated %s -> %s. Retrying once.",
                                    task.id,
                                    current_chat_id,
                                    migrated_chat_id,
                                )
                                current_chat_id = int(migrated_chat_id)

                            retried_after_migration = True

                except FloodWaitError as e:
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
