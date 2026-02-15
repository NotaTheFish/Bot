import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

from .config import Settings
from .db import (
    claim_pending_tasks,
    ensure_schema,
    mark_task_done,
    mark_task_error,
    update_task_progress,
)
from .sender import extract_migrated_chat_id, send_post_to_chat
from .tasks import remap_chat_id_everywhere

logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


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
        try:
            object.__setattr__(settings, "storage_chat_id", int(new_chat_id))
        except Exception:
            logger.debug("Failed to update settings.storage_chat_id", exc_info=True)
        task.storage_chat_id = int(new_chat_id)


async def run_worker(settings: Settings, pool, client: TelegramClient, stop_event: asyncio.Event) -> None:
    logger.info("Worker started. poll=%ss", settings.worker_poll_seconds)
    await ensure_schema(pool)

    while not stop_event.is_set():
        try:
            tasks = await claim_pending_tasks(pool, limit=25, now=_now_utc())
        except Exception:
            logger.exception("DB: failed to claim pending tasks")
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        if not tasks:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        for task in tasks:
            target_ids = list(task.target_chat_ids or settings.target_chat_ids)
            if not target_ids:
                await mark_task_error(
                    pool,
                    task_id=task.id,
                    attempts=task.attempts,
                    max_attempts=settings.max_task_attempts,
                    error="Task has empty target_chat_ids and worker TARGET_CHAT_IDS is not configured.",
                    sent_count=task.sent_count,
                    error_count=task.error_count,
                )
                continue

            sent = 0
            errors = 0
            last_error: Optional[str] = None

            for chat_id in target_ids:
                current_chat_id = int(chat_id)
                retried_after_migration = False
                while True:
                    try:
                        await send_post_to_chat(
                            client=client,
                            source_chat_id=int(task.storage_chat_id),
                            source_message_ids=task.storage_message_ids,
                            target_chat_id=current_chat_id,
                            min_delay=settings.min_seconds_between_chats,
                            max_delay=settings.max_seconds_between_chats,
                        )
                        sent += 1
                        await update_task_progress(
                            pool,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=None,
                        )
                        break
                    except FloodWaitError as e:
                        wait_s = int(getattr(e, "seconds", 0) or 0)
                        await asyncio.sleep(max(wait_s, 1))
                        continue
                    except RPCError as e:
                        migrated_chat_id = extract_migrated_chat_id(e)
                        if retried_after_migration or migrated_chat_id is None:
                            last_error = str(e)
                            errors += 1
                            await update_task_progress(
                                pool,
                                task_id=task.id,
                                sent_count=sent,
                                error_count=errors,
                                last_error=last_error,
                            )
                            break

                        error_text = str(e)
                        is_storage_migration = str(task.storage_chat_id) in error_text and str(current_chat_id) not in error_text
                        old_chat_id = int(task.storage_chat_id if is_storage_migration else current_chat_id)

                        await _apply_chat_migration(
                            settings=settings,
                            pool=pool,
                            task=task,
                            old_chat_id=old_chat_id,
                            new_chat_id=int(migrated_chat_id),
                            is_storage_chat=is_storage_migration,
                        )

                        if not is_storage_migration:
                            current_chat_id = int(migrated_chat_id)
                        retried_after_migration = True
                        continue
                    except Exception as e:
                        last_error = repr(e)
                        errors += 1
                        await update_task_progress(
                            pool,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=last_error,
                        )
                        break

            if errors == 0:
                await mark_task_done(
                    pool,
                    task_id=task.id,
                    sent_count=sent,
                    error_count=errors,
                    last_error=None,
                )
                logger.info("Task %s done. sent=%s", task.id, sent)
                continue

            err_text = last_error or f"Broadcast completed with {errors} errors."
            await mark_task_error(
                pool,
                task_id=task.id,
                attempts=task.attempts,
                max_attempts=settings.max_task_attempts,
                error=err_text,
                sent_count=sent,
                error_count=errors,
            )
            logger.warning(
                "Task %s finished with errors: sent=%s errors=%s attempts=%s/%s",
                task.id,
                sent,
                errors,
                task.attempts,
                settings.max_task_attempts,
            )

        await asyncio.sleep(settings.worker_poll_seconds)
