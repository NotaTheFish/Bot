import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import User

from .config import Settings
from .db import (
    claim_pending_tasks,
    disable_stale_userbot_targets,
    disable_userbot_target,
    ensure_schema,
    load_enabled_userbot_targets,
    mark_task_done,
    mark_task_error,
    remap_userbot_target_chat_id,
    update_task_progress,
    upsert_userbot_target,
)
from .sender import extract_migrated_chat_id, send_post_to_chat
from .tasks import remap_chat_id_everywhere

logger = logging.getLogger(__name__)


def _normalize_chat_id(raw_chat_id: int) -> int:
    value = int(raw_chat_id)
    if value > 0:
        return int(f"-100{value}")
    return value


def _chat_type_from_dialog(dialog) -> Optional[str]:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return None
    if getattr(dialog, "is_group", False):
        if bool(getattr(entity, "megagroup", False)):
            return "supergroup"
        return "group"
    if getattr(dialog, "is_channel", False):
        return "channel"
    return None


def _is_write_permission_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    markers = (
        "chatwriteforbidden",
        "chatadminrequired",
        "userbannedinchannel",
        "channelprivate",
        "forbidden",
        "not enough rights",
    )
    return any(marker in text for marker in markers)


async def sync_userbot_targets(pool, client: TelegramClient, settings: Settings) -> dict:
    total_seen = 0
    new_count = 0
    include_channels = settings.auto_targets_mode == "groups_and_channels"

    async for dialog in client.iter_dialogs():
        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue
        if isinstance(entity, User) or getattr(dialog, "is_user", False):
            continue

        if not getattr(dialog, "is_group", False):
            if not (include_channels and getattr(dialog, "is_channel", False)):
                continue

        chat_type = _chat_type_from_dialog(dialog)
        if chat_type not in {"group", "supergroup", "channel"}:
            continue

        chat_id = _normalize_chat_id(getattr(entity, "id", 0))
        if not chat_id:
            continue
        if int(chat_id) == int(settings.storage_chat_id):
            continue
        if int(chat_id) in settings.blacklist_chat_ids:
            continue

        inserted = await upsert_userbot_target(
            pool,
            chat_id=int(chat_id),
            chat_type=chat_type,
            title=getattr(entity, "title", None),
            username=getattr(entity, "username", None),
        )
        total_seen += 1
        if inserted:
            new_count += 1

    disabled_count = await disable_stale_userbot_targets(pool, grace_days=30)
    logger.info("Disabled stale targets: %s", disabled_count)
    enabled_count = len(await load_enabled_userbot_targets(pool))
    stats = {
        "total": total_seen,
        "enabled": enabled_count,
        "new_count": new_count,
        "disabled_count": disabled_count,
    }
    logger.info("Targets sync done: %s", stats)
    return stats


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
    await remap_userbot_target_chat_id(pool, old_chat_id=old_chat_id, new_chat_id=new_chat_id)

    if is_storage_chat:
        try:
            object.__setattr__(settings, "storage_chat_id", int(new_chat_id))
        except Exception:
            logger.debug("Failed to update settings.storage_chat_id", exc_info=True)
        task.storage_chat_id = int(new_chat_id)


async def run_worker(settings: Settings, pool, client: TelegramClient, stop_event: asyncio.Event) -> None:
    logger.info("Worker started. poll=%ss", settings.worker_poll_seconds)
    await ensure_schema(pool)

    try:
        await sync_userbot_targets(pool, client, settings)
    except Exception:
        logger.exception("Initial userbot targets sync failed")
    last_targets_sync_at = _now_utc()

    while not stop_event.is_set():
        now_utc = _now_utc()
        if (now_utc - last_targets_sync_at).total_seconds() >= settings.targets_sync_seconds:
            try:
                await sync_userbot_targets(pool, client, settings)
            except Exception:
                logger.exception("Periodic userbot targets sync failed")
            last_targets_sync_at = now_utc

        try:
            tasks = await claim_pending_tasks(pool, limit=25, now=now_utc)
        except Exception:
            logger.exception("DB: failed to claim pending tasks")
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        if not tasks:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        for task in tasks:
            if task.target_chat_ids:
                target_ids = list(task.target_chat_ids)
            elif settings.target_chat_ids:
                target_ids = list(settings.target_chat_ids)
            else:
                target_ids = await load_enabled_userbot_targets(pool)

            filtered_target_ids: list[int] = []
            seen_target_ids: set[int] = set()
            for target_id in target_ids:
                normalized_target_id = int(target_id)
                if normalized_target_id == int(task.storage_chat_id):
                    continue
                if normalized_target_id in settings.blacklist_chat_ids:
                    continue
                if normalized_target_id in seen_target_ids:
                    continue
                seen_target_ids.add(normalized_target_id)
                filtered_target_ids.append(normalized_target_id)
            target_ids = filtered_target_ids

            if not target_ids:
                await mark_task_error(
                    pool,
                    task_id=task.id,
                    attempts=task.attempts,
                    max_attempts=settings.max_task_attempts,
                    error="No targets: join group chats or enable AUTO_TARGETS_MODE, or set TARGET_CHAT_IDS.",
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
                            if _is_write_permission_error(e):
                                await disable_userbot_target(
                                    pool,
                                    chat_id=current_chat_id,
                                    error_text=last_error,
                                )
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
                        if _is_write_permission_error(e):
                            await disable_userbot_target(
                                pool,
                                chat_id=current_chat_id,
                                error_text=last_error,
                            )
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
