from __future__ import annotations

import asyncio
import json
import logging
import asyncpg
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User
from telethon.errors import RPCError

from .config import Settings
from .sender import forward_post
from .tasks import (
    DISABLE_RPC_ERRORS,
    claim_pending_task,
    finalize_task,
    log_broadcast_attempt,
    mark_chat_error,
    mark_chat_success,
)

logger = logging.getLogger(__name__)


async def notify_admin(settings: Settings, text: str) -> None:
    if not settings.admin_notify_bot_token or settings.admin_id <= 0:
        return
    try:
        import aiohttp

        async with aiohttp.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{settings.admin_notify_bot_token}/sendMessage",
                json={"chat_id": settings.admin_id, "text": text},
                timeout=15,
            )
    except Exception as exc:
        logger.warning("admin notify failed: %s", exc)


async def run_worker(settings: Settings, pool: asyncpg.Pool, client: TelegramClient, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        task = await claim_pending_task(pool)
        if not task:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        sent_count = 0
        error_count = 0
        last_error: str | None = None

        logger.info(
            json.dumps(
                {
                    "event": "task_started",
                    "task_id": task.id,
                    "targets": len(task.target_chat_ids or []),
                },
                ensure_ascii=False,
            )
        )

        target_chat_ids = list(task.target_chat_ids or [])
        if task.target_chat_ids is None:
            storage_chat_id = settings.storage_chat_id or task.storage_chat_id
            auto_targets: list[int] = []

            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, User):
                    continue
                if getattr(dialog, "is_user", False):
                    continue
                if getattr(entity, "id", None) is None:
                    continue

                if isinstance(entity, Chat):
                    if getattr(entity, "left", False) or getattr(entity, "deactivated", False):
                        continue
                elif isinstance(entity, Channel):
                    if getattr(entity, "broadcast", False):
                        can_post = bool(getattr(entity, "creator", False))
                        if not can_post:
                            admin_rights = getattr(entity, "admin_rights", None)
                            can_post = bool(admin_rights and getattr(admin_rights, "post_messages", False))
                        if not can_post:
                            continue
                else:
                    continue

                chat_id = dialog.id
                if chat_id == storage_chat_id:
                    continue
                auto_targets.append(chat_id)

            target_chat_ids = auto_targets
            logger.info("AUTO targets count=%s", len(target_chat_ids))

            if not target_chat_ids:
                await finalize_task(pool, task.id, "error", 0, 0, "No writable chats found")
                logger.info("Userbot task #%s: status=error, sent=0, errors=0", task.id)
                await notify_admin(settings, f"Userbot task #{task.id}: status=error, sent=0, errors=0")
                continue

        for chat_id in target_chat_ids:
            try:
                source_message_ids = task.storage_message_ids or ([] if task.storage_message_id is None else [task.storage_message_id])
                if not source_message_ids:
                    raise ValueError("Task has empty storage_message_ids")

                msg_id = await forward_post(
                    client,
                    source_chat_id=task.storage_chat_id,
                    source_message_ids=source_message_ids,
                    target_chat_id=chat_id,
                    min_delay=settings.min_seconds_between_chats,
                    max_delay=settings.max_seconds_between_chats,
                )
                await mark_chat_success(pool, chat_id, msg_id)
                await log_broadcast_attempt(pool, chat_id, status="sent", reason="userbot_ok")
                sent_count += 1
            except RPCError as exc:
                error_count += 1
                last_error = str(exc)
                err_name = exc.__class__.__name__
                disable = err_name in DISABLE_RPC_ERRORS
                await mark_chat_error(pool, chat_id, str(exc), disable=disable)
                await log_broadcast_attempt(
                    pool,
                    chat_id,
                    status="error",
                    reason=("forbidden" if disable else "rpc_error"),
                    error_text=f"{err_name}: {exc}",
                )
                logger.warning("RPCError task=%s chat=%s err=%s", task.id, chat_id, err_name)
            except Exception as exc:
                error_count += 1
                last_error = str(exc)
                await mark_chat_error(pool, chat_id, str(exc), disable=False)
                await log_broadcast_attempt(
                    pool,
                    chat_id,
                    status="error",
                    reason="exception",
                    error_text=str(exc),
                )
                logger.exception("unexpected error task=%s chat=%s", task.id, chat_id)

        if sent_count > 0 and error_count == 0:
            status = "sent"
        elif sent_count > 0 and error_count > 0:
            status = "partial"
        else:
            status = "error"

        await finalize_task(pool, task.id, status, sent_count, error_count, last_error)
        summary = f"Userbot task #{task.id}: status={status}, sent={sent_count}, errors={error_count}"
        logger.info(summary)
        await notify_admin(settings, summary)