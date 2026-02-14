from __future__ import annotations

import asyncio
import json
import logging
import asyncpg
from telethon import TelegramClient
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
                    "targets": len(task.target_chat_ids),
                },
                ensure_ascii=False,
            )
        )

        for chat_id in task.target_chat_ids:
            try:
                msg_id = await forward_post(
                    client,
                    source_chat_id=task.source_chat_id,
                    source_message_id=task.source_message_id,
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