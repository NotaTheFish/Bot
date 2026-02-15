from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import asyncpg


@dataclass
class UserbotTask:
    id: int
    created_at: datetime
    run_at: datetime
    status: str
    attempts: int
    last_error: str | None
    storage_chat_id: int
    storage_message_id: int | None
    storage_message_ids: list[int]
    target_chat_ids: list[int] | None
    sent_count: int
    error_count: int


DISABLE_RPC_ERRORS = {
    "ChatWriteForbiddenError",
    "ChannelPrivateError",
    "ChatAdminRequiredError",
    "UserBannedInChannelError",
}

TASK_STATUS_PENDING = "pending"
TASK_STATUS_RUNNING = "processing"
TASK_STATUS_DONE = "done"
TASK_STATUS_FAILED = "error"



def row_to_task(row: asyncpg.Record) -> UserbotTask:
    return UserbotTask(
        id=row["id"],
        created_at=row["created_at"],
        run_at=row["run_at"],
        status=row["status"],
        attempts=row["attempts"],
        last_error=row["last_error"],
        storage_chat_id=row["storage_chat_id"],
        storage_message_id=row["storage_message_id"],
        storage_message_ids=list(row["storage_message_ids"] or []),
        target_chat_ids=(list(row["target_chat_ids"]) if row["target_chat_ids"] is not None else None),
        sent_count=row["sent_count"],
        error_count=row["error_count"],
    )


async def claim_pending_task(pool: asyncpg.Pool) -> UserbotTask | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE userbot_tasks
                SET status=$1, attempts=attempts+1
                WHERE id = (
                    SELECT id FROM userbot_tasks
                    WHERE status=$2 AND run_at<=NOW()
                    ORDER BY run_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
                """,
                TASK_STATUS_RUNNING,
                TASK_STATUS_PENDING,
            )
    return row_to_task(row) if row else None


async def finalize_task(
    pool: asyncpg.Pool,
    task_id: int,
    status: str,
    sent_count: int,
    error_count: int,
    last_error: str | None,
) -> None:
    await pool.execute(
        """
        UPDATE userbot_tasks
        SET status=$2, sent_count=$3, error_count=$4, last_error=$5
        WHERE id=$1
        """,
        task_id,
        status,
        sent_count,
        error_count,
        (last_error or "")[:500] or None,
    )


async def log_broadcast_attempt(
    pool: asyncpg.Pool,
    chat_id: int,
    status: str,
    reason: str,
    error_text: str | None = None,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO broadcast_attempts(created_at, chat_id, status, reason, error_text)
                VALUES ($1, $2, $3, $4, $5)
                """,
                datetime.now(timezone.utc).isoformat(),
                chat_id,
                status,
                reason,
                (error_text or "")[:500] or None,
            )
            await conn.execute(
                """
                DELETE FROM broadcast_attempts
                WHERE id NOT IN (
                    SELECT id
                    FROM broadcast_attempts
                    ORDER BY created_at DESC
                    LIMIT 100
                )
                """
            )


async def mark_chat_success(pool: asyncpg.Pool, chat_id: int, message_id: int) -> None:
    await pool.execute(
        """
        UPDATE chats
        SET last_bot_post_msg_id = $1,
            user_messages_since_last_post = 0,
            last_error = NULL,
            last_success_post_at = $2
        WHERE chat_id = $3
        """,
        message_id,
        datetime.now(timezone.utc).isoformat(),
        chat_id,
    )


async def mark_chat_error(pool: asyncpg.Pool, chat_id: int, error_text: str, disable: bool = False) -> None:
    if disable:
        await pool.execute(
            "UPDATE chats SET disabled = 1, last_error = $1 WHERE chat_id = $2",
            error_text[:500],
            chat_id,
        )
    else:
        await pool.execute(
            "UPDATE chats SET last_error = $1 WHERE chat_id = $2",
            error_text[:500],
            chat_id,
        )


async def remap_chat_id_everywhere(
    pool: asyncpg.Pool,
    old_chat_id: int,
    new_chat_id: int,
    *,
    is_storage_chat: bool = False,
) -> None:
    """
    Централизованно переносит старый chat_id на новый:
    - chats.chat_id (чтобы старый id больше не использовался)
    - userbot_tasks.target_chat_ids
    - userbot_tasks.storage_chat_id + post.storage_chat_id (если мигрировал storage)
    """
    if old_chat_id == new_chat_id:
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1) Основная карта target-чатов
            await conn.execute(
                """
                UPDATE userbot_tasks
                SET target_chat_ids = array_replace(target_chat_ids, $1, $2)
                WHERE target_chat_ids IS NOT NULL
                  AND array_position(target_chat_ids, $1) IS NOT NULL
                """,
                old_chat_id,
                new_chat_id,
            )

            # 2) При миграции storage обновляем пост и все задачи по storage
            if is_storage_chat:
                await conn.execute(
                    "UPDATE post SET storage_chat_id = $2 WHERE storage_chat_id = $1",
                    old_chat_id,
                    new_chat_id,
                )
                await conn.execute(
                    "UPDATE userbot_tasks SET storage_chat_id = $2 WHERE storage_chat_id = $1",
                    old_chat_id,
                    new_chat_id,
                )

            # 3) chats.chat_id: переносим карточку чата на новый ID
            await conn.execute(
                """
                INSERT INTO chats(chat_id, disabled, last_bot_post_msg_id, user_messages_since_last_post, last_error, last_success_post_at)
                SELECT $2, disabled, last_bot_post_msg_id, user_messages_since_last_post, last_error, last_success_post_at
                FROM chats
                WHERE chat_id = $1
                ON CONFLICT (chat_id) DO NOTHING
                """,
                old_chat_id,
                new_chat_id,
            )
            await conn.execute("DELETE FROM chats WHERE chat_id = $1", old_chat_id)