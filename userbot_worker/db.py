from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import asyncpg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskRow:
    id: int
    status: str
    run_at: datetime
    target_chat_ids: list[int]
    source_chat_id: int | None
    source_message_id: int | None
    attempts: int
    last_error: str | None
    sent_count: int
    error_count: int
    dedupe_key: str | None


async def create_pool(database_url: str) -> asyncpg.Pool:
    """
    Creates asyncpg pool.
    """
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)


async def ensure_schema(pool: asyncpg.Pool) -> None:
    """
    Ensures required tables/columns exist. Safe to call multiple times.
    """
    async with pool.acquire() as conn:
        # Create table if missing (minimal baseline)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS userbot_tasks (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                run_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                target_chat_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
                source_chat_id BIGINT,
                source_message_id BIGINT,
                sent_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # Add/ensure columns (for older schemas)
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW();")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS run_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW();")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS last_error TEXT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS target_chat_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[];")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS source_chat_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS source_message_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS sent_count INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS error_count INTEGER NOT NULL DEFAULT 0;")

        # Optional dedupe support
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS dedupe_key TEXT;")
        await conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_userbot_tasks_dedupe_key
            ON userbot_tasks(dedupe_key)
            WHERE dedupe_key IS NOT NULL;
            """
        )

        # Helpful index for polling
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
            ON userbot_tasks(status, run_at);
            """
        )


def _row_to_task(row: asyncpg.Record) -> TaskRow:
    def _as_list(v: Any) -> list[int]:
        if v is None:
            return []
        return [int(x) for x in v]

    return TaskRow(
        id=int(row["id"]),
        status=str(row["status"]),
        run_at=row["run_at"],
        target_chat_ids=_as_list(row.get("target_chat_ids")),
        source_chat_id=(int(row["source_chat_id"]) if row.get("source_chat_id") is not None else None),
        source_message_id=(int(row["source_message_id"]) if row.get("source_message_id") is not None else None),
        attempts=int(row.get("attempts") or 0),
        last_error=(str(row["last_error"]) if row.get("last_error") is not None else None),
        sent_count=int(row.get("sent_count") or 0),
        error_count=int(row.get("error_count") or 0),
        dedupe_key=(str(row["dedupe_key"]) if row.get("dedupe_key") is not None else None),
    )


async def get_pending_tasks(
    pool: asyncpg.Pool,
    *,
    limit: int = 50,
    now: Optional[datetime] = None,
) -> list[TaskRow]:
    """
    Returns pending tasks that are due.
    Does NOT lock rows. (Use acquire_pending_tasks() if you want locking.)
    """
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)  # keep DB style (no tz)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM userbot_tasks
            WHERE status = 'pending' AND run_at <= $1
            ORDER BY run_at ASC, id ASC
            LIMIT $2
            """,
            now,
            int(limit),
        )
    return [_row_to_task(r) for r in rows]


async def acquire_pending_tasks(
    pool: asyncpg.Pool,
    *,
    limit: int = 20,
    now: Optional[datetime] = None,
) -> list[TaskRow]:
    """
    Atomically locks and marks tasks as 'running' to avoid duplicate workers.
    Uses FOR UPDATE SKIP LOCKED.
    """
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                WITH picked AS (
                    SELECT id
                    FROM userbot_tasks
                    WHERE status = 'pending' AND run_at <= $1
                    ORDER BY run_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                )
                UPDATE userbot_tasks t
                SET status = 'running'
                FROM picked
                WHERE t.id = picked.id
                RETURNING t.*;
                """,
                now,
                int(limit),
            )
    return [_row_to_task(r) for r in rows]


async def mark_task_done(pool: asyncpg.Pool, task_id: int, *, sent_inc: int = 0, error_inc: int = 0) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status = 'done',
                sent_count = sent_count + $2,
                error_count = error_count + $3,
                last_error = NULL
            WHERE id = $1
            """,
            int(task_id),
            int(sent_inc),
            int(error_inc),
        )


async def mark_task_failed(pool: asyncpg.Pool, task_id: int, *, error: str, attempts_inc: int = 1) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status = 'failed',
                attempts = attempts + $2,
                last_error = $3
            WHERE id = $1
            """,
            int(task_id),
            int(attempts_inc),
            str(error)[:4000],
        )


async def mark_task_pending_again(
    pool: asyncpg.Pool,
    task_id: int,
    *,
    next_run_at: Optional[datetime] = None,
    error: Optional[str] = None,
    attempts_inc: int = 1,
) -> None:
    """
    For retries: put task back to pending with updated run_at.
    """
    if next_run_at is None:
        next_run_at = datetime.now(timezone.utc).replace(tzinfo=None)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status = 'pending',
                run_at = $2,
                attempts = attempts + $3,
                last_error = COALESCE($4, last_error)
            WHERE id = $1
            """,
            int(task_id),
            next_run_at,
            int(attempts_inc),
            (str(error)[:4000] if error else None),
        )
