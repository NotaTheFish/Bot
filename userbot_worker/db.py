from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional, Union

import asyncpg

DBOrPool = Union[str, asyncpg.Pool]


@dataclass(frozen=True)
class TaskRow:
    id: int
    status: str
    run_at: datetime
    target_chat_ids: List[int]
    storage_chat_id: int
    storage_message_ids: List[int]
    attempts: int
    last_error: Optional[str]
    sent_count: int
    error_count: int
    dedupe_key: Optional[str]


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)


async def _acquire_conn(db: DBOrPool) -> asyncpg.Connection:
    if isinstance(db, asyncpg.Pool):
        return await db.acquire()
    return await asyncpg.connect(dsn=db)


async def _release_conn(db: DBOrPool, conn: asyncpg.Connection) -> None:
    if isinstance(db, asyncpg.Pool):
        await db.release(conn)
    else:
        await conn.close()


async def ensure_schema(db: DBOrPool) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS userbot_tasks (
                id BIGSERIAL PRIMARY KEY,
                run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                target_chat_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
                storage_chat_id BIGINT NOT NULL,
                storage_message_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
                sent_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                dedupe_key TEXT
            );
            """
        )

        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS run_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS last_error TEXT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS target_chat_ids BIGINT[];")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS storage_chat_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS storage_message_ids BIGINT[];")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS sent_count INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS error_count INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS dedupe_key TEXT;")

        await conn.execute("UPDATE userbot_tasks SET target_chat_ids='{}' WHERE target_chat_ids IS NULL;")
        await conn.execute("UPDATE userbot_tasks SET storage_message_ids='{}' WHERE storage_message_ids IS NULL;")
        await conn.execute("UPDATE userbot_tasks SET status='processing' WHERE status='running';")
        await conn.execute("UPDATE userbot_tasks SET status='error' WHERE status='failed';")

        await conn.execute("ALTER TABLE userbot_tasks ALTER COLUMN target_chat_ids SET DEFAULT '{}'::bigint[];")
        await conn.execute("ALTER TABLE userbot_tasks ALTER COLUMN target_chat_ids SET NOT NULL;")
        await conn.execute("ALTER TABLE userbot_tasks ALTER COLUMN storage_chat_id SET NOT NULL;")
        await conn.execute("ALTER TABLE userbot_tasks ALTER COLUMN storage_message_ids SET DEFAULT '{}'::bigint[];")
        await conn.execute("ALTER TABLE userbot_tasks ALTER COLUMN storage_message_ids SET NOT NULL;")

        await conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_userbot_tasks_dedupe_key
            ON userbot_tasks(dedupe_key)
            WHERE dedupe_key IS NOT NULL;
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
            ON userbot_tasks(status, run_at);
            """
        )
    finally:
        await _release_conn(db, conn)


def _to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    return [int(x) for x in v]


def _row_to_task(row: asyncpg.Record) -> TaskRow:
    return TaskRow(
        id=int(row["id"]),
        status=str(row["status"]),
        run_at=row["run_at"],
        target_chat_ids=_to_int_list(row.get("target_chat_ids")),
        storage_chat_id=int(row["storage_chat_id"]),
        storage_message_ids=_to_int_list(row.get("storage_message_ids")),
        attempts=int(row.get("attempts") or 0),
        last_error=(str(row["last_error"]) if row.get("last_error") is not None else None),
        sent_count=int(row.get("sent_count") or 0),
        error_count=int(row.get("error_count") or 0),
        dedupe_key=(str(row["dedupe_key"]) if row.get("dedupe_key") is not None else None),
    )


async def claim_pending_tasks(
    db: DBOrPool,
    *,
    limit: int,
    now: Optional[datetime] = None,
) -> List[TaskRow]:
    now = now or datetime.now(timezone.utc)

    conn = await _acquire_conn(db)
    try:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                WITH picked AS (
                    SELECT id
                    FROM userbot_tasks
                    WHERE status='pending' AND run_at <= $1
                    ORDER BY run_at ASC, id ASC
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE userbot_tasks t
                SET status='processing', attempts=t.attempts + 1
                FROM picked
                WHERE t.id = picked.id
                RETURNING t.*
                """,
                now,
                int(limit),
            )
        return [_row_to_task(r) for r in rows]
    finally:
        await _release_conn(db, conn)


async def update_task_progress(
    db: DBOrPool,
    *,
    task_id: int,
    sent_count: int,
    error_count: int,
    last_error: Optional[str] = None,
) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET sent_count = $2,
                error_count = $3,
                last_error = $4
            WHERE id = $1
            """,
            int(task_id),
            int(sent_count),
            int(error_count),
            (str(last_error)[:4000] if last_error else None),
        )
    finally:
        await _release_conn(db, conn)


async def mark_task_done(
    db: DBOrPool,
    *,
    task_id: int,
    sent_count: int,
    error_count: int,
    last_error: Optional[str] = None,
) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status='done',
                sent_count=$2,
                error_count=$3,
                last_error=$4
            WHERE id = $1
            """,
            int(task_id),
            int(sent_count),
            int(error_count),
            (str(last_error)[:4000] if last_error else None),
        )
    finally:
        await _release_conn(db, conn)


async def requeue_or_mark_error(
    db: DBOrPool,
    *,
    task_id: int,
    attempts: int,
    max_attempts: int,
    error: str,
    sent_count: int,
    error_count: int,
) -> None:
    next_status = "pending" if int(attempts) < int(max_attempts) else "error"
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status=$2,
                last_error=$3,
                sent_count=$4,
                error_count=$5
            WHERE id=$1
            """,
            int(task_id),
            next_status,
            str(error)[:4000],
            int(sent_count),
            int(error_count),
        )
    finally:
        await _release_conn(db, conn)


async def mark_task_error(
    db: DBOrPool,
    *,
    task_id: int,
    attempts: int,
    max_attempts: int,
    error: str,
    sent_count: int,
    error_count: int,
) -> None:
    await requeue_or_mark_error(
        db,
        task_id=task_id,
        attempts=attempts,
        max_attempts=max_attempts,
        error=error,
        sent_count=sent_count,
        error_count=error_count,
    )
