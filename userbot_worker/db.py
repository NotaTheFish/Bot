from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Union, List

import asyncpg

logger = logging.getLogger(__name__)

DBOrPool = Union[str, asyncpg.Pool]


@dataclass(frozen=True)
class TaskRow:
    id: int
    status: str
    run_at: datetime
    target_chat_ids: List[int]
    storage_chat_id: Optional[int]
    storage_message_id: Optional[int]
    storage_message_ids: List[int]
    attempts: int
    last_error: Optional[str]
    sent_count: int
    error_count: int
    dedupe_key: Optional[str]


def _utc_naive_now() -> datetime:
    # Многие схемы/коды используют TIMESTAMP WITHOUT TIME ZONE
    # Поэтому тут отдаём naive datetime (как раньше).
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)


async def _acquire_conn(db: DBOrPool) -> asyncpg.Connection:
    """
    Вспомогательная: если передали pool — берём conn из pool,
    если передали строку DATABASE_URL — создаём одноразовый conn.
    """
    if isinstance(db, asyncpg.Pool):
        return await db.acquire()
    return await asyncpg.connect(dsn=db)


async def _release_conn(db: DBOrPool, conn: asyncpg.Connection) -> None:
    if isinstance(db, asyncpg.Pool):
        await db.release(conn)
    else:
        await conn.close()


async def ensure_schema(db: DBOrPool) -> None:
    """
    Мягкая миграция: создаёт/добавляет недостающие колонки.
    Безопасно вызывать при старте воркера.
    """
    conn = await _acquire_conn(db)
    try:
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
                storage_chat_id BIGINT,
                storage_message_id BIGINT,
                storage_message_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
                sent_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                dedupe_key TEXT
            );
            """
        )

        # add missing columns (safe)
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW();")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS run_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW();")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS last_error TEXT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS target_chat_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[];")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS storage_chat_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS storage_message_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS storage_message_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[];")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS sent_count INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS error_count INTEGER NOT NULL DEFAULT 0;")
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS dedupe_key TEXT;")

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
        storage_chat_id=(int(row["storage_chat_id"]) if row.get("storage_chat_id") is not None else None),
        storage_message_id=(int(row["storage_message_id"]) if row.get("storage_message_id") is not None else None),
        storage_message_ids=_to_int_list(row.get("storage_message_ids")),
        attempts=int(row.get("attempts") or 0),
        last_error=(str(row["last_error"]) if row.get("last_error") is not None else None),
        sent_count=int(row.get("sent_count") or 0),
        error_count=int(row.get("error_count") or 0),
        dedupe_key=(str(row["dedupe_key"]) if row.get("dedupe_key") is not None else None),
    )


async def get_pending_tasks(
    db: DBOrPool,
    *,
    limit: int = 50,
    now: Optional[datetime] = None,
) -> List[TaskRow]:
    """
    Возвращает pending-задачи, которые уже пора выполнять (run_at <= now).
    """
    if now is None:
        now = _utc_naive_now()

    conn = await _acquire_conn(db)
    try:
        rows = await conn.fetch(
            """
            SELECT *
            FROM userbot_tasks
            WHERE status='pending' AND run_at <= $1
            ORDER BY run_at ASC, id ASC
            LIMIT $2
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
    """
    Обновляет прогресс по задаче (в процессе рассылки).
    """
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
    sent_count: Optional[int] = None,
    error_count: Optional[int] = None,
    last_error: Optional[str] = None,
) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status='done',
                sent_count = COALESCE($2, sent_count),
                error_count = COALESCE($3, error_count),
                last_error = $4
            WHERE id = $1
            """,
            int(task_id),
            (int(sent_count) if sent_count is not None else None),
            (int(error_count) if error_count is not None else None),
            (str(last_error)[:4000] if last_error else None),
        )
    finally:
        await _release_conn(db, conn)


async def mark_task_failed(
    db: DBOrPool,
    *,
    task_id: int,
    error: str,
    attempts_inc: int = 1,
) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status='failed',
                attempts = attempts + $2,
                last_error = $3
            WHERE id = $1
            """,
            int(task_id),
            int(attempts_inc),
            str(error)[:4000],
        )
    finally:
        await _release_conn(db, conn)


# === Backward-compatible names expected by some worker.py versions ===

async def mark_task_error(
    db: DBOrPool,
    *,
    task_id: int,
    error: str,
    attempts_inc: int = 1,
) -> None:
    """
    Совместимость: некоторые версии worker.py импортируют mark_task_error.
    """
    await mark_task_failed(db, task_id=task_id, error=error, attempts_inc=attempts_inc)
