"""Shared queue insert for userbot_tasks (used by main controller and tenant controller)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

_userbot_tasks_schema_lock = asyncio.Lock()
_userbot_tasks_schema_ready = False
_userbot_tasks_columns_cache: Optional[set[str]] = None


def invalidate_column_cache() -> None:
    global _userbot_tasks_columns_cache
    _userbot_tasks_columns_cache = None


_USERBOT_TASKS_MIN_SQL = [
    """
    CREATE TABLE IF NOT EXISTS userbot_tasks (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      last_error TEXT,
      storage_chat_id BIGINT NOT NULL,
      storage_message_id BIGINT,
      dedupe_key TEXT,
      storage_message_ids BIGINT[] NOT NULL DEFAULT '{}',
      target_chat_ids BIGINT[] NOT NULL DEFAULT '{}',
      sent_count INT NOT NULL DEFAULT 0,
      error_count INT NOT NULL DEFAULT 0,
      skipped_breakdown JSONB
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_userbot_tasks_pending
    ON userbot_tasks(status, run_at)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_userbot_tasks_dedupe_active
    ON userbot_tasks(dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'processing')
    """,
    """
    ALTER TABLE userbot_tasks
    ADD COLUMN IF NOT EXISTS workspace_key TEXT NOT NULL DEFAULT 'main'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_userbot_tasks_workspace_pending
    ON userbot_tasks(workspace_key, status, run_at)
    """,
]


async def ensure_userbot_tasks_minimal_schema(pool: asyncpg.Pool) -> None:
    global _userbot_tasks_schema_ready
    if _userbot_tasks_schema_ready:
        return
    async with _userbot_tasks_schema_lock:
        if _userbot_tasks_schema_ready:
            return
        async with pool.acquire() as conn:
            async with conn.transaction():
                for query in _USERBOT_TASKS_MIN_SQL:
                    await conn.execute(query)
        global _userbot_tasks_columns_cache
        _userbot_tasks_columns_cache = None
        _userbot_tasks_schema_ready = True


async def get_userbot_tasks_columns(pool: asyncpg.Pool) -> set[str]:
    global _userbot_tasks_columns_cache
    if _userbot_tasks_columns_cache is not None:
        return _userbot_tasks_columns_cache
    rows = await pool.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'userbot_tasks'
        """
    )
    _userbot_tasks_columns_cache = {str(row["column_name"]) for row in rows}
    return _userbot_tasks_columns_cache


async def create_userbot_task(
    pool: asyncpg.Pool,
    storage_chat_id: int,
    storage_message_ids: list[int],
    target_chat_ids: list[int] | None,
    run_at: datetime,
    workspace_key: str = "main",
) -> Optional[int]:
    def _logical_schedule_bucket(run_at_value: datetime) -> str:
        return run_at_value.strftime("%Y%m%d%H%M")

    normalized_target_chat_ids = sorted(set(target_chat_ids or []))
    normalized_storage_message_ids = sorted(storage_message_ids or [])
    run_at_utc = run_at if run_at.tzinfo else run_at.replace(tzinfo=timezone.utc)
    run_at_utc = run_at_utc.astimezone(timezone.utc)
    schedule_bucket = _logical_schedule_bucket(run_at_utc)

    storage_message_id = normalized_storage_message_ids[0] if normalized_storage_message_ids else None
    ws = (workspace_key or "main").strip() or "main"
    dedupe_key = (
        f"v3:{ws}:{storage_chat_id}:"
        f"{','.join(str(message_id) for message_id in normalized_storage_message_ids)}:"
        f"{schedule_bucket}"
    )

    await ensure_userbot_tasks_minimal_schema(pool)
    existing_columns = await get_userbot_tasks_columns(pool)

    insert_columns: list[str] = ["storage_chat_id", "storage_message_ids", "run_at", "status", "target_chat_ids"]
    insert_values: list[object] = [
        storage_chat_id,
        normalized_storage_message_ids,
        run_at_utc,
        "pending",
        normalized_target_chat_ids,
    ]

    if "storage_message_id" in existing_columns:
        insert_columns.append("storage_message_id")
        insert_values.append(storage_message_id)
    if "attempts" in existing_columns:
        insert_columns.append("attempts")
        insert_values.append(0)
    if "last_error" in existing_columns:
        insert_columns.append("last_error")
        insert_values.append(None)

    dedupe_enabled = "dedupe_key" in existing_columns and "status" in existing_columns
    if dedupe_enabled:
        insert_columns.append("dedupe_key")
        insert_values.append(dedupe_key)
    if "workspace_key" in existing_columns:
        insert_columns.append("workspace_key")
        insert_values.append(ws)

    placeholders: list[str] = []
    for index, column in enumerate(insert_columns, start=1):
        if column in {"storage_message_ids", "target_chat_ids"}:
            placeholders.append(f"${index}::BIGINT[]")
        elif column == "run_at":
            placeholders.append(f"${index}::TIMESTAMPTZ")
        else:
            placeholders.append(f"${index}")

    query = (
        f"INSERT INTO userbot_tasks({', '.join(insert_columns)}) "
        f"VALUES ({', '.join(placeholders)})"
    )
    if dedupe_enabled:
        query += " " + (
            "ON CONFLICT (dedupe_key) "
            "WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'processing') "
            "DO NOTHING"
        )
    query += " RETURNING id"

    row = await pool.fetchrow(query, *insert_values)
    if row:
        return int(row["id"])

    if not dedupe_enabled:
        return None

    existing_task_id = await pool.fetchval(
        """
        SELECT id
        FROM userbot_tasks
        WHERE dedupe_key = $1
          AND status IN ('pending', 'processing')
        ORDER BY id DESC
        LIMIT 1
        """,
        dedupe_key,
    )
    if existing_task_id:
        logger.info(
            "Userbot task deduplicated: existing task id=%s dedupe_key=%s",
            existing_task_id,
            dedupe_key,
        )
        return int(existing_task_id)
    return None
