from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

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
    skipped_breakdown: Dict[str, int]




@dataclass(frozen=True)
class WorkerAutoreplySettings:
    enabled: bool
    reply_text: str
    template_source: str
    template_storage_chat_id: Optional[int]
    template_storage_message_ids: List[int]
    trigger_mode: str
    offline_threshold_minutes: int
    cooldown_seconds: int

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
        await conn.execute("ALTER TABLE userbot_tasks ADD COLUMN IF NOT EXISTS skipped_breakdown JSONB;")
        await conn.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name='userbot_tasks'
                      AND column_name='skipped_breakdown'
                      AND data_type='text'
                ) THEN
                    ALTER TABLE userbot_tasks
                    ALTER COLUMN skipped_breakdown TYPE JSONB
                    USING NULLIF(skipped_breakdown, '')::jsonb;
                END IF;
            END$$;
            """
        )

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

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS userbot_targets (
              chat_id BIGINT PRIMARY KEY,
              chat_type TEXT NOT NULL,
              title TEXT,
              username TEXT,
              enabled BOOLEAN NOT NULL DEFAULT TRUE,
              last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              last_error TEXT,
              migrated_to BIGINT
            );
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_userbot_targets_enabled
            ON userbot_targets(enabled);
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_autoreply_settings (
              id INTEGER PRIMARY KEY DEFAULT 1,
              enabled BOOLEAN NOT NULL DEFAULT TRUE,
              reply_text TEXT NOT NULL DEFAULT 'Здравствуйте! Я технический аккаунт рассылки. Ответим вам позже.',
              template_source TEXT NOT NULL DEFAULT 'text',
              template_storage_chat_id BIGINT,
              template_storage_message_ids BIGINT[] DEFAULT '{}'::bigint[],
              trigger_mode TEXT NOT NULL DEFAULT 'both',
              offline_threshold_minutes INTEGER NOT NULL DEFAULT 60,
              cooldown_seconds INTEGER NOT NULL DEFAULT 3600,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            "ALTER TABLE worker_autoreply_settings ADD COLUMN IF NOT EXISTS template_source TEXT NOT NULL DEFAULT 'text';"
        )
        await conn.execute("ALTER TABLE worker_autoreply_settings ADD COLUMN IF NOT EXISTS template_storage_chat_id BIGINT;")
        await conn.execute(
            "ALTER TABLE worker_autoreply_settings ADD COLUMN IF NOT EXISTS template_storage_message_ids BIGINT[];"
        )
        await conn.execute(
            "UPDATE worker_autoreply_settings SET template_storage_message_ids='{}'::bigint[] WHERE template_storage_message_ids IS NULL;"
        )
        await conn.execute(
            "ALTER TABLE worker_autoreply_settings ALTER COLUMN template_storage_message_ids SET DEFAULT '{}'::bigint[];"
        )
        await conn.execute("INSERT INTO worker_autoreply_settings(id) VALUES (1) ON CONFLICT(id) DO NOTHING;")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_autoreply_contacts (
              user_id BIGINT PRIMARY KEY,
              first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              last_replied_at TIMESTAMPTZ
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_state (
              id INTEGER PRIMARY KEY DEFAULT 1,
              last_outgoing_at TIMESTAMPTZ,
              last_manual_outgoing_at TIMESTAMPTZ
            );
            """
        )
        await conn.execute("ALTER TABLE worker_state ADD COLUMN IF NOT EXISTS last_manual_outgoing_at TIMESTAMPTZ;")
        await conn.execute(
            "INSERT INTO worker_state(id,last_outgoing_at,last_manual_outgoing_at) VALUES (1, NULL, NULL) ON CONFLICT(id) DO NOTHING;"
        )
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMPTZ;")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS last_success_post_at TIMESTAMPTZ;")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS last_self_message_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS last_post_fingerprint TEXT;")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS last_post_at TIMESTAMPTZ;")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS peer_type TEXT NOT NULL DEFAULT 'channel';")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS peer_id BIGINT;")
        await conn.execute("ALTER TABLE userbot_targets ADD COLUMN IF NOT EXISTS access_hash BIGINT;")
        await conn.execute(
            """
            UPDATE userbot_targets
            SET peer_type = CASE WHEN chat_id::text LIKE '-100%' THEN 'channel' ELSE 'chat' END
            WHERE peer_type IS NULL OR peer_type = ''
            """
        )
        await conn.execute(
            """
            UPDATE userbot_targets
            SET peer_id = CASE
                WHEN chat_id::text LIKE '-100%' THEN ABS((right(chat_id::text, length(chat_id::text) - 4))::bigint)
                ELSE ABS(chat_id)
            END
            WHERE peer_id IS NULL
            """
        )
    finally:
        await _release_conn(db, conn)


def _to_int_list(v: Any) -> List[int]:
    if v is None:
        return []
    return [int(x) for x in v]


def _row_to_task(row: asyncpg.Record) -> TaskRow:
    raw_skipped_breakdown = row.get("skipped_breakdown")
    if isinstance(raw_skipped_breakdown, str):
        try:
            raw_skipped_breakdown = json.loads(raw_skipped_breakdown)
        except json.JSONDecodeError:
            raw_skipped_breakdown = {}
    elif raw_skipped_breakdown is None:
        raw_skipped_breakdown = {}

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
        skipped_breakdown={str(k): int(v) for k, v in raw_skipped_breakdown.items()},
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
    skipped_breakdown: Optional[Dict[str, int]] = None,
) -> None:
    skipped_breakdown_json = json.dumps(skipped_breakdown) if skipped_breakdown is not None else None

    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET sent_count = $2,
                error_count = $3,
                last_error = $4,
                skipped_breakdown = NULLIF($5, '')::jsonb
            WHERE id = $1
            """,
            int(task_id),
            int(sent_count),
            int(error_count),
            (str(last_error)[:4000] if last_error else None),
            skipped_breakdown_json,
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
    skipped_breakdown: Optional[Dict[str, int]] = None,
    final_status: str = "done",
) -> None:
    skipped_breakdown_json = json.dumps(skipped_breakdown) if skipped_breakdown is not None else None

    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_tasks
            SET status=$2,
                sent_count=$3,
                error_count=$4,
                last_error=$5,
                skipped_breakdown = NULLIF($6, '')::jsonb
            WHERE id = $1
            """,
            int(task_id),
            str(final_status),
            int(sent_count),
            int(error_count),
            (str(last_error)[:4000] if last_error else None),
            skipped_breakdown_json,
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


@dataclass(frozen=True)
class UserbotTargetState:
    chat_id: int
    enabled: bool
    last_success_post_at: Optional[datetime]
    last_self_message_id: Optional[int]
    last_post_fingerprint: Optional[str]
    last_post_at: Optional[datetime]


@dataclass(frozen=True)
class UserbotTargetPeer:
    chat_id: int
    peer_type: str
    peer_id: int
    access_hash: Optional[int]
    title: Optional[str]
    username: Optional[str]


async def get_userbot_target_state(db: DBOrPool, *, chat_id: int) -> Optional[UserbotTargetState]:
    conn = await _acquire_conn(db)
    try:
        row = await conn.fetchrow(
            """
            SELECT chat_id, enabled, last_success_post_at, last_self_message_id, last_post_fingerprint, last_post_at
            FROM userbot_targets
            WHERE chat_id = $1
            """,
            int(chat_id),
        )
        if not row:
            return None
        return UserbotTargetState(
            chat_id=int(row["chat_id"]),
            enabled=bool(row["enabled"]),
            last_success_post_at=row.get("last_success_post_at"),
            last_self_message_id=(int(row["last_self_message_id"]) if row.get("last_self_message_id") is not None else None),
            last_post_fingerprint=(str(row["last_post_fingerprint"]) if row.get("last_post_fingerprint") is not None else None),
            last_post_at=row.get("last_post_at"),
        )
    finally:
        await _release_conn(db, conn)


async def set_userbot_target_last_self_message_id(db: DBOrPool, *, chat_id: int, message_id: int) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_targets
            SET last_self_message_id = $2
            WHERE chat_id = $1
            """,
            int(chat_id),
            int(message_id),
        )
    finally:
        await _release_conn(db, conn)


async def mark_userbot_target_disabled(db: DBOrPool, *, chat_id: int, error_text: str) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_targets
            SET enabled = FALSE,
                last_error = $2,
                last_error_at = NOW()
            WHERE chat_id = $1
            """,
            int(chat_id),
            str(error_text)[:2000],
        )
    finally:
        await _release_conn(db, conn)


async def mark_userbot_target_success(
    db: DBOrPool,
    *,
    chat_id: int,
    sent_message_id: int,
    fingerprint: str,
) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            UPDATE userbot_targets
            SET enabled = TRUE,
                last_error = NULL,
                last_error_at = NULL,
                last_success_post_at = NOW(),
                last_self_message_id = $2,
                last_post_fingerprint = $3,
                last_post_at = NOW()
            WHERE chat_id = $1
            """,
            int(chat_id),
            int(sent_message_id),
            str(fingerprint)[:200],
        )
    finally:
        await _release_conn(db, conn)


async def upsert_userbot_target(
    db: DBOrPool,
    *,
    chat_id: int,
    chat_type: str,
    peer_type: str,
    peer_id: int,
    access_hash: Optional[int],
    title: Optional[str],
    username: Optional[str],
) -> bool:
    conn = await _acquire_conn(db)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO userbot_targets(chat_id, chat_type, peer_type, peer_id, access_hash, title, username, enabled, last_seen_at, last_error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, NOW(), NULL)
            ON CONFLICT(chat_id) DO UPDATE
            SET chat_type = EXCLUDED.chat_type,
                peer_type = EXCLUDED.peer_type,
                peer_id = EXCLUDED.peer_id,
                access_hash = EXCLUDED.access_hash,
                title = COALESCE(EXCLUDED.title, userbot_targets.title),
                username = COALESCE(EXCLUDED.username, userbot_targets.username),
                enabled = TRUE,
                last_seen_at = NOW(),
                last_error = NULL
            RETURNING (xmax = 0) AS inserted
            """,
            int(chat_id),
            str(chat_type),
            str(peer_type),
            int(peer_id),
            (int(access_hash) if access_hash is not None else None),
            (str(title) if title else None),
            (str(username) if username else None),
        )
        return bool(row and row["inserted"])
    finally:
        await _release_conn(db, conn)


async def disable_stale_userbot_targets(
    db: DBOrPool,
    *,
    grace_days: int,
) -> int:
    conn = await _acquire_conn(db)
    try:
        result = await conn.execute(
            """
            UPDATE userbot_targets
            SET enabled = FALSE,
                last_error = COALESCE(last_error, 'Not seen in recent dialogs sync')
            WHERE enabled = TRUE
              AND last_seen_at < NOW() - ($1::int * INTERVAL '1 day')
            """,
            max(1, int(grace_days)),
        )
        return int(str(result).split()[-1])
    finally:
        await _release_conn(db, conn)


async def load_enabled_userbot_targets(db: DBOrPool) -> List[UserbotTargetPeer]:
    conn = await _acquire_conn(db)
    try:
        rows = await conn.fetch(
            """
            SELECT chat_id, peer_type, peer_id, access_hash, title, username
            FROM userbot_targets
            WHERE enabled = TRUE
            ORDER BY chat_id
            """
        )
        return [
            UserbotTargetPeer(
                chat_id=int(r["chat_id"]),
                peer_type=str(r["peer_type"] or "channel"),
                peer_id=int(r["peer_id"] or 0),
                access_hash=(int(r["access_hash"]) if r["access_hash"] is not None else None),
                title=(str(r["title"]) if r["title"] is not None else None),
                username=(str(r["username"]) if r["username"] is not None else None),
            )
            for r in rows
            if r["peer_id"] is not None
        ]
    finally:
        await _release_conn(db, conn)


async def load_userbot_targets_by_chat_ids(db: DBOrPool, *, chat_ids: List[int]) -> List[UserbotTargetPeer]:
    normalized_ids = sorted({int(chat_id) for chat_id in chat_ids})
    if not normalized_ids:
        return []

    conn = await _acquire_conn(db)
    try:
        rows = await conn.fetch(
            """
            SELECT chat_id, peer_type, peer_id, access_hash, title, username
            FROM userbot_targets
            WHERE chat_id = ANY($1::bigint[])
            """,
            normalized_ids,
        )
        return [
            UserbotTargetPeer(
                chat_id=int(r["chat_id"]),
                peer_type=str(r["peer_type"] or "channel"),
                peer_id=int(r["peer_id"] or 0),
                access_hash=(int(r["access_hash"]) if r["access_hash"] is not None else None),
                title=(str(r["title"]) if r["title"] is not None else None),
                username=(str(r["username"]) if r["username"] is not None else None),
            )
            for r in rows
            if r["peer_id"] is not None
        ]
    finally:
        await _release_conn(db, conn)


async def disable_userbot_target(
    db: DBOrPool,
    *,
    chat_id: int,
    error_text: str,
) -> None:
    await mark_userbot_target_disabled(db, chat_id=chat_id, error_text=error_text)


async def remap_userbot_target_chat_id(
    db: DBOrPool,
    *,
    old_chat_id: int,
    new_chat_id: int,
) -> None:
    if int(old_chat_id) == int(new_chat_id):
        return

    conn = await _acquire_conn(db)
    try:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO userbot_targets(chat_id, chat_type, peer_type, peer_id, access_hash, title, username, enabled, last_seen_at, last_error, migrated_to)
                SELECT $2,
                       chat_type,
                       peer_type,
                       peer_id,
                       access_hash,
                       title,
                       username,
                       enabled,
                       last_seen_at,
                       last_error,
                       migrated_to
                FROM userbot_targets
                WHERE chat_id = $1
                ON CONFLICT(chat_id) DO UPDATE
                SET chat_type = EXCLUDED.chat_type,
                    peer_type = EXCLUDED.peer_type,
                    peer_id = EXCLUDED.peer_id,
                    access_hash = EXCLUDED.access_hash,
                    title = COALESCE(EXCLUDED.title, userbot_targets.title),
                    username = COALESCE(EXCLUDED.username, userbot_targets.username),
                    enabled = userbot_targets.enabled OR EXCLUDED.enabled,
                    last_seen_at = GREATEST(userbot_targets.last_seen_at, EXCLUDED.last_seen_at),
                    last_error = COALESCE(userbot_targets.last_error, EXCLUDED.last_error),
                    migrated_to = NULL
                """,
                int(old_chat_id),
                int(new_chat_id),
            )
            await conn.execute(
                """
                UPDATE userbot_targets
                SET migrated_to = $2,
                    enabled = FALSE
                WHERE chat_id = $1
                """,
                int(old_chat_id),
                int(new_chat_id),
            )
            await conn.execute("DELETE FROM userbot_targets WHERE chat_id = $1", int(old_chat_id))
    finally:
        await _release_conn(db, conn)


async def get_worker_autoreply_settings(db: DBOrPool) -> WorkerAutoreplySettings:
    conn = await _acquire_conn(db)
    try:
        row = await conn.fetchrow(
            """
            SELECT enabled, reply_text, template_source, template_storage_chat_id, template_storage_message_ids,
                   trigger_mode, offline_threshold_minutes, cooldown_seconds
            FROM worker_autoreply_settings
            WHERE id = 1
            """
        )
        if not row:
            return WorkerAutoreplySettings(
                enabled=True,
                reply_text="Здравствуйте! Я технический аккаунт рассылки. Ответим вам позже.",
                template_source="text",
                template_storage_chat_id=None,
                template_storage_message_ids=[],
                trigger_mode="both",
                offline_threshold_minutes=60,
                cooldown_seconds=3600,
            )
        mode = str(row["trigger_mode"] or "both")
        if mode not in {"first_message_only", "offline_over_minutes", "both"}:
            mode = "both"
        template_source = str(row["template_source"] or "text")
        if template_source not in {"text", "storage"}:
            template_source = "text"
        return WorkerAutoreplySettings(
            enabled=bool(row["enabled"]),
            reply_text=str(row["reply_text"] or "Здравствуйте! Я технический аккаунт рассылки. Ответим вам позже."),
            template_source=template_source,
            template_storage_chat_id=int(row["template_storage_chat_id"]) if row["template_storage_chat_id"] is not None else None,
            template_storage_message_ids=_to_int_list(row["template_storage_message_ids"]),
            trigger_mode=mode,
            offline_threshold_minutes=max(0, int(row["offline_threshold_minutes"] or 60)),
            cooldown_seconds=max(0, int(row["cooldown_seconds"] or 3600)),
        )
    finally:
        await _release_conn(db, conn)


async def get_worker_state_last_outgoing_at(db: DBOrPool) -> Optional[datetime]:
    conn = await _acquire_conn(db)
    try:
        value = await conn.fetchval("SELECT last_outgoing_at FROM worker_state WHERE id = 1")
        return value
    finally:
        await _release_conn(db, conn)


async def get_worker_state_last_manual_outgoing_at(db: DBOrPool) -> Optional[datetime]:
    conn = await _acquire_conn(db)
    try:
        value = await conn.fetchval("SELECT last_manual_outgoing_at FROM worker_state WHERE id = 1")
        return value
    finally:
        await _release_conn(db, conn)


async def touch_worker_last_outgoing_at(db: DBOrPool) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            INSERT INTO worker_state(id, last_outgoing_at)
            VALUES (1, NOW())
            ON CONFLICT(id) DO UPDATE SET last_outgoing_at = EXCLUDED.last_outgoing_at
            """
        )
    finally:
        await _release_conn(db, conn)


async def touch_worker_last_manual_outgoing_at(db: DBOrPool) -> None:
    conn = await _acquire_conn(db)
    try:
        await conn.execute(
            """
            INSERT INTO worker_state(id, last_manual_outgoing_at)
            VALUES (1, NOW())
            ON CONFLICT(id) DO UPDATE SET last_manual_outgoing_at = EXCLUDED.last_manual_outgoing_at
            """
        )
    finally:
        await _release_conn(db, conn)


async def get_worker_autoreply_contact(db: DBOrPool, user_id: int) -> Optional[dict]:
    conn = await _acquire_conn(db)
    try:
        row = await conn.fetchrow(
            "SELECT user_id, first_seen_at, last_replied_at FROM worker_autoreply_contacts WHERE user_id = $1",
            int(user_id),
        )
        if not row:
            return None
        return {"user_id": int(row["user_id"]), "first_seen_at": row["first_seen_at"], "last_replied_at": row["last_replied_at"]}
    finally:
        await _release_conn(db, conn)


async def upsert_worker_autoreply_contact(db: DBOrPool, user_id: int, replied: bool = False) -> None:
    conn = await _acquire_conn(db)
    try:
        if replied:
            await conn.execute(
                """
                INSERT INTO worker_autoreply_contacts(user_id, first_seen_at, last_replied_at)
                VALUES ($1, NOW(), NOW())
                ON CONFLICT(user_id) DO UPDATE
                SET last_replied_at = NOW()
                """,
                int(user_id),
            )
        else:
            await conn.execute(
                """
                INSERT INTO worker_autoreply_contacts(user_id, first_seen_at)
                VALUES ($1, NOW())
                ON CONFLICT(user_id) DO NOTHING
                """,
                int(user_id),
            )
    finally:
        await _release_conn(db, conn)


async def try_mark_autoreply(db: DBOrPool, user_id: int, cooldown_seconds: int) -> bool:
    conn = await _acquire_conn(db)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO worker_autoreply_contacts (user_id, first_seen_at, last_replied_at)
            VALUES ($1, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET last_replied_at = NOW()
            WHERE worker_autoreply_contacts.last_replied_at IS NULL
               OR worker_autoreply_contacts.last_replied_at <= NOW() - ($2::int * INTERVAL '1 second')
            RETURNING CASE WHEN TRUE THEN TRUE END AS should_reply
            """,
            int(user_id),
            max(0, int(cooldown_seconds)),
        )
        return bool(row)
    finally:
        await _release_conn(db, conn)


async def get_worker_autoreply_remaining(db: DBOrPool, user_id: int, cooldown_seconds: int) -> int:
    contact = await get_worker_autoreply_contact(db, user_id=user_id)
    if not contact or not contact.get("last_replied_at"):
        return 0
    delta = datetime.now(timezone.utc) - contact["last_replied_at"]
    left = int(max(0, cooldown_seconds - delta.total_seconds()))
    return left


async def get_task_status(db: DBOrPool, *, task_id: int) -> Optional[str]:
    conn = await _acquire_conn(db)
    try:
        value = await conn.fetchval("SELECT status FROM userbot_tasks WHERE id = $1", int(task_id))
        return str(value) if value is not None else None
    finally:
        await _release_conn(db, conn)