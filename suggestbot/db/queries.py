from typing import Any
import asyncpg
from config import TABLE_PREFIX
from db.database import get_pool

p = TABLE_PREFIX


# ─── USERS ────────────────────────────────────────────────────────────────────

async def upsert_user(user_id: int, username: str | None, first_name: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {p}users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name
        """, user_id, username, first_name)


async def get_user(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(f"SELECT * FROM {p}users WHERE user_id = $1", user_id)


async def ban_user(user_id: int, reason: str, banned_by: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE {p}users
            SET is_banned = TRUE, ban_reason = $2, banned_by = $3, banned_at = NOW()
            WHERE user_id = $1
        """, user_id, reason, banned_by)


async def unban_user(user_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE {p}users
            SET is_banned = FALSE, ban_reason = NULL, banned_by = NULL, banned_at = NULL
            WHERE user_id = $1
        """, user_id)


async def get_banned_users() -> list[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
            SELECT user_id, username, first_name, ban_reason, banned_at
            FROM {p}users WHERE is_banned = TRUE ORDER BY banned_at DESC
        """)


# ─── SUBMISSIONS ──────────────────────────────────────────────────────────────

async def create_submission(user_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            INSERT INTO {p}submissions (user_id) VALUES ($1) RETURNING id
        """, user_id)
        return row["id"]


async def add_content(
    submission_id: int,
    content_type: str,
    file_id: str | None,
    caption: str | None,
    sort_order: int = 0
) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {p}submission_content (submission_id, content_type, file_id, caption, sort_order)
            VALUES ($1, $2, $3, $4, $5)
        """, submission_id, content_type, file_id, caption, sort_order)


async def get_submission(submission_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(f"SELECT * FROM {p}submissions WHERE id = $1", submission_id)


async def get_submission_content(submission_id: int) -> list[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
            SELECT * FROM {p}submission_content
            WHERE submission_id = $1
            ORDER BY sort_order
        """, submission_id)


async def update_submission_status(
    submission_id: int,
    status: str,
    handled_by: int | None = None
) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE {p}submissions
            SET status = $2, handled_by = $3, updated_at = NOW()
            WHERE id = $1
        """, submission_id, status, handled_by)


async def save_admin_msg(submission_id: int, msg_id: int, chat_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE {p}submissions SET admin_msg_id = $2, admin_chat_id = $3 WHERE id = $1
        """, submission_id, msg_id, chat_id)


async def save_source_messages(submission_id: int, chat_id: int, message_ids: list[int]) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE {p}submissions
            SET source_chat_id = $2, source_message_ids = $3
            WHERE id = $1
        """, submission_id, chat_id, message_ids)


async def save_channel_msg(submission_id: int, msg_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE {p}submissions SET channel_msg_id = $2 WHERE id = $1
        """, submission_id, msg_id)


async def get_pending_submissions() -> list[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(f"""
            SELECT s.*, u.username, u.first_name
            FROM {p}submissions s
            JOIN {p}users u ON s.user_id = u.user_id
            WHERE s.status = 'pending'
            ORDER BY s.created_at ASC
        """)


# ─── SETTINGS ─────────────────────────────────────────────────────────────────

async def get_setting(key: str) -> str | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT value FROM {p}settings WHERE key = $1", key)
        return row["value"] if row else None


async def set_setting(key: str, value: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {p}settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, key, value)
