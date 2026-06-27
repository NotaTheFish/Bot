import os
import asyncpg
from typing import Optional

DATABASE_URL = os.getenv("DATABASE_URL")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                key TEXT NOT NULL UNIQUE,
                announcement TEXT NOT NULL,
                prize_places INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS prizes (
                id SERIAL PRIMARY KEY,
                giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
                place INTEGER NOT NULL,
                description TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
                chat_id BIGINT NOT NULL,
                chat_title TEXT,
                invite_link TEXT
            );

            CREATE TABLE IF NOT EXISTS participants (
                id SERIAL PRIMARY KEY,
                giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(giveaway_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS winners (
                id SERIAL PRIMARY KEY,
                giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                place INTEGER NOT NULL,
                prize TEXT NOT NULL
            );
        """)


# ── Giveaways ─────────────────────────────────────────────────────────────────

async def create_giveaway(title: str, key: str, announcement: str, prize_places: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO giveaways (title, key, announcement, prize_places) VALUES ($1, $2, $3, $4) RETURNING id",
            title, key, announcement, prize_places
        )
        return row['id']


async def get_giveaway_by_id(giveaway_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM giveaways WHERE id = $1", giveaway_id)
        return dict(row) if row else None


async def get_giveaway_by_key(key: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM giveaways WHERE key = $1 AND status = 'active'", key
        )
        return dict(row) if row else None


async def get_all_giveaways() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM giveaways ORDER BY created_at DESC")
        return [dict(r) for r in rows]


async def get_active_giveaways() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM giveaways WHERE status = 'active' ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]


async def update_giveaway_status(giveaway_id: int, status: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE giveaways SET status = $1 WHERE id = $2", status, giveaway_id
        )


async def key_exists(key: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM giveaways WHERE key = $1", key)
        return row is not None


# ── Prizes ────────────────────────────────────────────────────────────────────

async def add_prize(giveaway_id: int, place: int, description: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO prizes (giveaway_id, place, description) VALUES ($1, $2, $3)",
            giveaway_id, place, description
        )


async def get_prizes(giveaway_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM prizes WHERE giveaway_id = $1 ORDER BY place", giveaway_id
        )
        return [dict(r) for r in rows]


# ── Channels ──────────────────────────────────────────────────────────────────

async def add_channel(giveaway_id: int, chat_id: int, chat_title: str, invite_link: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO channels (giveaway_id, chat_id, chat_title, invite_link) VALUES ($1, $2, $3, $4)",
            giveaway_id, chat_id, chat_title, invite_link
        )


async def get_channels(giveaway_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM channels WHERE giveaway_id = $1", giveaway_id
        )
        return [dict(r) for r in rows]


# ── Participants ──────────────────────────────────────────────────────────────

async def add_participant(giveaway_id: int, user_id: int, username: str, full_name: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO participants (giveaway_id, user_id, username, full_name) VALUES ($1, $2, $3, $4)",
                giveaway_id, user_id, username, full_name
            )
            return True
        except asyncpg.UniqueViolationError:
            return False


async def get_participants(giveaway_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM participants WHERE giveaway_id = $1", giveaway_id
        )
        return [dict(r) for r in rows]


async def get_participant_count(giveaway_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM participants WHERE giveaway_id = $1", giveaway_id
        )
        return row['cnt']


# ── Delete ───────────────────────────────────────────────────────────────────

async def delete_giveaway(giveaway_id: int):
    """Hard delete giveaway and all related data (CASCADE handles the rest)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM giveaways WHERE id = $1", giveaway_id)


# ── Winners ───────────────────────────────────────────────────────────────────

async def add_winner(giveaway_id: int, user_id: int, username: str, full_name: str, place: int, prize: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO winners (giveaway_id, user_id, username, full_name, place, prize) VALUES ($1, $2, $3, $4, $5, $6)",
            giveaway_id, user_id, username, full_name, place, prize
        )


async def get_winners(giveaway_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM winners WHERE giveaway_id = $1 ORDER BY place", giveaway_id
        )
        return [dict(r) for r in rows]
