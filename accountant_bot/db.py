from __future__ import annotations

import asyncpg


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=database_url)


async def init_db(pool: asyncpg.Pool) -> None:
    """Initialize database structures required by accountant bot."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accountant_bot_healthcheck (
                id SMALLINT PRIMARY KEY DEFAULT 1,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )