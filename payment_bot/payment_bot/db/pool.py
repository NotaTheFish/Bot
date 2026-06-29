import asyncpg
from typing import Optional
from payment_bot.config import settings

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=settings.DATABASE_URL,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def run_migrations():
    pool = await get_pool()
    with open("payment_bot/db/migrations.sql") as f:
        sql = f.read()
    async with pool.acquire() as conn:
        await conn.execute(sql)
