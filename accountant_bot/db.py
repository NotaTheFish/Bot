from __future__ import annotations

from typing import Any, Optional, Sequence

import asyncpg


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)


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
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id BIGSERIAL PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                review_key TEXT NOT NULL,
                source_chat_id BIGINT,
                source_message_id BIGINT,
                message_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
                review_text TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                deleted_at TIMESTAMPTZ
            )
            """
        )
        await conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_reviews_channel_review_key
            ON reviews(channel_id, review_key)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reviews_channel_deleted_at
            ON reviews(channel_id, deleted_at)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS gin_reviews_message_ids
            ON reviews USING GIN(message_ids)
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id BIGSERIAL PRIMARY KEY,
                review_id BIGINT REFERENCES reviews(id) ON DELETE SET NULL,
                admin_id BIGINT NOT NULL,
                amount_kopecks BIGINT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'RUB',
                note TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_created_at
            ON transactions(created_at)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_admin_id
            ON transactions(admin_id)
            """
        )
        await conn.execute(
            """
            ALTER TABLE transactions
            ADD COLUMN IF NOT EXISTS item TEXT,
            ADD COLUMN IF NOT EXISTS qty NUMERIC,
            ADD COLUMN IF NOT EXISTS unit_price NUMERIC,
            ADD COLUMN IF NOT EXISTS total NUMERIC,
            ADD COLUMN IF NOT EXISTS pay_method TEXT,
            ADD COLUMN IF NOT EXISTS receipt_file_id TEXT
            """
        )


async def insert_review(
    pool: asyncpg.Pool,
    *,
    channel_id: int,
    review_key: str,
    message_ids: Sequence[int],
    source_chat_id: Optional[int] = None,
    source_message_id: Optional[int] = None,
    review_text: Optional[str] = None,
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO reviews (
                channel_id,
                review_key,
                source_chat_id,
                source_message_id,
                message_ids,
                review_text
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (channel_id, review_key)
            DO UPDATE
            SET source_chat_id = EXCLUDED.source_chat_id,
                source_message_id = EXCLUDED.source_message_id,
                message_ids = EXCLUDED.message_ids,
                review_text = EXCLUDED.review_text,
                updated_at = NOW(),
                deleted_at = NULL
            RETURNING *
            """,
            int(channel_id),
            str(review_key),
            source_chat_id,
            source_message_id,
            [int(message_id) for message_id in message_ids],
            review_text,
        )


async def fetch_review_by_key(
    pool: asyncpg.Pool,
    *,
    channel_id: int,
    review_key: str,
    include_deleted: bool = False,
) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        if include_deleted:
            return await conn.fetchrow(
                """
                SELECT *
                FROM reviews
                WHERE channel_id = $1 AND review_key = $2
                """,
                int(channel_id),
                str(review_key),
            )
        return await conn.fetchrow(
            """
            SELECT *
            FROM reviews
            WHERE channel_id = $1
              AND review_key = $2
              AND deleted_at IS NULL
            """,
            int(channel_id),
            str(review_key),
        )


async def fetch_reviews_by_message_id(
    pool: asyncpg.Pool,
    *,
    channel_id: int,
    message_id: int,
    include_deleted: bool = False,
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        if include_deleted:
            rows = await conn.fetch(
                """
                SELECT *
                FROM reviews
                WHERE channel_id = $1
                  AND message_ids @> ARRAY[$2]::bigint[]
                ORDER BY created_at DESC, id DESC
                """,
                int(channel_id),
                int(message_id),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM reviews
                WHERE channel_id = $1
                  AND deleted_at IS NULL
                  AND message_ids @> ARRAY[$2]::bigint[]
                ORDER BY created_at DESC, id DESC
                """,
                int(channel_id),
                int(message_id),
            )
    return list(rows)


async def insert_transaction(
    pool: asyncpg.Pool,
    *,
    admin_id: int,
    amount_kopecks: int,
    review_id: Optional[int] = None,
    currency: str = "RUB",
    note: Optional[str] = None,
    item: Optional[str] = None,
    qty: Optional[str] = None,
    unit_price: Optional[str] = None,
    total: Optional[str] = None,
    pay_method: Optional[str] = None,
    receipt_file_id: Optional[str] = None,
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO transactions (
                review_id,
                admin_id,
                amount_kopecks,
                currency,
                note,
                item,
                qty,
                unit_price,
                total,
                pay_method,
                receipt_file_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *
            """,
            review_id,
            int(admin_id),
            int(amount_kopecks),
            str(currency),
            note,
            item,
            qty,
            unit_price,
            total,
            pay_method,
            receipt_file_id,
        )


async def fetch_transactions(
    pool: asyncpg.Pool,
    *,
    admin_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        if admin_id is None:
            rows = await conn.fetch(
                """
                SELECT *
                FROM transactions
                ORDER BY created_at DESC, id DESC
                LIMIT $1 OFFSET $2
                """,
                int(limit),
                int(offset),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM transactions
                WHERE admin_id = $1
                ORDER BY created_at DESC, id DESC
                LIMIT $2 OFFSET $3
                """,
                int(admin_id),
                int(limit),
                int(offset),
            )
    return list(rows)


async def fetch_transaction_by_id(pool: asyncpg.Pool, *, transaction_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT *
            FROM transactions
            WHERE id = $1
            """,
            int(transaction_id),
        )