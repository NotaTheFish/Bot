from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence

import asyncpg


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)


async def ensure_schema(pool: asyncpg.Pool) -> None:
    """Ensure database schema exists and is compatible with current bot version."""
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

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipts (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                admin_id BIGINT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'RUB',
                pay_method TEXT,
                note TEXT,
                receipt_file_id TEXT,
                receipt_file_type TEXT,
                status TEXT NOT NULL DEFAULT 'created',
                canceled_at TIMESTAMPTZ,
                refunded_at TIMESTAMPTZ
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipt_items (
                id BIGSERIAL PRIMARY KEY,
                receipt_id BIGINT NOT NULL REFERENCES receipts(id) ON DELETE CASCADE,
                category TEXT,
                item_name TEXT NOT NULL,
                qty NUMERIC,
                unit_price NUMERIC,
                unit_basis TEXT,
                line_total NUMERIC,
                note TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipts_created_at_desc
            ON receipts(created_at DESC)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipts_admin_id_created_at_desc
            ON receipts(admin_id, created_at DESC)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipt_items_receipt_id
            ON receipt_items(receipt_id)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipt_items_category
            ON receipt_items(category)
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipt_items_created_at_desc
            ON receipt_items(created_at DESC)
            """
        )

        # Backward-compatible migration for environments where table may have
        # been created manually with partial set of columns.
        for alter_sql in (
            "ALTER TABLE receipts ADD COLUMN IF NOT EXISTS receipt_file_type TEXT",
            "ALTER TABLE receipts ADD COLUMN IF NOT EXISTS status TEXT",
            "ALTER TABLE receipts ADD COLUMN IF NOT EXISTS canceled_at TIMESTAMPTZ",
            "ALTER TABLE receipts ADD COLUMN IF NOT EXISTS refunded_at TIMESTAMPTZ",
            "ALTER TABLE receipt_items ADD COLUMN IF NOT EXISTS unit_basis TEXT",
            "ALTER TABLE receipt_items ADD COLUMN IF NOT EXISTS line_total NUMERIC",
        ):
            try:
                await conn.execute(alter_sql)
            except Exception:
                # Keep startup resilient and idempotent when old inconsistent
                # schemas are encountered.
                pass


async def init_db(pool: asyncpg.Pool) -> None:
    """Backward-compatible wrapper for schema initialization."""
    await ensure_schema(pool)


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


def _period_start(period: str) -> Optional[datetime]:
    period_normalized = (period or "").strip().lower()
    now = datetime.now(timezone.utc)

    if period_normalized == "all":
        return None
    if period_normalized in {"day", "1day", "today"}:
        return now - timedelta(days=1)
    if period_normalized in {"7days", "week"}:
        return now - timedelta(days=7)
    if period_normalized in {"30days", "month"}:
        return now - timedelta(days=30)

    raise ValueError("unsupported period")


async def insert_receipt(
    pool: asyncpg.Pool,
    *,
    admin_id: int,
    currency: str = "RUB",
    pay_method: Optional[str] = None,
    note: Optional[str] = None,
    receipt_file_id: Optional[str] = None,
    receipt_file_type: Optional[str] = None,
    status: str = "created",
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO receipts (
                admin_id,
                currency,
                pay_method,
                note,
                receipt_file_id,
                receipt_file_type,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
            """,
            int(admin_id),
            str(currency),
            pay_method,
            note,
            receipt_file_id,
            receipt_file_type,
            str(status),
        )


async def insert_receipt_item(
    pool: asyncpg.Pool,
    *,
    receipt_id: int,
    item_name: str,
    category: Optional[str] = None,
    qty: Optional[str] = None,
    unit_price: Optional[str] = None,
    unit_basis: Optional[str] = None,
    line_total: Optional[str] = None,
    note: Optional[str] = None,
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO receipt_items (
                receipt_id,
                category,
                item_name,
                qty,
                unit_price,
                unit_basis,
                line_total,
                note
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
            """,
            int(receipt_id),
            category,
            str(item_name),
            qty,
            unit_price,
            unit_basis,
            line_total,
            note,
        )




async def create_receipt_with_items(
    pool: asyncpg.Pool,
    *,
    admin_id: int,
    currency: str = "RUB",
    pay_method: Optional[str] = None,
    note: Optional[str] = None,
    receipt_file_id: Optional[str] = None,
    receipt_file_type: Optional[str] = None,
    items: list[dict[str, Optional[str]]],
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            receipt = await conn.fetchrow(
                """
                INSERT INTO receipts (
                    admin_id,
                    currency,
                    pay_method,
                    note,
                    receipt_file_id,
                    receipt_file_type,
                    status
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'created')
                RETURNING *
                """,
                int(admin_id),
                str(currency),
                pay_method,
                note,
                receipt_file_id,
                receipt_file_type,
            )

            saved_items: list[asyncpg.Record] = []
            for item in items:
                row = await conn.fetchrow(
                    """
                    INSERT INTO receipt_items (
                        receipt_id,
                        category,
                        item_name,
                        qty,
                        unit_price,
                        unit_basis,
                        line_total,
                        note
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING *
                    """,
                    int(receipt["id"]),
                    item.get("category"),
                    str(item.get("item_name") or ""),
                    item.get("qty"),
                    item.get("unit_price"),
                    item.get("unit_basis"),
                    item.get("line_total"),
                    item.get("note"),
                )
                saved_items.append(row)

    return {"receipt": receipt, "items": saved_items}


async def get_receipt_with_items(pool: asyncpg.Pool, receipt_id: int) -> Optional[dict[str, Any]]:
    async with pool.acquire() as conn:
        receipt = await conn.fetchrow(
            """
            SELECT *
            FROM receipts
            WHERE id = $1
            """,
            int(receipt_id),
        )
        if receipt is None:
            return None

        items = await conn.fetch(
            """
            SELECT *
            FROM receipt_items
            WHERE receipt_id = $1
            ORDER BY created_at ASC, id ASC
            """,
            int(receipt_id),
        )

    return {"receipt": receipt, "items": list(items)}


async def list_receipts_by_period(pool: asyncpg.Pool, *, period: str) -> list[asyncpg.Record]:
    start = _period_start(period)
    async with pool.acquire() as conn:
        if start is None:
            rows = await conn.fetch(
                """
                SELECT *
                FROM receipts
                ORDER BY created_at DESC, id DESC
                """
            )
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM receipts
                WHERE created_at >= $1
                ORDER BY created_at DESC, id DESC
                """,
                start,
            )
    return list(rows)


async def cancel_receipt(pool: asyncpg.Pool, *, receipt_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            UPDATE receipts
            SET status = 'canceled',
                canceled_at = NOW()
            WHERE id = $1
            RETURNING *
            """,
            int(receipt_id),
        )


async def refund_receipt(pool: asyncpg.Pool, *, receipt_id: int) -> Optional[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            UPDATE receipts
            SET status = 'refunded',
                refunded_at = NOW()
            WHERE id = $1
            RETURNING *
            """,
            int(receipt_id),
        )