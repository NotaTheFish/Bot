from __future__ import annotations

from datetime import datetime
import logging
import re
from typing import Any, Optional, Sequence
from zoneinfo import ZoneInfo

import asyncpg

from .time_ranges import DateRange, period_to_utc_range


logger = logging.getLogger(__name__)


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=5)


async def acquire_singleton_lock(pool: asyncpg.Pool, key: int) -> asyncpg.Connection | None:
    conn = await pool.acquire()
    try:
        acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1)", key)
        if acquired:
            return conn
    except Exception:
        await pool.release(conn)
        raise
    await pool.release(conn)
    return None


async def release_singleton_lock(pool: asyncpg.Pool, conn: asyncpg.Connection | None, key: int) -> None:
    if conn is None:
        return
    try:
        await conn.execute("SELECT pg_advisory_unlock($1)", key)
    except Exception:
        pass
    await pool.release(conn)


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
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_catalog (
                id BIGSERIAL PRIMARY KEY,
                category_code TEXT NOT NULL,
                name TEXT NOT NULL,
                name_norm TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ux_product_catalog_category_name_norm UNIQUE (category_code, name_norm)
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accountant_bot_migrations (
                key TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews_listener_state (
                channel_id BIGINT PRIMARY KEY,
                last_message_id BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_product_catalog_category_name_norm
            ON product_catalog(category_code, name_norm)
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

        migration_key = "normalize_item_names_v1"
        async with conn.transaction():
            already_applied = await conn.fetchval(
                """
                SELECT 1
                FROM accountant_bot_migrations
                WHERE key = $1
                """,
                migration_key,
            )
            if already_applied:
                logger.info("Data migration %s already applied, skipping", migration_key)
            else:
                rules = (
                    ("Макс рост", "Max Growth Token"),
                    ("Ревы", "Revive Token"),
                    ("Частицы", "Partial Growth Token"),
                )
                total_updated = 0
                for old_name, new_name in rules:
                    result = await conn.execute(
                        """
                        UPDATE receipt_items
                        SET item_name = $1
                        WHERE btrim(item_name) = $2
                        """,
                        new_name,
                        old_name,
                    )
                    updated_count = int(result.split()[-1])
                    total_updated += updated_count
                    logger.info(
                        "Data migration %s: updated %s rows (%r -> %r)",
                        migration_key,
                        updated_count,
                        old_name,
                        new_name,
                    )

                await conn.execute(
                    """
                    INSERT INTO accountant_bot_migrations(key)
                    VALUES ($1)
                    """,
                    migration_key,
                )
                logger.info(
                    "Data migration %s applied successfully, total rows updated: %s",
                    migration_key,
                    total_updated,
                )


async def init_db(pool: asyncpg.Pool) -> None:
    """Backward-compatible wrapper for schema initialization."""
    await ensure_schema(pool)


def normalize_product_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


async def add_product(pool: asyncpg.Pool, category_code: str, name: str) -> dict[str, Any]:
    normalized_name = normalize_product_name(name)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO product_catalog (category_code, name, name_norm)
            VALUES ($1, $2, $3)
            ON CONFLICT (category_code, name_norm)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id, category_code, name
            """,
            str(category_code),
            str(name).strip(),
            normalized_name,
        )
    return dict(row) if row else {}


async def delete_product(pool: asyncpg.Pool, category_code: str, product_id: int) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM product_catalog
            WHERE category_code = $1
              AND id = $2
            """,
            str(category_code),
            int(product_id),
        )
    return result.endswith("1")


async def list_products(pool: asyncpg.Pool, category_code: str, offset: int, limit: int) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, category_code, name
            FROM product_catalog
            WHERE category_code = $1
            ORDER BY name_norm ASC
            OFFSET $2 LIMIT $3
            """,
            str(category_code),
            int(offset),
            int(limit),
        )
    return [dict(row) for row in rows]


async def count_products(pool: asyncpg.Pool, category_code: str) -> int:
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM product_catalog
                WHERE category_code = $1
                """,
                str(category_code),
            )
            or 0
        )


async def search_products(pool: asyncpg.Pool, category_code: str, query: str, limit: int = 20) -> list[dict[str, Any]]:
    q_norm = normalize_product_name(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, category_code, name
            FROM product_catalog
            WHERE category_code = $1
              AND name_norm LIKE '%' || $2 || '%'
            ORDER BY name_norm ASC
            LIMIT $3
            """,
            str(category_code),
            q_norm,
            int(limit),
        )
    return [dict(row) for row in rows]


async def get_product(pool: asyncpg.Pool, category_code: str, product_id: int) -> Optional[dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, category_code, name
            FROM product_catalog
            WHERE category_code = $1
              AND id = $2
            """,
            str(category_code),
            int(product_id),
        )
    return dict(row) if row else None


async def get_reviews_checkpoint(pool: asyncpg.Pool, channel_id: int) -> int:
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT last_message_id
            FROM reviews_listener_state
            WHERE channel_id = $1
            """,
            int(channel_id),
        )
    return int(value or 0)


async def set_reviews_checkpoint(pool: asyncpg.Pool, channel_id: int, last_message_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO reviews_listener_state(channel_id, last_message_id, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (channel_id)
            DO UPDATE SET
                last_message_id = EXCLUDED.last_message_id,
                updated_at = NOW()
            """,
            int(channel_id),
            int(last_message_id),
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


async def list_receipts_by_period(
    pool: asyncpg.Pool,
    *,
    date_range: DateRange | None = None,
    period: str | None = None,
    tz: ZoneInfo | None = None,
) -> list[asyncpg.Record]:
    if date_range is None:
        if period is None or tz is None:
            raise ValueError("Either date_range or both period and tz are required")
        date_range = period_to_utc_range(period, tz)
    async with pool.acquire() as conn:
        if date_range.start is None:
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
                  AND created_at <= $2
                ORDER BY created_at DESC, id DESC
                """,
                date_range.start,
                date_range.end,
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