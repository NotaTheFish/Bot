from __future__ import annotations

from decimal import Decimal
from datetime import timezone
from typing import Any, Optional

import asyncpg

from .db import create_receipt_with_items as db_create_receipt_with_items
from .db import insert_transaction as db_insert_transaction
from .time_ranges import period_to_utc_range


async def add_transaction(
    pool: asyncpg.Pool,
    *,
    admin_id: int,
    amount_kopecks: int,
    review_id: Optional[int] = None,
    currency: str = "RUB",
    note: Optional[str] = None,
    item: Optional[str] = None,
    qty: Optional[Decimal] = None,
    unit_price: Optional[Decimal] = None,
    total: Optional[Decimal] = None,
    pay_method: Optional[str] = None,
    receipt_file_id: Optional[str] = None,
) -> asyncpg.Record:
    return await db_insert_transaction(
        pool,
        admin_id=admin_id,
        amount_kopecks=amount_kopecks,
        review_id=review_id,
        currency=currency,
        note=note,
        item=item,
        qty=None if qty is None else str(qty),
        unit_price=None if unit_price is None else str(unit_price),
        total=None if total is None else str(total),
        pay_method=pay_method,
        receipt_file_id=receipt_file_id,
    )


async def add_receipt_with_items(
    pool: asyncpg.Pool,
    *,
    admin_id: int,
    currency: str,
    pay_method: Optional[str],
    note: Optional[str],
    receipt_file_id: Optional[str],
    receipt_file_type: Optional[str],
    items: list[dict[str, Optional[str]]],
) -> dict[str, Any]:
    return await db_create_receipt_with_items(
        pool,
        admin_id=admin_id,
        currency=currency,
        pay_method=pay_method,
        note=note,
        receipt_file_id=receipt_file_id,
        receipt_file_type=receipt_file_type,
        items=items,
    )


async def list_transactions_by_period(pool: asyncpg.Pool, *, period: str) -> list[asyncpg.Record]:
    date_range = period_to_utc_range(period, timezone.utc)
    async with pool.acquire() as conn:
        if date_range.start is None:
            rows = await conn.fetch("SELECT * FROM transactions ORDER BY created_at DESC, id DESC")
        else:
            rows = await conn.fetch(
                """
                SELECT *
                FROM transactions
                WHERE created_at >= $1
                  AND created_at <= $2
                ORDER BY created_at DESC, id DESC
                """,
                date_range.start,
                date_range.end,
            )
    return list(rows)


def to_excel_rows(rows: list[dict[str, Any] | asyncpg.Record]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        record = dict(row)
        result.append(
            {
                "id": record.get("id"),
                "created_at": record.get("created_at"),
                "admin_id": record.get("admin_id"),
                "item": record.get("item"),
                "qty": record.get("qty"),
                "unit_price": record.get("unit_price"),
                "total": record.get("total"),
                "currency": record.get("currency"),
                "pay_method": record.get("pay_method"),
                "note": record.get("note"),
                "receipt_file_id": record.get("receipt_file_id"),
                "amount_kopecks": record.get("amount_kopecks"),
            }
        )
    return result