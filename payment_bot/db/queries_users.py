from typing import Optional
import asyncpg
from payment_bot.db.pool import get_pool


# ─── USERS ────────────────────────────────────────────────────────────────────

async def get_or_create_user(telegram_id: int) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM pt_users WHERE telegram_id = $1", telegram_id
        )
        if row:
            return row
        return await conn.fetchrow(
            "INSERT INTO pt_users (telegram_id) VALUES ($1) RETURNING *",
            telegram_id
        )


async def get_user_by_telegram_id(telegram_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM pt_users WHERE telegram_id = $1", telegram_id
        )


async def set_user_role(telegram_id: int, role: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pt_users SET role = $1 WHERE telegram_id = $2",
            role, telegram_id
        )


# ─── ADMINS ───────────────────────────────────────────────────────────────────

async def get_admin_by_telegram_id(telegram_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT a.*, u.telegram_id
            FROM pt_admins a
            JOIN pt_users u ON u.id = a.user_id
            WHERE u.telegram_id = $1
            """,
            telegram_id
        )


async def create_main_admin(telegram_id: int) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            """
            INSERT INTO pt_users (telegram_id, role)
            VALUES ($1, 'main_admin')
            ON CONFLICT (telegram_id) DO UPDATE SET role = 'main_admin'
            RETURNING *
            """,
            telegram_id
        )
        admin = await conn.fetchrow(
            """
            INSERT INTO pt_admins (user_id, is_main)
            VALUES ($1, TRUE)
            ON CONFLICT (user_id) DO UPDATE SET is_main = TRUE
            RETURNING *
            """,
            user["id"]
        )
        return admin


async def create_sub_admin(telegram_id: int) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            """
            INSERT INTO pt_users (telegram_id, role)
            VALUES ($1, 'sub_admin')
            ON CONFLICT (telegram_id) DO UPDATE SET role = 'sub_admin'
            RETURNING *
            """,
            telegram_id
        )
        return await conn.fetchrow(
            """
            INSERT INTO pt_admins (user_id, is_main)
            VALUES ($1, FALSE)
            ON CONFLICT (user_id) DO NOTHING
            RETURNING *
            """,
            user["id"]
        )


async def update_admin_aggregator(
    admin_id: int,
    aggregator: str,
    aggregator_key: str,
    aggregator_data: dict
):
    pool = await get_pool()
    import json
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE pt_admins
            SET aggregator = $1, aggregator_key = $2, aggregator_data = $3
            WHERE id = $4
            """,
            aggregator, aggregator_key, json.dumps(aggregator_data), admin_id
        )


async def get_all_sub_admins_total_volume() -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                a.id,
                u.telegram_id,
                COALESCE(SUM(p.amount_usd_equivalent), 0) AS total_usd
            FROM pt_admins a
            JOIN pt_users u ON u.id = a.user_id
            LEFT JOIN pt_deals d ON d.admin_id = a.id AND d.status = 'CLOSED'
            LEFT JOIN pt_payments p ON p.deal_id = d.id AND p.status = 'confirmed'
            WHERE a.is_main = FALSE
            GROUP BY a.id, u.telegram_id
            """
        )


async def get_admin_balance(admin_id: int) -> float:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(p.amount_usd_equivalent), 0) AS total
            FROM pt_payments p
            JOIN pt_deals d ON d.id = p.deal_id
            WHERE d.admin_id = $1
              AND d.status = 'CLOSED'
              AND p.status = 'confirmed'
            """,
            admin_id
        )
        return float(row["total"])
