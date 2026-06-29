from typing import Optional
import asyncpg
from payment_bot.db.pool import get_pool


# ─── DEALS ────────────────────────────────────────────────────────────────────

async def create_deal(admin_id: int, base_price_usd: float, invite_token: str, expires_at) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO pt_deals (admin_id, invite_token, base_price_usd, expires_at)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            """,
            admin_id, invite_token, base_price_usd, expires_at
        )


async def get_deal_by_token(token: str) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM pt_deals WHERE invite_token = $1", token
        )


async def get_deal_by_id(deal_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM pt_deals WHERE id = $1", deal_id
        )


async def activate_deal_token(deal_id: int, client_user_id: int) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            UPDATE pt_deals
            SET client_id = $1, status = 'WAITING_PAYMENT'
            WHERE id = $2
            RETURNING *
            """,
            client_user_id, deal_id
        )


async def set_deal_currency(deal_id: int, currency: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pt_deals SET selected_currency = $1 WHERE id = $2",
            currency, deal_id
        )


async def set_deal_paid(deal_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pt_deals SET status = 'PAID' WHERE id = $1", deal_id
        )


async def close_deal(deal_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pt_deals SET status = 'CLOSED', closed_at = NOW() WHERE id = $1",
            deal_id
        )


async def expire_overdue_deals() -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            UPDATE pt_deals
            SET status = 'EXPIRED'
            WHERE status IN ('CREATED', 'WAITING_PAYMENT')
              AND expires_at < NOW()
            RETURNING *
            """
        )


async def get_admin_deals(admin_id: int, status: Optional[str] = None) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if status:
            return await conn.fetch(
                "SELECT * FROM pt_deals WHERE admin_id = $1 AND status = $2 ORDER BY created_at DESC",
                admin_id, status
            )
        return await conn.fetch(
            "SELECT * FROM pt_deals WHERE admin_id = $1 ORDER BY created_at DESC LIMIT 50",
            admin_id
        )


async def get_active_deal_for_client(client_user_id: int) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT * FROM pt_deals
            WHERE client_id = $1 AND status = 'WAITING_PAYMENT'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            client_user_id
        )


# ─── PAYMENTS ─────────────────────────────────────────────────────────────────

async def create_payment(
    deal_id: int,
    provider: str,
    amount_original: float,
    currency: str,
    amount_usd_equivalent: float,
    idempotency_key: str,
) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO pt_payments
                (deal_id, provider, amount_original, currency, amount_usd_equivalent, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            """,
            deal_id, provider, amount_original, currency, amount_usd_equivalent, idempotency_key
        )


async def confirm_payment(idempotency_key: str, external_tx_id: str) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT * FROM pt_payments WHERE idempotency_key = $1", idempotency_key
        )
        if not existing:
            return None
        if existing["status"] == "confirmed":
            return existing  # idempotent — already processed
        return await conn.fetchrow(
            """
            UPDATE pt_payments
            SET status = 'confirmed', external_tx_id = $1
            WHERE idempotency_key = $2
            RETURNING *
            """,
            external_tx_id, idempotency_key
        )


async def fail_payment(idempotency_key: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE pt_payments SET status = 'failed' WHERE idempotency_key = $1",
            idempotency_key
        )


async def get_payment_by_external_tx(external_tx_id: str) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM pt_payments WHERE external_tx_id = $1", external_tx_id
        )


# ─── CURRENCY RATES ───────────────────────────────────────────────────────────

async def get_rate(currency: str) -> Optional[float]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT usd_rate FROM pt_currency_rates WHERE currency = $1", currency
        )
        return float(row["usd_rate"]) if row else None


async def update_rates(rates: dict):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for currency, rate in rates.items():
                await conn.execute(
                    """
                    INSERT INTO pt_currency_rates (currency, usd_rate, updated_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (currency) DO UPDATE SET usd_rate = $2, updated_at = NOW()
                    """,
                    currency, rate
                )


async def get_all_rates() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT currency, usd_rate FROM pt_currency_rates")
        return {r["currency"]: float(r["usd_rate"]) for r in rows}


# ─── ADMIN KEYS ───────────────────────────────────────────────────────────────

async def create_admin_key(created_by_admin_id: int, key_token: str, expires_at) -> asyncpg.Record:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO pt_admin_keys (key_token, created_by, expires_at)
            VALUES ($1, $2, $3)
            RETURNING *
            """,
            key_token, created_by_admin_id, expires_at
        )


async def get_admin_key(key_token: str) -> Optional[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM pt_admin_keys WHERE key_token = $1", key_token
        )


async def consume_admin_key(key_token: str, used_by_user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE pt_admin_keys
            SET used_by = $1, used_at = NOW()
            WHERE key_token = $2
            """,
            used_by_user_id, key_token
        )


# ─── LOGS ─────────────────────────────────────────────────────────────────────

async def log_event(
    event_type: str,
    payload: dict,
    deal_id: Optional[int] = None,
    admin_id: Optional[int] = None,
):
    pool = await get_pool()
    import json
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO pt_logs (deal_id, admin_id, event_type, payload)
            VALUES ($1, $2, $3, $4)
            """,
            deal_id, admin_id, event_type, json.dumps(payload)
        )
