import asyncpg
from config import DATABASE_URL

_pool: asyncpg.Pool | None = None


async def init():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10, command_timeout=30)


async def close():
    if _pool:
        await _pool.close()


def pool() -> asyncpg.Pool:
    assert _pool is not None, "db.init() не вызван"
    return _pool


# ======================= ДЕНЬГИ =======================
# Единственная точка, где меняется баланс. Больше нигде UPDATE rb_balances не пишем.

async def apply(conn, tg_id: int, currency: str, delta: int,
                reason: str, idem: str, ref_id: int | None = None) -> int | None:
    """
    Начислить/списать. idem — ключ идемпотентности, повтор просто ничего не сделает.
    Возвращает новый баланс или None, если проводка уже была.
    Бросает asyncpg.CheckViolationError при попытке уйти в минус.
    ВСЕГДА вызывать внутри transaction().
    """
    exists = await conn.fetchval("SELECT 1 FROM rb_ledger WHERE idempotency_key = $1", idem)
    if exists:
        return None

    new_balance = await conn.fetchval(
        """
        INSERT INTO rb_balances (tg_id, currency, amount)
        VALUES ($1, $2, GREATEST($3, 0))
        ON CONFLICT (tg_id, currency)
        DO UPDATE SET amount = rb_balances.amount + $3
        RETURNING amount
        """,
        tg_id, currency, delta,
    )
    await conn.execute(
        """
        INSERT INTO rb_ledger (tg_id, currency, delta, balance_after, reason, ref_id, idempotency_key)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        tg_id, currency, delta, new_balance, reason, ref_id, idem,
    )
    return new_balance


async def balance(tg_id: int, currency: str) -> int:
    return await pool().fetchval(
        "SELECT COALESCE(amount, 0) FROM rb_balances WHERE tg_id = $1 AND currency = $2",
        tg_id, currency,
    ) or 0


async def balances(tg_id: int) -> dict[str, int]:
    rows = await pool().fetch("SELECT currency, amount FROM rb_balances WHERE tg_id = $1", tg_id)
    out = {"mushrooms": 0, "coins": 0}
    for r in rows:
        out[r["currency"]] = r["amount"]
    return out


# ======================= ПОЛЬЗОВАТЕЛИ =======================

async def upsert_user(tg_id: int, username: str | None, first_name: str | None):
    await pool().execute(
        """
        INSERT INTO rb_users (tg_id, username, first_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (tg_id) DO UPDATE
        SET username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_seen = now()
        """,
        tg_id, username, first_name,
    )


async def get_user(tg_id: int):
    return await pool().fetchrow("SELECT * FROM rb_users WHERE tg_id = $1", tg_id)


async def is_banned(tg_id: int) -> bool:
    return bool(await pool().fetchval("SELECT banned FROM rb_users WHERE tg_id = $1", tg_id))


async def is_admin(chat_id: int, tg_id: int) -> bool:
    return bool(await pool().fetchval(
        "SELECT 1 FROM rb_admins WHERE chat_id = $1 AND tg_id = $2", chat_id, tg_id))


async def admin_chats(tg_id: int) -> list[int]:
    rows = await pool().fetch("SELECT chat_id FROM rb_admins WHERE tg_id = $1", tg_id)
    return [r["chat_id"] for r in rows]


EXPECTED_TABLES = [
    "rb_users", "rb_chats", "rb_admins", "rb_balances", "rb_ledger", "rb_ref_links",
    "rb_invites", "rb_targets", "rb_referrals", "rb_withdrawals", "rb_spins", "rb_audit",
    "rb_settings", "rb_free_budget", "rb_free_chats",
    "rb_contest_chats", "rb_week_msgs", "rb_week_draws",
]


async def check_schema() -> list[str]:
    """
    Какие таблицы отсутствуют. Зовётся на старте и ОРЁТ в лог.
    Иначе недостающая таблица проявляется как молча не работающая команда —
    ровно так и потерялся /шимшайнуть.
    """
    rows = await pool().fetch(
        "SELECT tablename FROM pg_tables WHERE tablename = ANY($1::text[])", EXPECTED_TABLES)
    have = {r["tablename"] for r in rows}
    return [t for t in EXPECTED_TABLES if t not in have]


async def audit(actor_id: int | None, action: str, payload: dict):
    import json
    await pool().execute(
        "INSERT INTO rb_audit (actor_id, action, payload) VALUES ($1, $2, $3::jsonb)",
        actor_id, action, json.dumps(payload, ensure_ascii=False),
    )
