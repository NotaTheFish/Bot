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


async def banned_users(limit: int = 100):
    return await pool().fetch(
        "SELECT tg_id, username, first_name, ban_reason, banned_at FROM rb_users "
        "WHERE banned ORDER BY banned_at DESC NULLS LAST LIMIT $1", limit)


async def set_ban(tg_id: int, reason: str, by: int):
    await pool().execute(
        "UPDATE rb_users SET banned=TRUE, ban_reason=$1, banned_by=$2, banned_at=now() "
        "WHERE tg_id=$3", reason or "не указана", by, tg_id)
    # гасим холды и активную заявку забаненного
    await pool().execute(
        "UPDATE rb_referrals SET status='void', voided_at=now() "
        "WHERE inviter_id=$1 AND status='hold'", tg_id)


async def clear_ban(tg_id: int):
    await pool().execute(
        "UPDATE rb_users SET banned=FALSE, ban_reason=NULL WHERE tg_id=$1", tg_id)


async def is_banned(tg_id: int) -> bool:
    return bool(await pool().fetchval("SELECT banned FROM rb_users WHERE tg_id = $1", tg_id))


async def is_admin(chat_id: int, tg_id: int) -> bool:
    return bool(await pool().fetchval(
        "SELECT 1 FROM rb_admins WHERE chat_id = $1 AND tg_id = $2", chat_id, tg_id))


async def admin_chats(tg_id: int) -> list[int]:
    rows = await pool().fetch("SELECT chat_id FROM rb_admins WHERE tg_id = $1", tg_id)
    return [r["chat_id"] for r in rows]


async def chats_overview():
    """
    Объединённый список всех чатов из трёх независимых систем с флагами услуг:
      referral — рефералка (/шайнуть, rb_chats)
      roulette — рулетка   (/шимм,     rb_roulette_chats)
      contest  — конкурс   (/шимшайнуть, rb_contest_chats)

    Один чат может использовать любую комбинацию. Собираем из всех источников,
    чтобы чат с одной лишь рулеткой тоже попал в список (раньше брали только
    rb_chats и такие чаты не показывались).

    Возвращает список dict: chat_id, title, referral, roulette, contest (bool),
    отсортирован: сначала где хоть что-то активно, потом по названию.
    """
    rows = await pool().fetch(
        """
        WITH ids AS (
            SELECT chat_id, title, active FROM rb_chats
            UNION ALL
            SELECT chat_id, title, active FROM rb_roulette_chats
            UNION ALL
            SELECT chat_id, title, active FROM rb_contest_chats
        ),
        titles AS (
            SELECT chat_id,
                   COALESCE(MAX(title) FILTER (WHERE title <> ''), MAX(title)) AS title
            FROM ids GROUP BY chat_id
        )
        SELECT t.chat_id, t.title,
               COALESCE(rc.active, FALSE) AS referral,
               COALESCE(ro.active, FALSE) AS roulette,
               COALESCE(cc.active, FALSE) AS contest
        FROM titles t
        LEFT JOIN rb_chats          rc ON rc.chat_id = t.chat_id
        LEFT JOIN rb_roulette_chats ro ON ro.chat_id = t.chat_id
        LEFT JOIN rb_contest_chats  cc ON cc.chat_id = t.chat_id
        ORDER BY (COALESCE(rc.active,FALSE) OR COALESCE(ro.active,FALSE)
                  OR COALESCE(cc.active,FALSE)) DESC, t.title
        """)
    return [dict(r) for r in rows]


EXPECTED_TABLES = [
    "rb_users", "rb_chats", "rb_admins", "rb_balances", "rb_ledger", "rb_ref_links",
    "rb_invites", "rb_targets", "rb_referrals", "rb_withdrawals", "rb_spins", "rb_audit",
    "rb_settings", "rb_roulette_budget", "rb_roulette_chats",
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
