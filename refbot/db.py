import contextlib

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
                reason: str, idem: str, ref_id: int | None = None,
                allow_negative: bool = False) -> int | None:
    """
    Начислить/списать. idem — ключ идемпотентности, повтор просто ничего не сделает.
    Возвращает новый баланс или None, если проводка уже была.
    При списании (delta<0) и allow_negative=False бросает asyncpg.CheckViolationError,
    если денег не хватает (защита от овердрафта — CHECK снят со схемы, проверяем в коде).
    allow_negative=True разрешает уйти в минус (админ-штрафы).
    ВСЕГДА вызывать внутри transaction().
    """
    exists = await conn.fetchval("SELECT 1 FROM rb_ledger WHERE idempotency_key = $1", idem)
    if exists:
        return None

    if delta < 0 and not allow_negative:
        cur_bal = await conn.fetchval(
            "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2",
            tg_id, currency) or 0
        if cur_bal + delta < 0:
            # эмулируем прежнее поведение (casino/withdrawals ловят CheckViolationError)
            raise asyncpg.CheckViolationError(
                f"overdraft: balance {cur_bal} + delta {delta} < 0")

    new_balance = await conn.fetchval(
        """
        INSERT INTO rb_balances (tg_id, currency, amount)
        VALUES ($1, $2, $3::bigint)
        ON CONFLICT (tg_id, currency)
        DO UPDATE SET amount = rb_balances.amount + $3::bigint
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
    out = {"mushrooms": 0, "coins": 0, "shimcoins": 0, "revive": 0, "max": 0, "partials": 0}
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


async def remember_username(username: str, tg_id: int) -> None:
    """Запомнить username -> tg_id (для резолва @username в шёпотах). Без @, нижний регистр."""
    if not username:
        return
    u = username.lstrip("@").lower()
    if not u:
        return
    with contextlib.suppress(Exception):
        await pool().execute(
            "INSERT INTO rb_usernames (username, tg_id, last_seen) VALUES ($1,$2,now()) "
            "ON CONFLICT (username) DO UPDATE SET tg_id=$2, last_seen=now()", u, tg_id)


async def resolve_username(username: str):
    """@username -> tg_id или None, если бот не видел этого юзера."""
    u = (username or "").lstrip("@").lower()
    if not u:
        return None
    return await pool().fetchval("SELECT tg_id FROM rb_usernames WHERE username=$1", u)


async def set_wheel_anim(tg_id: int, style: str) -> None:
    """Стиль анимации платной рулетки: 'runner' (бегунок) | 'drum' (барабан)."""
    await pool().execute(
        "UPDATE rb_users SET wheel_anim=$2 WHERE tg_id=$1", tg_id, style)


async def grant_free_spin(tg_id: int) -> None:
    """Выдать игроку один доп-спин !шайн."""
    await pool().execute(
        "UPDATE rb_users SET free_spins = free_spins + 1 WHERE tg_id=$1", tg_id)


async def active_roulette_chats() -> list[dict]:
    """Чаты с активной рулеткой — для объявлений акции x5."""
    rows = await pool().fetch(
        "SELECT chat_id, title FROM rb_roulette_chats WHERE active = TRUE")
    return [dict(r) for r in rows]


# ======================= ОСОБЫЕ ПРЕДЛОЖЕНИЯ (акции) =======================
async def offer_create(tg_id: int, price_mush: int | None, price_coin: int | None,
                       limit_mush: int | None, limit_coin: int | None,
                       expires_at, created_by: int) -> int:
    return await pool().fetchval(
        "INSERT INTO rb_offers (tg_id, price_mush, price_coin, limit_mush, limit_coin, "
        "expires_at, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
        tg_id, price_mush, price_coin, limit_mush, limit_coin, expires_at, created_by)


async def offer_get(offer_id: int) -> dict | None:
    r = await pool().fetchrow("SELECT * FROM rb_offers WHERE id=$1", offer_id)
    return dict(r) if r else None


async def offers_for_user(tg_id: int) -> list[dict]:
    """Активные, не истёкшие акции игрока."""
    rows = await pool().fetch(
        "SELECT * FROM rb_offers WHERE tg_id=$1 AND active=TRUE "
        "AND (expires_at IS NULL OR expires_at > now()) ORDER BY created_at DESC", tg_id)
    return [dict(r) for r in rows]


async def offers_all() -> list[dict]:
    """Все акции (для админа) — активные и нет."""
    rows = await pool().fetch("SELECT * FROM rb_offers ORDER BY created_at DESC LIMIT 100")
    return [dict(r) for r in rows]


async def offer_delete(offer_id: int) -> None:
    await pool().execute("DELETE FROM rb_offers WHERE id=$1", offer_id)


async def offer_is_live(o: dict) -> bool:
    """Акция ещё действует (активна, не истекла, лимит не выбран полностью)?"""
    import datetime as _dt
    if not o["active"]:
        return False
    if o["expires_at"] is not None and o["expires_at"] <= _dt.datetime.now(_dt.timezone.utc):
        return False
    # если оба лимита заданы и оба выбраны — мертва
    mush_left = o["price_mush"] is not None and (
        o["limit_mush"] is None or o["sold_mush"] < o["limit_mush"])
    coin_left = o["price_coin"] is not None and (
        o["limit_coin"] is None or o["sold_coin"] < o["limit_coin"])
    return mush_left or coin_left


async def use_free_spin(conn, tg_id: int) -> bool:
    """
    Списать один доп-спин, если есть. Возвращает True если списан (можно крутить
    сверх лимита). Атомарно внутри переданной транзакции.
    """
    n = await conn.fetchval(
        "UPDATE rb_users SET free_spins = free_spins - 1 "
        "WHERE tg_id=$1 AND free_spins > 0 RETURNING free_spins", tg_id)
    return n is not None


async def has_free_spin(tg_id: int) -> bool:
    n = await pool().fetchval("SELECT free_spins FROM rb_users WHERE tg_id=$1", tg_id)
    return bool(n and n > 0)


async def all_active_user_ids() -> list[int]:
    """Все незабаненные пользователи — для массовой рассылки."""
    rows = await pool().fetch("SELECT tg_id FROM rb_users WHERE banned = FALSE")
    return [r["tg_id"] for r in rows]


async def apply_admin(tg_id: int, currency: str, delta: int, reason: str,
                      idem: str, allow_negative: bool = False) -> int:
    """
    Админское начисление/изъятие в своей транзакции. allow_negative=True разрешает
    отрицательный баланс (штрафы). Возвращает новый баланс.
    """
    async with pool().acquire() as conn:
        async with conn.transaction():
            bal = await apply(conn, tg_id, currency, delta, reason, idem,
                              allow_negative=allow_negative)
            if bal is None:
                # проводка уже была (дубль idem) — вернём текущий баланс
                bal = await conn.fetchval(
                    "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2",
                    tg_id, currency) or 0
    return bal


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


# ==================== РОЗЫГРЫШИ (giveaway) ====================
import json as _json


async def gw_create(data: dict) -> int:
    """Создать розыгрыш (черновик). data — собранное из FSM. Возвращает id."""
    return await pool().fetchval(
        """
        INSERT INTO rb_giveaways
          (title, title_html, key_on, key_off, announce_text, finish_text, reward_mode,
           other_desc, places, prizes, ends_at, created_by, announce_photo, finish_photo,
           status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,'draft')
        RETURNING id
        """,
        data["title"], data.get("title_html") or data["title"],
        data["key_on"], data["key_off"], data["announce_text"],
        data["finish_text"], data["reward_mode"], data.get("other_desc"),
        data["places"], _json.dumps(data["prizes"]), data.get("ends_at"),
        data["created_by"], data.get("announce_photo"), data.get("finish_photo"))


async def gw_key_taken(key_on: str) -> bool:
    """Занят ли ключ активным (draft|running) розыгрышем."""
    return bool(await pool().fetchval(
        "SELECT 1 FROM rb_giveaways WHERE key_on=$1 AND status IN ('draft','running')",
        key_on))


async def gw_get(gid: int) -> dict | None:
    row = await pool().fetchrow("SELECT * FROM rb_giveaways WHERE id=$1", gid)
    if not row:
        return None
    d = dict(row)
    if isinstance(d.get("prizes"), str):
        d["prizes"] = _json.loads(d["prizes"])
    return d


async def gw_list(status: str = None) -> list[dict]:
    """Список розыгрышей, опционально по статусу. Свежие сверху."""
    if status:
        rows = await pool().fetch(
            "SELECT * FROM rb_giveaways WHERE status=$1 ORDER BY created_at DESC", status)
    else:
        rows = await pool().fetch(
            "SELECT * FROM rb_giveaways ORDER BY created_at DESC")
    return [dict(r) for r in rows]


async def gw_delete(gid: int) -> None:
    """Удалить розыгрыш целиком (CASCADE снесёт чаты и участников)."""
    await pool().execute("DELETE FROM rb_giveaways WHERE id=$1", gid)


async def gw_set_status(gid: int, status: str) -> None:
    ts = {"running": "started_at", "finished": "finished_at"}.get(status)
    if ts:
        await pool().execute(
            f"UPDATE rb_giveaways SET status=$1, {ts}=now() WHERE id=$2", status, gid)
    else:
        await pool().execute("UPDATE rb_giveaways SET status=$1 WHERE id=$2", status, gid)


async def gw_by_key_on(key: str) -> dict | None:
    """Активный (draft|running) розыгрыш по ключу привязки."""
    row = await pool().fetchrow(
        "SELECT * FROM rb_giveaways WHERE key_on=$1 AND status IN ('draft','running')", key)
    return dict(row) if row else None


async def gw_by_key_off(key: str) -> dict | None:
    row = await pool().fetchrow(
        "SELECT * FROM rb_giveaways WHERE key_off=$1 AND status IN ('draft','running')", key)
    return dict(row) if row else None


async def gw_bind_chat(gid: int, chat_id: int, title: str, kind: str,
                       invite_link: str = None) -> None:
    """Привязать чат/канал к розыгрышу (или обновить, если уже привязан)."""
    await pool().execute(
        """
        INSERT INTO rb_giveaway_chats (giveaway_id, chat_id, title, kind, invite_link)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (giveaway_id, chat_id) DO UPDATE
        SET title=EXCLUDED.title, kind=EXCLUDED.kind,
            invite_link=COALESCE(EXCLUDED.invite_link, rb_giveaway_chats.invite_link)
        """, gid, chat_id, title, kind, invite_link)


async def gw_unbind_chat(gid: int, chat_id: int) -> bool:
    """Отвязать чат. True если что-то удалили."""
    r = await pool().execute(
        "DELETE FROM rb_giveaway_chats WHERE giveaway_id=$1 AND chat_id=$2", gid, chat_id)
    return r.endswith(("1",))


async def gw_chats(gid: int) -> list[dict]:
    rows = await pool().fetch(
        "SELECT * FROM rb_giveaway_chats WHERE giveaway_id=$1 ORDER BY title", gid)
    return [dict(r) for r in rows]


async def gw_update_invite(gid: int, chat_id: int, link: str) -> None:
    await pool().execute(
        "UPDATE rb_giveaway_chats SET invite_link=$1 WHERE giveaway_id=$2 AND chat_id=$3",
        link, gid, chat_id)


async def gw_save_announce_msg(gid: int, chat_id: int, msg_id: int) -> None:
    await pool().execute(
        "UPDATE rb_giveaway_chats SET announce_msg=$1 WHERE giveaway_id=$2 AND chat_id=$3",
        msg_id, gid, chat_id)


async def gw_save_result_msg(gid: int, chat_id: int, msg_id: int) -> None:
    await pool().execute(
        "UPDATE rb_giveaway_chats SET result_msg=$1 WHERE giveaway_id=$2 AND chat_id=$3",
        msg_id, gid, chat_id)


async def gw_join(gid: int, tg_id: int, currency: str = None) -> None:
    """Записать участника (или обновить выбранную валюту)."""
    await pool().execute(
        """
        INSERT INTO rb_giveaway_members (giveaway_id, tg_id, currency)
        VALUES ($1,$2,$3)
        ON CONFLICT (giveaway_id, tg_id) DO UPDATE SET currency=EXCLUDED.currency
        """, gid, tg_id, currency)


async def gw_is_member(gid: int, tg_id: int) -> bool:
    return bool(await pool().fetchval(
        "SELECT 1 FROM rb_giveaway_members WHERE giveaway_id=$1 AND tg_id=$2", gid, tg_id))


async def gw_members(gid: int) -> list[dict]:
    rows = await pool().fetch(
        "SELECT * FROM rb_giveaway_members WHERE giveaway_id=$1 ORDER BY joined_at", gid)
    return [dict(r) for r in rows]


async def gw_member_count(gid: int) -> int:
    return await pool().fetchval(
        "SELECT count(*) FROM rb_giveaway_members WHERE giveaway_id=$1", gid) or 0


async def gw_strike(tg_id: int) -> int:
    """Добавить глобальный страйк. Возвращает новое число страйков."""
    return await pool().fetchval(
        "UPDATE rb_users SET strikes = strikes + 1 WHERE tg_id=$1 RETURNING strikes", tg_id)


async def gw_get_strikes(tg_id: int) -> int:
    return await pool().fetchval("SELECT strikes FROM rb_users WHERE tg_id=$1", tg_id) or 0


async def gw_clear_strikes(tg_id: int) -> None:
    await pool().execute("UPDATE rb_users SET strikes=0 WHERE tg_id=$1", tg_id)


async def gw_mark_struck(gid: int, tg_id: int) -> None:
    await pool().execute(
        "UPDATE rb_giveaway_members SET struck=TRUE WHERE giveaway_id=$1 AND tg_id=$2",
        gid, tg_id)


async def gw_mark_winner(gid: int, tg_id: int, place: int) -> None:
    await pool().execute(
        "UPDATE rb_giveaway_members SET is_winner=TRUE, place=$3 "
        "WHERE giveaway_id=$1 AND tg_id=$2", gid, tg_id, place)


async def gw_strikes_list() -> list[dict]:
    """Все, у кого есть страйки или бан — для панели «Страйки и баны»."""
    rows = await pool().fetch(
        "SELECT tg_id, username, first_name, strikes, banned FROM rb_users "
        "WHERE strikes > 0 OR banned = TRUE ORDER BY strikes DESC, banned DESC")
    return [dict(r) for r in rows]


async def gw_finished_before(cutoff) -> list[int]:
    """id завершённых розыгрышей старше cutoff — для автоудаления."""
    rows = await pool().fetch(
        "SELECT id FROM rb_giveaways WHERE status='finished' AND finished_at < $1", cutoff)
    return [r["id"] for r in rows]


async def gw_due_timers(now):
    """Розыгрыши, которым пора автозавершиться (ends_at прошёл, ещё running)."""
    rows = await pool().fetch(
        "SELECT id FROM rb_giveaways WHERE status='running' AND ends_at IS NOT NULL "
        "AND ends_at <= $1", now)
    return [r["id"] for r in rows]


async def log_case_open(tg_id: int, case_key: str, currency: str,
                        cost: int, won: int, multiplier: float) -> None:
    """Записать открытие кейса в лог (история + антифрод)."""
    await pool().execute(
        "INSERT INTO rb_case_opens (tg_id, case_key, currency, cost, won, multiplier) "
        "VALUES ($1,$2,$3,$4,$5,$6)", tg_id, case_key, currency, cost, won, multiplier)


async def casino_stats(tg_id: int) -> dict:
    """
    Статистика игрока по казино, раздельно по валютам.
    Возвращает {currency: {games, wins, bet_total, won_total}} — где
    wins = игры с выигрышем > ставки. Плюс last: список последних 20 игр.
    """
    rows = await pool().fetch(
        "SELECT case_key, currency, cost, won, multiplier, created_at "
        "FROM rb_case_opens WHERE tg_id=$1 ORDER BY created_at DESC", tg_id)
    per: dict[str, dict] = {}
    for r in rows:
        cur = r["currency"]
        d = per.setdefault(cur, {"games": 0, "wins": 0, "bet_total": 0, "won_total": 0})
        d["games"] += 1
        d["bet_total"] += r["cost"]
        d["won_total"] += r["won"]
        if r["won"] > r["cost"]:
            d["wins"] += 1
    last = [dict(r) for r in rows[:20]]
    return {"per": per, "last": last}


async def promo_create(code: str, reward_mush: int, max_acts: int | None,
                       expires_at, created_by: int, reward_kind: str = "rate") -> int:
    return await pool().fetchval(
        "INSERT INTO rb_promo (code, reward_mush, reward_kind, max_acts, expires_at, created_by) "
        "VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
        code, reward_mush, reward_kind, max_acts, expires_at, created_by)


async def promo_list() -> list:
    return await pool().fetch(
        "SELECT * FROM rb_promo ORDER BY created_at DESC")


async def promo_get(pid: int):
    return await pool().fetchrow("SELECT * FROM rb_promo WHERE id=$1", pid)


async def promo_delete(pid: int) -> None:
    await pool().execute("DELETE FROM rb_promo WHERE id=$1", pid)


async def promo_activate(tg_id: int, code: str, coin_rate: int):
    """
    Активировать промокод по кодовому слову (без учёта регистра). Атомарно.
    Возвращает (status, reward_mush, pid, reward_kind):
      status: 'ok' | 'notfound' | 'expired' | 'used_up' | 'already'
    Начисление делает вызывающий: reward_kind говорит, как трактовать reward_mush
    (rate — в валюте игрока; mushrooms/coins — фиксированная валюта).
    """
    async with pool().acquire() as conn:
        async with conn.transaction():
            p = await conn.fetchrow(
                "SELECT * FROM rb_promo WHERE lower(code)=lower($1) "
                "ORDER BY created_at DESC LIMIT 1 FOR UPDATE", code)
            if not p:
                return ("notfound", 0, None, None)
            # срок
            if p["expires_at"] is not None:
                exp = await conn.fetchval("SELECT $1::timestamptz < now()", p["expires_at"])
                if exp:
                    return ("expired", 0, None, None)
            # лимит активаций
            if p["max_acts"] is not None and p["used"] >= p["max_acts"]:
                return ("used_up", 0, None, None)
            # уже активировал этот человек?
            dup = await conn.fetchval(
                "SELECT 1 FROM rb_promo_acts WHERE promo_id=$1 AND tg_id=$2", p["id"], tg_id)
            if dup:
                return ("already", 0, None, None)
            # резервируем активацию
            await conn.execute(
                "INSERT INTO rb_promo_acts (promo_id, tg_id) VALUES ($1,$2)", p["id"], tg_id)
            await conn.execute(
                "UPDATE rb_promo SET used = used + 1 WHERE id=$1", p["id"])
            kind = p["reward_kind"] if "reward_kind" in p else "rate"
            return ("ok", p["reward_mush"], p["id"], kind)


EXPECTED_TABLES = [
    "rb_users", "rb_chats", "rb_admins", "rb_balances", "rb_ledger", "rb_ref_links",
    "rb_invites", "rb_targets", "rb_referrals", "rb_withdrawals", "rb_spins", "rb_audit",
    "rb_settings", "rb_roulette_budget", "rb_roulette_chats",
    "rb_contest_chats", "rb_week_msgs", "rb_week_draws",
    "rb_giveaways", "rb_giveaway_chats", "rb_giveaway_members",
    "rb_case_opens", "rb_promo", "rb_promo_acts",
    "rb_offers",
    "rb_bank_exch",
    "rb_usernames",
    "rb_wd_items",
    "rb_matches",
    "rb_match_players",
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
