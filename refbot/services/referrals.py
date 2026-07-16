"""
Реферальная механика. Здесь вся логика холдов.

Правила (жёсткие, не обходятся):
  1. Привязка (chat_id, invitee) -> first_inviter вечная. Кто привёл первым — тот
     и получает всегда, даже если реферал потом придёт по чужой ссылке.
     Иначе появляется схема "перепродажа рефералов между мультиаккаунтами".
  2. burned = TRUE ставится, когда реферал ушёл ПОСЛЕ выплаты. Обратной дороги нет.
  3. Валюта и сумма фиксируются в момент входа реферала, а не в момент выплаты.
  4. Начисление идёт только через db.apply() с ключом ref:<id> — двойная выплата
     физически невозможна даже если планировщик отработает дважды.
"""
import secrets
from datetime import datetime, timedelta, timezone

import db
from config import (REWARD, HOLD_HOURS, INVITE_TTL_MIN, MIN_ACCOUNT_ID_THRESHOLD,
                    BURST_WINDOW_MIN, BURST_COUNT, COIN_RATE)


def now() -> datetime:
    return datetime.now(timezone.utc)


# ---------- публичная реф-ссылка ----------
async def get_or_create_ref_code(chat_id: int, inviter_id: int) -> str:
    code = await db.pool().fetchval(
        "SELECT code FROM rb_ref_links WHERE chat_id = $1 AND inviter_id = $2", chat_id, inviter_id)
    if code:
        return code
    code = secrets.token_urlsafe(9)
    await db.pool().execute(
        "INSERT INTO rb_ref_links (code, chat_id, inviter_id) VALUES ($1, $2, $3) "
        "ON CONFLICT (chat_id, inviter_id) DO NOTHING",
        code, chat_id, inviter_id)
    return await db.pool().fetchval(
        "SELECT code FROM rb_ref_links WHERE chat_id = $1 AND inviter_id = $2", chat_id, inviter_id)


async def resolve_ref_code(code: str):
    return await db.pool().fetchrow("SELECT * FROM rb_ref_links WHERE code = $1", code)


# ---------- выдача одноразовой ссылки гостю ----------
async def issue_invite(bot, chat_id: int, inviter_id: int, visitor_id: int) -> tuple[str | None, str]:
    """
    Возвращает (url, причина_отказа). Отказ — если гость не проходит проверки.
    Ссылка одноразовая (member_limit=1) и живёт INVITE_TTL_MIN минут.
    name ссылки = ключ атрибуции, он прилетит обратно в chat_member апдейте.
    """
    if visitor_id == inviter_id:
        return None, "Себя пригласить нельзя."
    if await db.is_banned(visitor_id):
        return None, "Аккаунт заблокирован."

    chat = await db.pool().fetchrow("SELECT * FROM rb_chats WHERE chat_id = $1 AND active", chat_id)
    if not chat:
        return None, "Чат не подключён или отключён."

    if MIN_ACCOUNT_ID_THRESHOLD and visitor_id > MIN_ACCOUNT_ID_THRESHOLD:
        return None, "Аккаунт слишком новый для участия в программе."

    tgt = await db.pool().fetchrow(
        "SELECT * FROM rb_targets WHERE chat_id = $1 AND user_id = $2", chat_id, visitor_id)
    if tgt and tgt["burned"]:
        return None, "За этот аккаунт награда уже была получена ранее. Ссылку на вход попроси у админов чата."

    # уже состоит в чате -> не реферал
    try:
        m = await bot.get_chat_member(chat_id, visitor_id)
        if m.status in ("creator", "administrator", "member", "restricted"):
            return None, "Ты уже состоишь в этом чате."
        if m.status == "kicked":
            return None, "Ты забанен в этом чате."
    except Exception:
        pass  # not found -> ок

    # переиспользуем живую ссылку, чтобы не плодить их пачками
    old = await db.pool().fetchrow(
        "SELECT * FROM rb_invites WHERE chat_id = $1 AND visitor_id = $2", chat_id, visitor_id)
    if old and not old["revoked"] and old["used_at"] is None and old["expires_at"] > now():
        return old["invite_url"], ""

    name = f"rb{secrets.token_hex(6)}"
    expires = now() + timedelta(minutes=INVITE_TTL_MIN)
    link = await bot.create_chat_invite_link(
        chat_id=chat_id, name=name, member_limit=1, expire_date=expires)

    await db.pool().execute(
        """
        INSERT INTO rb_invites (chat_id, inviter_id, visitor_id, invite_name, invite_url, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (chat_id, visitor_id) DO UPDATE
        SET inviter_id = EXCLUDED.inviter_id, invite_name = EXCLUDED.invite_name,
            invite_url = EXCLUDED.invite_url, expires_at = EXCLUDED.expires_at,
            revoked = FALSE, used_at = NULL, used_by = NULL, created_at = now()
        """,
        chat_id, inviter_id, visitor_id, name, link.invite_link, expires)
    return link.invite_link, ""


# ---------- вход в чат ----------
async def on_join(bot, chat_id: int, user_id: int, invite_name: str | None):
    """Вызывается из chat_member апдейта. Идемпотентна."""
    chat = await db.pool().fetchrow("SELECT * FROM rb_chats WHERE chat_id = $1 AND active", chat_id)
    if not chat:
        return

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            tgt = await conn.fetchrow(
                "SELECT * FROM rb_targets WHERE chat_id = $1 AND user_id = $2 FOR UPDATE",
                chat_id, user_id)

            if tgt and tgt["burned"]:
                return  # сгорел навсегда, пусть заходит, но денег нет

            inviter_id = tgt["first_inviter_id"] if tgt else None

            if inviter_id is None:
                # первый вход: определяем пригласившего по name одноразовой ссылки
                if not invite_name:
                    return  # зашёл сам / по общей ссылке — не реферал
                inv = await conn.fetchrow(
                    "SELECT * FROM rb_invites WHERE invite_name = $1", invite_name)
                if not inv:
                    return
                if inv["visitor_id"] != user_id:
                    # ссылку передали другому. Не считаем — это первый признак фермы.
                    await conn.execute(
                        "INSERT INTO rb_audit (actor_id, action, payload) VALUES ($1,$2,$3::jsonb)",
                        user_id, "invite_hijack",
                        f'{{"chat_id":{chat_id},"expected":{inv["visitor_id"]}}}')
                    return
                inviter_id = inv["inviter_id"]
                await conn.execute(
                    "UPDATE rb_invites SET used_by=$1, used_at=now() WHERE invite_name=$2",
                    user_id, invite_name)
                await conn.execute(
                    "INSERT INTO rb_targets (chat_id, user_id, first_inviter_id) VALUES ($1,$2,$3) "
                    "ON CONFLICT DO NOTHING",
                    chat_id, user_id, inviter_id)

            if inviter_id == user_id:
                return
            if await conn.fetchval("SELECT banned FROM rb_users WHERE tg_id = $1", inviter_id):
                return

            # уже есть живой холд/выплата -> ничего не делаем
            alive = await conn.fetchval(
                "SELECT 1 FROM rb_referrals WHERE chat_id=$1 AND invitee_id=$2 "
                "AND status IN ('hold','paid')", chat_id, user_id)
            if alive:
                return

            # лимит рефералов в сутки
            today_cnt = await conn.fetchval(
                "SELECT count(*) FROM rb_referrals WHERE inviter_id=$1 AND chat_id=$2 "
                "AND joined_at > now() - interval '24 hours' AND status <> 'void'",
                inviter_id, chat_id)
            if today_cnt >= chat["max_refs_per_day"]:
                return

            cur = await conn.fetchval("SELECT currency FROM rb_users WHERE tg_id = $1", inviter_id) \
                  or "mushrooms"
            amount = chat["reward_coins"] if cur == "coins" else chat["reward_mushrooms"]

            # детект всплеска -> флаг на ручную проверку, выплата не пройдёт автоматом
            burst = await conn.fetchval(
                f"SELECT count(*) FROM rb_referrals WHERE inviter_id=$1 "
                f"AND joined_at > now() - interval '{BURST_WINDOW_MIN} minutes'", inviter_id)
            flagged = burst >= BURST_COUNT

            await conn.execute(
                """
                INSERT INTO rb_referrals (chat_id, inviter_id, invitee_id, currency, amount,
                                          status, hold_until, flagged, flag_reason)
                VALUES ($1,$2,$3,$4,$5,'hold', now() + make_interval(hours => $6), $7, $8)
                """,
                chat_id, inviter_id, user_id, cur, amount, chat["hold_hours"],
                flagged, "burst" if flagged else None)


# ---------- выход из чата ----------
async def on_leave(chat_id: int, user_id: int):
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            ref = await conn.fetchrow(
                "SELECT * FROM rb_referrals WHERE chat_id=$1 AND invitee_id=$2 "
                "AND status IN ('hold','paid') FOR UPDATE", chat_id, user_id)
            if not ref:
                return None

            if ref["status"] == "hold":
                # холд сгорает, деньги не начислялись -> просто закрываем
                await conn.execute(
                    "UPDATE rb_referrals SET status='void', voided_at=now() WHERE id=$1", ref["id"])
                return ("hold_lost", ref)

            # status == 'paid': деньги уже у пригласившего. Не отбираем, но жжём реферала.
            await conn.execute(
                "UPDATE rb_targets SET burned=TRUE, burned_at=now() WHERE chat_id=$1 AND user_id=$2",
                chat_id, user_id)
            await conn.execute(
                "UPDATE rb_referrals SET status='void', voided_at=now() WHERE id=$1", ref["id"])
            return ("burned", ref)


# ---------- начисление по истечении холда ----------
async def credit_due(bot) -> list[dict]:
    """Планировщик. Перед выплатой ПЕРЕПРОВЕРЯЕТ членство через API — это защита
    от пропущенных апдейтов (бот лежал / Telegram не доставил chat_member)."""
    rows = await db.pool().fetch(
        "SELECT * FROM rb_referrals WHERE status='hold' AND hold_until <= now() "
        "AND flagged = FALSE ORDER BY hold_until LIMIT 50")
    paid = []
    for r in rows:
        try:
            m = await bot.get_chat_member(r["chat_id"], r["invitee_id"])
            still_in = m.status in ("creator", "administrator", "member", "restricted")
        except Exception:
            still_in = False

        async with db.pool().acquire() as conn:
            async with conn.transaction():
                cur = await conn.fetchrow(
                    "SELECT * FROM rb_referrals WHERE id=$1 FOR UPDATE", r["id"])
                if cur["status"] != "hold":
                    continue

                if not still_in:
                    await conn.execute(
                        "UPDATE rb_referrals SET status='void', voided_at=now() WHERE id=$1", r["id"])
                    continue

                # бюджетный предохранитель
                chat = await conn.fetchrow(
                    "SELECT * FROM rb_chats WHERE chat_id=$1 FOR UPDATE", r["chat_id"])
                if chat["budget_date"] != now().date():
                    await conn.execute(
                        "UPDATE rb_chats SET budget_date=CURRENT_DATE, budget_spent_mush=0 "
                        "WHERE chat_id=$1", r["chat_id"])
                    spent = 0
                else:
                    spent = chat["budget_spent_mush"]
                cost = r["amount"] // COIN_RATE if r["currency"] == "coins" else r["amount"]
                if spent + cost > chat["daily_budget_mush"]:
                    continue  # подождёт до завтра, деньги не сгорают

                await conn.execute(
                    "UPDATE rb_chats SET budget_spent_mush = budget_spent_mush + $1 WHERE chat_id=$2",
                    cost, r["chat_id"])
                bal = await db.apply(conn, r["inviter_id"], r["currency"], r["amount"],
                                     "referral", f"ref:{r['id']}", r["id"])
                await conn.execute(
                    "UPDATE rb_referrals SET status='paid', credited_at=now() WHERE id=$1", r["id"])
        if bal is not None:
            paid.append({"inviter_id": r["inviter_id"], "amount": r["amount"],
                         "currency": r["currency"], "balance": bal})
    return paid


# ---------- статистика для профиля ----------
async def my_refs(tg_id: int):
    return await db.pool().fetch(
        """
        SELECT r.*, u.username, u.first_name
        FROM rb_referrals r LEFT JOIN rb_users u ON u.tg_id = r.invitee_id
        WHERE r.inviter_id = $1 AND r.status IN ('hold','paid')
        ORDER BY r.joined_at DESC LIMIT 50
        """, tg_id)
