"""
PvP-матчи со ставками (крестики-нолики, морской бой, будущие игры).

Денежная модель:
  - Равная ставка. При входе игрока его ставка ЗАМОРАЖИВАЕТСЯ (списывается).
  - Победитель получает банк = 2*stake. Ничья — каждому возврат его ставки.
  - Отмена/таймаут поиска/отклонение — возврат уже замороженных ставок.
  idem-ключи привязаны к match.id и роли игрока, поэтому повторы безопасны.

Статусы: searching (ждём случайного) | invited (ждём приглашённого) |
         active (идёт) | finished | cancelled | expired.

Одна активная запись на игрока: перед созданием/входом проверяем has_active.
Игровая логика (доска, победа) — в отдельных модулях (ttt.py и т.д.);
здесь только матчмейкинг, деньги и общие переходы состояний.
"""
import json
from datetime import datetime, timedelta, timezone

import db

SEARCH_MINUTES = 10   # поиск случайного оппонента
MOVE_MINUTES = 5      # тайм-аут на ход


def _now():
    return datetime.now(timezone.utc)


async def has_active(uid: int, exclude_mid: int | None = None) -> bool:
    """Игрок уже в поиске/приглашении/игре (как p1 или p2).
    exclude_mid — не считать этот матч (например, приглашение, которое игрок сейчас принимает)."""
    if exclude_mid is None:
        row = await db.pool().fetchval(
            "SELECT 1 FROM rb_matches "
            "WHERE status IN ('searching','invited','active') "
            "AND (p1=$1 OR p2=$1) LIMIT 1", uid)
    else:
        row = await db.pool().fetchval(
            "SELECT 1 FROM rb_matches "
            "WHERE status IN ('searching','invited','active') "
            "AND (p1=$1 OR p2=$1) AND id<>$2 LIMIT 1", uid, exclude_mid)
    return bool(row)


async def _freeze(conn, uid: int, currency: str, stake: int, mid: int, role: str) -> bool:
    """Заморозить ставку игрока. False, если не хватает."""
    bal = await conn.fetchval(
        "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2 FOR UPDATE",
        uid, currency) or 0
    if bal < stake:
        return False
    await db.apply(conn, uid, currency, -stake, "match_stake", f"mstake:{mid}:{role}", mid)
    return True


async def _refund(conn, uid: int, currency: str, stake: int, mid: int, role: str):
    await db.apply(conn, uid, currency, stake, "match_refund", f"mrefund:{mid}:{role}", mid)


async def create_match(game: str, p1: int, currency: str, stake: int,
                       mode: str, opponent: int | None = None,
                       origin_chat: int | None = None) -> tuple[int | None, str]:
    """
    Создать матч. mode='online' (ищем случайного) | 'direct' (приглашаем opponent).
    Замораживает ставку создателя. Возвращает (match_id, "") или (None, ошибка).
    """
    if stake <= 0:
        return None, "Ставка должна быть больше нуля."
    if await db.is_banned(p1):
        return None, "Аккаунт заблокирован."
    if await has_active(p1):
        return None, "У тебя уже есть активная игра. Заверши её или отмени."
    if mode == "direct":
        if not opponent:
            return None, "Не указан оппонент."
        if opponent == p1:
            return None, "Нельзя играть с самим собой."
        if await db.is_banned(opponent):
            return None, "Этот игрок недоступен."

    status = "searching" if mode == "online" else "invited"
    # и поиск, и приглашение имеют дедлайн — чтобы не висели вечно, блокируя игроков
    search_dl = _now() + timedelta(minutes=SEARCH_MINUTES)

    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                mid = await conn.fetchval(
                    "INSERT INTO rb_matches (game, status, currency, stake, p1, p2, "
                    "origin_chat, search_deadline) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
                    game, status, currency, stake, p1, opponent, origin_chat, search_dl)
                if not await _freeze(conn, p1, currency, stake, mid, "p1"):
                    raise _NoFunds()
    except _NoFunds:
        return None, "Недостаточно средств для ставки."
    await db.audit(p1, "match_create",
                   {"id": mid, "game": game, "mode": mode, "stake": stake, "cur": currency})
    return mid, ""


class _NoFunds(Exception):
    pass


async def join_online(game: str, p2: int) -> tuple[dict | None, str]:
    """Присоединиться к случайному ищущему матчу этой игры. Замораживает ставку p2."""
    if await db.is_banned(p2):
        return None, "Аккаунт заблокирован."
    if await has_active(p2):
        return None, "У тебя уже есть активная игра."
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow(
                "SELECT * FROM rb_matches WHERE game=$1 AND status='searching' AND p1<>$2 "
                "ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED", game, p2)
            if not m:
                return None, "Нет открытых игр — создай свою."
            if not await _freeze(conn, p2, m["currency"], m["stake"], m["id"], "p2"):
                return None, "Недостаточно средств для ставки."
            await conn.execute(
                "UPDATE rb_matches SET p2=$1, status='active', search_deadline=NULL WHERE id=$2",
                p2, m["id"])
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1", m["id"])
    await db.audit(p2, "match_join", {"id": m["id"]})
    return dict(m), ""


async def accept_invite(mid: int, p2: int) -> tuple[dict | None, str]:
    """Приглашённый принимает игру. Замораживает его ставку, статус -> active."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m:
                return None, "Игра не найдена."
            if m["status"] != "invited":
                return None, "Игра уже недоступна."
            if m["p2"] != p2:
                return None, "Это приглашение не тебе."
            if await has_active(p2, exclude_mid=mid):
                return None, "У тебя уже есть активная игра."
            if not await _freeze(conn, p2, m["currency"], m["stake"], mid, "p2"):
                return None, "Недостаточно средств для ставки."
            await conn.execute("UPDATE rb_matches SET status='active' WHERE id=$1", mid)
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1", mid)
    await db.audit(p2, "match_accept", {"id": mid})
    return dict(m), ""


async def decline_invite(mid: int, p2: int) -> tuple[dict | None, str]:
    """Приглашённый отклоняет. Возврат ставки создателю, матч -> cancelled."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m or m["status"] != "invited":
                return None, "Игра уже недоступна."
            if m["p2"] != p2:
                return None, "Это приглашение не тебе."
            await _refund(conn, m["p1"], m["currency"], m["stake"], mid, "p1")
            await conn.execute(
                "UPDATE rb_matches SET status='cancelled', finished_at=now() WHERE id=$1", mid)
    await db.audit(p2, "match_decline", {"id": mid})
    return dict(m), ""


async def cancel(mid: int, by: int) -> tuple[dict | None, str]:
    """Создатель отменяет поиск/приглашение. Возврат его ставки."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m:
                return None, "Игра не найдена."
            if m["p1"] != by:
                return None, "Только создатель может отменить."
            if m["status"] not in ("searching", "invited"):
                return None, "Игру уже нельзя отменить."
            await _refund(conn, m["p1"], m["currency"], m["stake"], mid, "p1")
            await conn.execute(
                "UPDATE rb_matches SET status='cancelled', finished_at=now() WHERE id=$1", mid)
    await db.audit(by, "match_cancel", {"id": mid})
    return dict(m), ""


async def finish(mid: int, winner: int | None) -> tuple[dict | None, str]:
    """
    Завершить игру. winner=tg_id победителя -> ему банк (2*stake).
    winner=0 -> ничья, возврат обоим. Идемпотентно по статусу.
    """
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m:
                return None, "Игра не найдена."
            if m["status"] != "active":
                return dict(m), ""  # уже завершена — идемпотентно
            cur, stake = m["currency"], m["stake"]
            if winner == 0 or winner is None:
                # ничья — возврат обоим, без комиссии
                await _refund(conn, m["p1"], cur, stake, mid, "p1")
                await _refund(conn, m["p2"], cur, stake, mid, "p2")
                win_val = 0
            else:
                # победитель забирает банк минус 3% комиссии казино с проигравшей ставки.
                # банк = 2*stake; комиссия = 3% от stake проигравшего; выплата = 2*stake - fee.
                from config import MATCH_FEE_PCT
                fee = int(stake * MATCH_FEE_PCT / 100)   # округление вниз — в пользу казны
                payout = stake * 2 - fee
                await db.apply(conn, winner, cur, payout, "match_win", f"mwin:{mid}", mid)
                win_val = winner
            await conn.execute(
                "UPDATE rb_matches SET status='finished', winner=$1, finished_at=now() WHERE id=$2",
                win_val, mid)
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1", mid)
    await db.audit(m["p1"], "match_finish", {"id": mid, "winner": winner})
    return dict(m), ""


async def get(mid: int) -> dict | None:
    m = await db.pool().fetchrow("SELECT * FROM rb_matches WHERE id=$1", mid)
    return dict(m) if m else None


async def active_of(uid: int) -> dict | None:
    m = await db.pool().fetchrow(
        "SELECT * FROM rb_matches WHERE status IN ('searching','invited','active') "
        "AND (p1=$1 OR p2=$1) ORDER BY id DESC LIMIT 1", uid)
    return dict(m) if m else None


async def save_state(mid: int, state: dict, turn: int, move_deadline_min: int = MOVE_MINUTES):
    """Сохранить состояние доски и передать ход, обновив дедлайн хода."""
    dl = _now() + timedelta(minutes=move_deadline_min)
    await db.pool().execute(
        "UPDATE rb_matches SET state=$1::jsonb, turn=$2, move_deadline=$3 WHERE id=$4",
        json.dumps(state), turn, dl, mid)


async def expire_searches() -> list[dict]:
    """Просроченные поиски И приглашения (дедлайн истёк) — вернуть ставку создателю.
    Возвращает список истёкших матчей (для уведомления)."""
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM rb_matches WHERE status IN ('searching','invited') "
                "AND ((search_deadline IS NOT NULL AND search_deadline < now()) "
                "     OR (search_deadline IS NULL AND created_at < now() - interval '10 minutes')) "
                "FOR UPDATE SKIP LOCKED")
            for m in rows:
                await _refund(conn, m["p1"], m["currency"], m["stake"], m["id"], "p1")
                await conn.execute(
                    "UPDATE rb_matches SET status='expired', finished_at=now() WHERE id=$1", m["id"])
                out.append(dict(m))
    return out


async def expire_moves() -> list[dict]:
    """Просроченные ходы (5 мин) — засчитать поражение зевнувшему, банк сопернику.
    Возвращает список завершённых по тайм-ауту матчей (для уведомления и синка)."""
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM rb_matches WHERE status='active' "
                "AND ((move_deadline IS NOT NULL AND move_deadline < now()) "
                "     OR (move_deadline IS NULL AND created_at < now() - interval '10 minutes')) "
                "FOR UPDATE SKIP LOCKED")
            for m in rows:
                loser = m["turn"]                      # чей был ход — тот зевнул
                if loser is None or m["p2"] is None:
                    # зависший матч без хода/оппонента — просто возврат обоим (ничья)
                    await _refund(conn, m["p1"], m["currency"], m["stake"], m["id"], "p1")
                    if m["p2"] is not None:
                        await _refund(conn, m["p2"], m["currency"], m["stake"], m["id"], "p2")
                    await conn.execute(
                        "UPDATE rb_matches SET status='expired', finished_at=now() WHERE id=$1", m["id"])
                    continue
                winner = m["p2"] if loser == m["p1"] else m["p1"]
                from config import MATCH_FEE_PCT
                fee = int(m["stake"] * MATCH_FEE_PCT / 100)
                payout = m["stake"] * 2 - fee
                await db.apply(conn, winner, m["currency"], payout, "match_win",
                               f"mwin:{m['id']}", m["id"])
                await conn.execute(
                    "UPDATE rb_matches SET status='finished', winner=$1, finished_at=now() "
                    "WHERE id=$2", winner, m["id"])
                d = dict(m)
                d["winner"] = winner
                d["loser"] = loser
                out.append(d)
    return out
