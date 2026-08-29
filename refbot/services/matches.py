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


# ==================== МУЛЬТИПЛЕЕР (камень-ножницы-бумага и др.) ====================
RPS_LOBBY_MINUTES = 15   # на набор игроков (нажать «Старт»)
RPS_MOVE_MINUTES = 5     # на выбор знака в раунде


async def create_lobby(game: str, creator: int, currency: str, stake: int,
                       origin_chat: int) -> tuple[int | None, str]:
    """Создать лобби мультиплеер-игры. Создатель сразу участник с замороженной
    ставкой. status='lobby'."""
    if stake <= 0:
        return None, "Ставка должна быть больше нуля."
    if await db.is_banned(creator):
        return None, "Аккаунт заблокирован."
    if await has_active(creator):
        return None, "У тебя уже есть активная игра."
    b = await db.balances(creator)
    if b.get(currency, 0) < stake:
        return None, "Недостаточно средств для ставки."
    dl = _now() + timedelta(minutes=RPS_LOBBY_MINUTES)
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                mid = await conn.fetchval(
                    "INSERT INTO rb_matches (game, status, currency, stake, p1, "
                    "origin_chat, search_deadline) VALUES ($1,'lobby',$2,$3,$4,$5,$6) RETURNING id",
                    game, currency, stake, creator, origin_chat, dl)
                if not await _freeze(conn, creator, currency, stake, mid, "creator"):
                    raise _NoFunds()
                await conn.execute(
                    "INSERT INTO rb_match_players (mid, tg_id, status, staked) "
                    "VALUES ($1,$2,'playing',TRUE)", mid, creator)
    except _NoFunds:
        return None, "Недостаточно средств для ставки."
    await db.audit(creator, "rps_create", {"id": mid, "stake": stake, "cur": currency})
    return mid, ""


async def join_lobby(mid: int, uid: int) -> tuple[dict | None, str]:
    """Присоединиться к лобби: проверка баланса, заморозка ставки, добавление игрока."""
    if await db.is_banned(uid):
        return None, "Аккаунт заблокирован."
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m or m["status"] != "lobby":
                return None, "Лобби уже закрыто."
            ex = await conn.fetchval(
                "SELECT status FROM rb_match_players WHERE mid=$1 AND tg_id=$2", mid, uid)
            if ex:
                return None, "Ты уже в игре."
            if await has_active(uid, exclude_mid=mid):
                return None, "У тебя уже есть активная игра."
            cnt = await conn.fetchval(
                "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND status='playing'", mid)
            if cnt >= 5:
                return None, "Уже набрано 5 игроков — мест нет."
            if not await _freeze(conn, uid, m["currency"], m["stake"], mid, f"j{uid}"):
                return None, "Недостаточно средств для ставки."
            await conn.execute(
                "INSERT INTO rb_match_players (mid, tg_id, status, staked) "
                "VALUES ($1,$2,'playing',TRUE)", mid, uid)
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1", mid)
    await db.audit(uid, "rps_join", {"id": mid})
    return dict(m), ""


async def lobby_players(mid: int, status: str = "playing") -> list[dict]:
    rows = await db.pool().fetch(
        "SELECT * FROM rb_match_players WHERE mid=$1 AND status=$2 ORDER BY joined_at", mid, status)
    return [dict(r) for r in rows]


async def cancel_lobby(mid: int, by: int | None = None) -> tuple[dict | None, str]:
    """Отменить лобби до старта — вернуть ставки всем участникам.
    В возвращённом матче поле 'refunded' — список tg_id, кому вернули ставку."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m or m["status"] != "lobby":
                return None, "Лобби уже закрыто."
            if by is not None and m["p1"] != by:
                return None, "Только создатель может отменить."
            players = await conn.fetch(
                "SELECT tg_id, staked FROM rb_match_players WHERE mid=$1 AND staked", mid)
            refunded = []
            for p in players:
                await _refund(conn, p["tg_id"], m["currency"], m["stake"], mid, f"lob{p['tg_id']}")
                refunded.append(p["tg_id"])
            await conn.execute(
                "UPDATE rb_matches SET status='cancelled', finished_at=now() WHERE id=$1", mid)
    d = dict(m)
    d["refunded"] = refunded
    return d, ""


# ---------------- КНБ: раунды ----------------
async def set_choice(mid: int, uid: int, choice: str) -> tuple[bool, str]:
    """Игрок делает тайный выбор в текущем раунде. Только для активных игроков."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            p = await conn.fetchrow(
                "SELECT * FROM rb_match_players WHERE mid=$1 AND tg_id=$2 FOR UPDATE", mid, uid)
            if not p or p["status"] != "playing":
                return False, "Ты не в игре."
            if p["choice"]:
                return False, "Ты уже выбрал."
            await conn.execute(
                "UPDATE rb_match_players SET choice=$1 WHERE mid=$2 AND tg_id=$3",
                choice, mid, uid)
    return True, ""


async def round_progress(mid: int) -> tuple[int, int]:
    """(сколько выбрали, сколько всего активных) в текущем раунде."""
    total = await db.pool().fetchval(
        "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND status='playing'", mid)
    chosen = await db.pool().fetchval(
        "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND status='playing' AND choice IS NOT NULL",
        mid)
    return chosen or 0, total or 0


async def active_choices(mid: int) -> dict[int, str]:
    """{tg_id: choice} активных игроков, которые выбрали."""
    rows = await db.pool().fetch(
        "SELECT tg_id, choice FROM rb_match_players "
        "WHERE mid=$1 AND status='playing' AND choice IS NOT NULL", mid)
    return {r["tg_id"]: r["choice"] for r in rows}


async def set_round_deadline(mid: int, minutes: int = 5):
    dl = _now() + timedelta(minutes=minutes)
    await db.pool().execute("UPDATE rb_matches SET move_deadline=$1 WHERE id=$2", dl, mid)


async def apply_elimination(mid: int, eliminated: list[int]):
    """Пометить выбывших, очистить выборы для нового раунда."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            if eliminated:
                await conn.execute(
                    "UPDATE rb_match_players SET status='out' WHERE mid=$1 AND tg_id=ANY($2::bigint[])",
                    mid, eliminated)
            # очистить выборы у оставшихся — для следующего раунда
            await conn.execute(
                "UPDATE rb_match_players SET choice=NULL WHERE mid=$1 AND status='playing'", mid)


async def disqualify_no_choice(mid: int) -> list[int]:
    """Дисквалифицировать активных, кто не выбрал (тайм-аут). Возврат их ставок.
    Возвращает список дисквалифицированных tg_id."""
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m:
                return []
            rows = await conn.fetch(
                "SELECT tg_id, staked FROM rb_match_players "
                "WHERE mid=$1 AND status='playing' AND choice IS NULL", mid)
            for r in rows:
                if r["staked"]:
                    await _refund(conn, r["tg_id"], m["currency"], m["stake"], mid, f"dq{r['tg_id']}")
                await conn.execute(
                    "UPDATE rb_match_players SET status='dq' WHERE mid=$1 AND tg_id=$2",
                    mid, r["tg_id"])
                out.append(r["tg_id"])
    return out


async def finish_multiplayer(mid: int, winner: int) -> tuple[dict | None, str]:
    """Завершить мультиплеер-матч: победитель забирает банк (сумма замороженных
    ставок оставшихся) минус 3%. Идемпотентно."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            if not m:
                return None, "Матч не найден."
            if m["status"] != "active":
                return dict(m), ""
            # банк = сумма ставок всех, кто реально заморозил (не dq, не отменённые до старта)
            staked_cnt = await conn.fetchval(
                "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND staked", mid)
            bank = m["stake"] * staked_cnt
            from config import MATCH_FEE_PCT
            # комиссия 3% с проигравшей части банка (всё, кроме ставки победителя)
            losers_pot = m["stake"] * (staked_cnt - 1)
            fee = int(losers_pot * MATCH_FEE_PCT / 100)
            payout = bank - fee
            await db.apply(conn, winner, m["currency"], payout, "rps_win", f"rpswin:{mid}", mid)
            await conn.execute(
                "UPDATE rb_match_players SET status='winner' WHERE mid=$1 AND tg_id=$2", mid, winner)
            await conn.execute(
                "UPDATE rb_matches SET status='finished', winner=$1, finished_at=now() WHERE id=$2",
                winner, mid)
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1", mid)
    await db.audit(winner, "rps_finish", {"id": mid, "payout": payout})
    return dict(m), ""


# ==================== ЛОББИ-БРАУЗЕР: список открытых игр ====================
async def open_games(game: str, exclude_uid: int | None = None,
                     limit: int = 5, offset: int = 0) -> tuple[list[dict], int]:
    """
    Открытые игры данного типа, к которым можно присоединиться.
      game='ttt' -> status='searching' (открытый онлайн-вызов, ждёт соперника)
      game='rps' -> status='lobby'     (набор игроков)
    Возвращает (список игр с доп.полями creator/players_count, всего игр).
    exclude_uid — не показывать игры, где этот игрок уже участвует (свои/занятые).
    """
    status = "lobby" if game == "rps" else "searching"
    # всего открытых (для пагинации)
    total = await db.pool().fetchval(
        "SELECT count(*) FROM rb_matches WHERE game=$1 AND status=$2", game, status) or 0

    rows = await db.pool().fetch(
        "SELECT * FROM rb_matches WHERE game=$1 AND status=$2 "
        "ORDER BY created_at DESC LIMIT $3 OFFSET $4", game, status, limit, offset)
    out = []
    for m in rows:
        d = dict(m)
        # число игроков в лобби (для rps); для ttt всегда 1 (ждёт второго)
        if game == "rps":
            cnt = await db.pool().fetchval(
                "SELECT count(*) FROM rb_match_players WHERE mid=$1 AND status='playing'", m["id"])
            d["players_count"] = cnt or 0
        else:
            d["players_count"] = 1
        # исключаем игры, где exclude_uid уже участник (свои)
        if exclude_uid is not None:
            if m["p1"] == exclude_uid:
                d["is_mine"] = True
            elif game == "rps":
                inp = await db.pool().fetchval(
                    "SELECT 1 FROM rb_match_players WHERE mid=$1 AND tg_id=$2", m["id"], exclude_uid)
                d["is_mine"] = bool(inp)
            else:
                d["is_mine"] = False
        else:
            d["is_mine"] = False
        out.append(d)
    return out, total


# ==================== МОРСКОЙ БОЙ ====================
NAVY_PLACE_MINUTES = 15   # на расстановку кораблей
NAVY_MOVE_MINUTES = 5     # на ход


async def create_navy(p1: int, currency: str, stake: int, mode: str,
                      opponent: int | None, origin_chat: int) -> tuple[int | None, str]:
    """Создать матч морского боя (2 игрока). mode: 'online'|'direct'.
    Замораживает ставку создателя. Фаза 'placement'."""
    if stake <= 0:
        return None, "Ставка должна быть больше нуля."
    if await db.is_banned(p1):
        return None, "Аккаунт заблокирован."
    if await has_active(p1):
        return None, "У тебя уже есть активная игра."
    b = await db.balances(p1)
    if b.get(currency, 0) < stake:
        return None, "Недостаточно средств для ставки."
    status = "searching" if mode == "online" else "invited"
    dl = _now() + timedelta(minutes=SEARCH_MINUTES)
    import json
    init_state = {"phase": "lobby", "fields": {}, "ready": []}
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                mid = await conn.fetchval(
                    "INSERT INTO rb_matches (game, status, currency, stake, p1, p2, "
                    "origin_chat, search_deadline, state) "
                    "VALUES ('navy',$1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
                    status, currency, stake, p1, opponent, origin_chat, dl,
                    json.dumps(init_state))
                if not await _freeze(conn, p1, currency, stake, mid, "p1"):
                    raise _NoFunds()
    except _NoFunds:
        return None, "Недостаточно средств для ставки."
    await db.audit(p1, "navy_create", {"id": mid, "stake": stake, "cur": currency})
    return mid, ""


async def navy_state(mid: int) -> dict | None:
    """Получить state морского боя (распарсенный)."""
    import json
    m = await get(mid)
    if not m:
        return None
    st = m["state"]
    return st if isinstance(st, dict) else json.loads(st)


async def navy_save_field(mid: int, uid: int, field: dict):
    """Сохранить поле игрока (ships + shots_at_me) в state."""
    import json
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT state FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            st = m["state"] if isinstance(m["state"], dict) else json.loads(m["state"])
            st.setdefault("fields", {})[str(uid)] = field
            await conn.execute("UPDATE rb_matches SET state=$1 WHERE id=$2",
                               json.dumps(st), mid)


async def navy_set_ready(mid: int, uid: int) -> tuple[bool, str]:
    """Отметить игрока готовым (расставил корабли). Когда оба готовы — фаза battle."""
    import json
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1 FOR UPDATE", mid)
            st = m["state"] if isinstance(m["state"], dict) else json.loads(m["state"])
            ready = set(st.get("ready", []))
            ready.add(uid)
            st["ready"] = list(ready)
            both = m["p1"] in ready and m["p2"] in ready
            if both:
                st["phase"] = "battle"
                # первый ход — случайный
                import random
                turn = random.choice([m["p1"], m["p2"]])
                dl = _now() + timedelta(minutes=NAVY_MOVE_MINUTES)
                await conn.execute(
                    "UPDATE rb_matches SET state=$1, status='active', turn=$2, move_deadline=$3 "
                    "WHERE id=$4", json.dumps(st), turn, dl, mid)
            else:
                await conn.execute("UPDATE rb_matches SET state=$1 WHERE id=$2",
                                   json.dumps(st), mid)
    return True, ""


async def navy_save_turn(mid: int, state: dict, turn: int):
    """Сохранить состояние после хода + сменить/оставить ход, обновить дедлайн."""
    import json
    dl = _now() + timedelta(minutes=NAVY_MOVE_MINUTES)
    await db.pool().execute(
        "UPDATE rb_matches SET state=$1, turn=$2, move_deadline=$3 WHERE id=$4",
        json.dumps(state), turn, dl, mid)


async def navy_join(mid: int, p2: int) -> tuple[dict | None, str]:
    """Присоединиться к конкретному открытому морскому бою (по mid из браузера).
    Замораживает ставку p2, переводит в расстановку (не сразу в active)."""
    if await db.is_banned(p2):
        return None, "Аккаунт заблокирован."
    if await has_active(p2, exclude_mid=mid):
        return None, "У тебя уже есть активная игра."
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow(
                "SELECT * FROM rb_matches WHERE id=$1 AND game='navy' AND status='searching' "
                "FOR UPDATE", mid)
            if not m:
                return None, "Эта игра уже недоступна."
            if m["p1"] == p2:
                return None, "Нельзя вступить в свою игру."
            if not await _freeze(conn, p2, m["currency"], m["stake"], mid, "p2"):
                return None, "Недостаточно средств для ставки."
            await conn.execute(
                "UPDATE rb_matches SET p2=$1, status='placement', search_deadline=$2 WHERE id=$3",
                p2, _now() + timedelta(minutes=NAVY_PLACE_MINUTES), mid)
            m = await conn.fetchrow("SELECT * FROM rb_matches WHERE id=$1", mid)
    await db.audit(p2, "navy_join", {"id": mid})
    return dict(m), ""


async def navy_expire_placement() -> list[dict]:
    """Морские бои, где расстановка (placement) просрочена — вернуть ставки обоим."""
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM rb_matches WHERE game='navy' AND status='placement' "
                "AND search_deadline IS NOT NULL AND search_deadline < now() FOR UPDATE SKIP LOCKED")
            for m in rows:
                await _refund(conn, m["p1"], m["currency"], m["stake"], m["id"], "p1")
                if m["p2"]:
                    await _refund(conn, m["p2"], m["currency"], m["stake"], m["id"], "p2")
                await conn.execute(
                    "UPDATE rb_matches SET status='expired', finished_at=now() WHERE id=$1", m["id"])
                out.append(dict(m))
    return out


async def navy_expire_moves() -> list[dict]:
    """Морские бои, где ход просрочен (5 мин) — проигрывает тот, чей ход.
    Победитель забирает банк минус 3%."""
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM rb_matches WHERE game='navy' AND status='active' "
                "AND move_deadline IS NOT NULL AND move_deadline < now() FOR UPDATE SKIP LOCKED")
            for m in rows:
                loser = m["turn"]
                if loser is None or m["p2"] is None:
                    await _refund(conn, m["p1"], m["currency"], m["stake"], m["id"], "p1")
                    if m["p2"]:
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
                d = dict(m); d["timeout_winner"] = winner; d["timeout_loser"] = loser
                out.append(d)
    return out


async def navy_find_open(currency: str, stake: int, exclude_uid: int) -> int | None:
    """Найти открытый морской бой с той же ставкой/валютой (не свой). Вернуть mid или None."""
    return await db.pool().fetchval(
        "SELECT id FROM rb_matches WHERE game='navy' AND status='searching' "
        "AND currency=$1 AND stake=$2 AND p1<>$3 ORDER BY created_at LIMIT 1",
        currency, stake, exclude_uid)
