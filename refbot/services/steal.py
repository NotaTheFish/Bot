"""
Воровство !сшайнить / защита !шимщит.
- Атака висит 20 мин; не отбили -> удача вора (жертва->вор), отбили -> штраф (вор->жертва).
- Сумма по полосам рулетки !шайн, потолок = min(баланс вора, баланс жертвы, 1М).
- Один активный налёт на вора И на жертву. Воровать раз в день (МСК), быть жертвой — без лимита.
- Копилки и вывод не участвуют (balances() уже без них).
"""
from datetime import datetime, timedelta, timezone

import db
import roulette

STEAL_WINDOW_MIN = 20
STEAL_MIN_BALANCE = 500          # минимум у вора, чтобы воровать
STEAL_MAX = 1_000_000            # потолок кражи (грибы)
STEAL_HOUR_FROM = 7              # МСК
STEAL_HOUR_TO = 23
MSK = timezone(timedelta(hours=3))


def _now_msk() -> datetime:
    return datetime.now(MSK)


def within_hours() -> bool:
    h = _now_msk().hour
    return STEAL_HOUR_FROM <= h < STEAL_HOUR_TO


async def active_involving(uid: int) -> dict | None:
    """Активная атака, где uid — вор или жертва (для блокировки)."""
    r = await db.pool().fetchrow(
        "SELECT * FROM rb_steal WHERE status='active' AND (thief=$1 OR victim=$1) LIMIT 1", uid)
    return dict(r) if r else None


async def can_steal_today(uid: int) -> bool:
    from config import UNLIMITED_SPIN_IDS
    if uid in UNLIMITED_SPIN_IDS:
        return True   # безлимитное воровство (как безлимитный !шайн)
    row = await db.pool().fetchval("SELECT last_day FROM rb_steal_cd WHERE tg_id=$1", uid)
    if not row:
        return True
    return row < _now_msk().date()


async def _roll_amount(cap: int) -> int:
    """Сумма кражи по полосам рулетки, обрезанная потолком cap.
    Спецшанс на 1М уже внутри roulette.roll (мега-джекпот)."""
    amount, _mega = roulette.roll("mushrooms")
    return max(1, min(amount, cap))


async def start_steal(thief: int, victim: int, chat_id: int) -> tuple[dict | None, str]:
    """Начать атаку. Проверки, фиксация суммы, окно 20 мин."""
    if thief == victim:
        return None, "Себя обворовать не выйдет 🙂"
    if await db.is_banned(thief):
        return None, "Аккаунт заблокирован."
    if not within_hours():
        return None, f"Воровать можно с {STEAL_HOUR_FROM}:00 до {STEAL_HOUR_TO}:00 по МСК."
    if not await can_steal_today(thief):
        return None, "Ты уже воровал сегодня. Попробуй после 00:00 по МСК."
    if await active_involving(thief):
        return None, "Ты уже участвуешь в налёте — дождись конца."
    if await active_involving(victim):
        return None, "На эту жертву уже идёт налёт — подожди."
    tb = (await db.balances(thief)).get("mushrooms", 0)
    vb = (await db.balances(victim)).get("mushrooms", 0)
    if tb < STEAL_MIN_BALANCE:
        return None, f"Для воровства нужно минимум {STEAL_MIN_BALANCE} 🍄 на балансе."
    if vb <= 0:
        return None, "У жертвы нет грибов для кражи."
    cap = min(tb, vb, STEAL_MAX)
    amount = await _roll_amount(cap)

    # временный щит у жертвы — блок мгновенно, вор теряет попытку дня
    shield = await _consume_time_shield(victim)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO rb_steal_cd (tg_id, last_day) VALUES ($1,$2) "
                "ON CONFLICT (tg_id) DO UPDATE SET last_day=$2", thief, _now_msk().date())
            if shield:
                # мгновенная защита — атака даже не начинается
                return {"instant_shield": True, "victim": victim, "thief": thief}, ""
            dl = datetime.now(timezone.utc) + timedelta(minutes=STEAL_WINDOW_MIN)
            sid = await conn.fetchval(
                "INSERT INTO rb_steal (thief, victim, amount, chat_id, deadline) "
                "VALUES ($1,$2,$3,$4,$5) RETURNING id", thief, victim, amount, chat_id, dl)
            m = await conn.fetchrow("SELECT * FROM rb_steal WHERE id=$1", sid)
    return dict(m), ""


async def _consume_time_shield(uid: int) -> bool:
    """Проверить/списать временной щит (мгновенная защита). True если сработал."""
    now = datetime.now(timezone.utc)
    r = await db.pool().fetchrow(
        "SELECT * FROM rb_shield WHERE tg_id=$1 AND kind='time' AND expires_at>$2 LIMIT 1",
        uid, now)
    return bool(r)


async def _consume_once_shield(conn, uid: int) -> bool:
    """Списать разовый щит (из rb_shield kind='uses' или инвентаря). True если был."""
    r = await conn.fetchrow(
        "SELECT * FROM rb_shield WHERE tg_id=$1 AND kind='uses' AND uses_left>0 "
        "ORDER BY created_at LIMIT 1 FOR UPDATE", uid)
    if r:
        left = r["uses_left"] - 1
        if left <= 0:
            await conn.execute("DELETE FROM rb_shield WHERE id=$1", r["id"])
        else:
            await conn.execute("UPDATE rb_shield SET uses_left=$1 WHERE id=$2", left, r["id"])
        return True
    return False


async def defend(uid: int) -> tuple[dict | None, str]:
    """Жертва отбивает атаку командой !шимщит. Штраф вору (вор->жертва)."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            r = await conn.fetchrow(
                "SELECT * FROM rb_steal WHERE victim=$1 AND status='active' FOR UPDATE", uid)
            if not r:
                return None, "На тебя сейчас нет активного налёта."
            m = dict(r)
            pay = await _finish(conn, m, defended=True)
            m["_pay"] = pay
    return m, ""


async def expire_due(bot=None) -> list[dict]:
    """Завершить просроченные атаки (удача вору). Возвращает завершённые."""
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM rb_steal WHERE status='active' AND deadline < now() "
                "FOR UPDATE SKIP LOCKED")
            for r in rows:
                m = dict(r)
                once = await _consume_once_shield(conn, m["victim"])
                pay = await _finish(conn, m, defended=once)
                m["auto_shield"] = once
                m["_pay"] = pay
                out.append(m)
    return out


async def _finish(conn, m: dict, defended: bool) -> int:
    """Провести исход атаки. defended=True -> штраф вору; иначе кража.
    Возвращает сумму перевода (pay). m — обычный dict."""
    thief, victim, amount = m["thief"], m["victim"], m["amount"]
    tb = await conn.fetchval(
        "SELECT amount FROM rb_balances WHERE tg_id=$1 AND currency='mushrooms'", thief) or 0
    vb = await conn.fetchval(
        "SELECT amount FROM rb_balances WHERE tg_id=$1 AND currency='mushrooms'", victim) or 0
    if defended:
        pay = min(amount, tb)
        if pay > 0:
            await db.apply(conn, thief, "mushrooms", -pay, "steal_penalty", f"stlp:{m['id']}")
            await db.apply(conn, victim, "mushrooms", pay, "steal_defended", f"stld:{m['id']}")
        await conn.execute(
            "UPDATE rb_steal SET status='defended', finished_at=now() WHERE id=$1", m["id"])
        return pay
    else:
        got = min(amount, vb)
        if got > 0:
            await db.apply(conn, victim, "mushrooms", -got, "stolen_from", f"stlf:{m['id']}")
            await db.apply(conn, thief, "mushrooms", got, "steal_success", f"stls:{m['id']}")
        await conn.execute(
            "UPDATE rb_steal SET status='success', finished_at=now() WHERE id=$1", m["id"])
        return got
