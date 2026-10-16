"""
Хеллоуин: проклятья и трик-о-трит (!буу).

Проклятья:
  • leak  — «Дырявый мешок»: в течение дня из баланса утекают грибы случайными
            порциями, но СУММАРНО не больше 5% от баланса на момент наложения.
  • hex   — «Порча»: ×2 неудача на всё (казино, рулетка, колесо). Как удача, но
            наоборот: делаем N бросков и берём ХУДШИЙ. Порчи суммируются
            (две порчи → худший из трёх).
  • fit   — «Припадок»: нельзя крутить бесплатный !шайн, и сообщения не идут в зачёт
            еженедельного конкурса, пока проклятье не снято / не прошло.

Снятие: в 00:00 по МСК (expires_at ставится на ближайшую полночь МСК) ИЛИ эликсиром
из магазина (снимает ОДНО случайное проклятье). Проклятья можно копить (несколько строк).

Трик-о-трит: игрок отвечает на чужое сообщение командой !буу (раз в день). У цели
5 минут: !Сладость → сладости (грибы/коины по шансам !шайн, вплоть до джекпота),
!Гадость → осознанно проклятье, молчание 5 минут → гарантированно проклятье.
"""
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone

import asyncpg

import db
import roulette

log = logging.getLogger("refbot")

MSK = timezone(timedelta(hours=3))

BOO_WINDOW_MIN = 5          # окно на ответ цели
LEAK_CAP_PCT = 0.05         # «дырявый мешок» утекает не больше 5% баланса

# код -> (эмодзи, название, короткое описание)
CURSE_KINDS = {
    "leak": ("🕳", "Дырявый мешок",
             "из баланса целый день утекают грибы (суммарно до 5% баланса)"),
    "hex":  ("💀", "Порча",
             "×2 неудача на всё — казино, рулетка, колесо дают худшее"),
    "fit":  ("😵", "Припадок",
             "нельзя крутить !шайн, сообщения не идут в зачёт конкурса"),
}


def _now_msk() -> datetime:
    return datetime.now(MSK)


def next_midnight_msk() -> datetime:
    """Ближайшая полночь по МСК, в UTC. Проклятье живёт до неё."""
    now = _now_msk()
    tomorrow = (now + timedelta(days=1)).date()
    midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=MSK)
    return midnight.astimezone(timezone.utc)


def kind_label(kind: str) -> str:
    e, name, _ = CURSE_KINDS.get(kind, ("🔮", kind, ""))
    return f"{e} {name}"


# ---------------- проклятья ----------------
async def apply_curse(uid: int, kind: str | None = None) -> dict:
    """Наложить проклятье (случайное, если kind не задан). Возвращает {kind,...}."""
    if kind is None:
        kind = secrets.choice(list(CURSE_KINDS.keys()))
    meta: dict = {}
    if kind == "leak":
        bal = (await db.balances(uid)).get("mushrooms", 0)
        cap = int(bal * LEAK_CAP_PCT)
        meta = {"cap": cap, "leaked": 0, "drip_no": 0}
    exp = next_midnight_msk()
    cid = await db.pool().fetchval(
        "INSERT INTO rb_curses (tg_id, kind, meta, expires_at) VALUES ($1,$2,$3,$4) RETURNING id",
        uid, kind, json.dumps(meta), exp)
    return {"id": cid, "kind": kind, "meta": meta, "expires_at": exp}


async def active(uid: int) -> list[dict]:
    # Таблицы ещё нет (не накатили setup.sql) — проклятий просто нет, не падаем.
    try:
        rows = await db.pool().fetch(
            "SELECT * FROM rb_curses WHERE tg_id=$1 AND expires_at>now() ORDER BY id", uid)
    except asyncpg.UndefinedTableError:
        return []
    out = []
    for r in rows:
        d = dict(r)
        d["meta"] = d["meta"] if isinstance(d["meta"], dict) else json.loads(d["meta"] or "{}")
        out.append(d)
    return out


async def has(uid: int, kind: str) -> bool:
    try:
        return bool(await db.pool().fetchval(
            "SELECT 1 FROM rb_curses WHERE tg_id=$1 AND kind=$2 AND expires_at>now() LIMIT 1",
            uid, kind))
    except asyncpg.UndefinedTableError:
        return False


async def hex_count(uid: int) -> int:
    """Сколько активных порч (для силы неудачи). 0 — порчи нет."""
    try:
        return await db.pool().fetchval(
            "SELECT count(*) FROM rb_curses WHERE tg_id=$1 AND kind='hex' AND expires_at>now()",
            uid) or 0
    except asyncpg.UndefinedTableError:
        return 0


async def remove_random(uid: int) -> dict | None:
    """Снять ОДНО случайное активное проклятье (эликсир). None — снимать нечего."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            r = await conn.fetchrow(
                "SELECT * FROM rb_curses WHERE tg_id=$1 AND expires_at>now() "
                "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED", uid)
            if not r:
                return None
            await conn.execute("DELETE FROM rb_curses WHERE id=$1", r["id"])
    return dict(r)


async def clear_expired() -> None:
    """Снести проклятья, у которых вышел срок (полночь МСК). Идемпотентно."""
    await db.pool().execute("DELETE FROM rb_curses WHERE expires_at<=now()")


async def leak_tick() -> list[tuple[int, int]]:
    """Один проход «дырявого мешка»: случайная порция грибов утекает с баланса,
    но суммарно не больше cap. Возвращает [(uid, сумма_за_тик)] для уведомлений."""
    drained: list[tuple[int, int]] = []
    rows = await db.pool().fetch(
        "SELECT * FROM rb_curses WHERE kind='leak' AND expires_at>now()")
    for r in rows:
        meta = r["meta"] if isinstance(r["meta"], dict) else json.loads(r["meta"] or "{}")
        cap = int(meta.get("cap", 0))
        leaked = int(meta.get("leaked", 0))
        remaining = cap - leaked
        if remaining <= 0:
            continue
        # «дырка» срабатывает не каждый тик — чтобы утекало неравномерно весь день
        if secrets.randbelow(100) >= 40:      # ~40% тиков
            continue
        uid = r["tg_id"]
        bal = (await db.balances(uid)).get("mushrooms", 0)
        if bal <= 0:
            continue
        # порция: случайная доля остатка, но не больше того, что есть на балансе
        step_cap = max(1, remaining // 3)
        drip = 1 + secrets.randbelow(step_cap)
        drip = min(drip, remaining, bal)
        if drip <= 0:
            continue
        drip_no = int(meta.get("drip_no", 0)) + 1
        try:
            async with db.pool().acquire() as conn:
                async with conn.transaction():
                    bal_now = await conn.fetchval(
                        "SELECT amount FROM rb_balances WHERE tg_id=$1 AND currency='mushrooms'",
                        uid) or 0
                    drip = min(drip, bal_now)
                    if drip <= 0:
                        continue
                    ok = await db.apply(conn, uid, "mushrooms", -drip, "curse_leak",
                                        f"leak:{r['id']}:{drip_no}")
                    if ok is None:
                        continue
                    meta["leaked"] = leaked + drip
                    meta["drip_no"] = drip_no
                    await conn.execute(
                        "UPDATE rb_curses SET meta=$1 WHERE id=$2", json.dumps(meta), r["id"])
            drained.append((uid, drip))
        except Exception as e:
            log.warning("leak_tick %s: %s", uid, e)
    return drained


# ---------------- !буу (трик-о-трит) ----------------
async def can_boo_today(uid: int) -> bool:
    from config import UNLIMITED_SPIN_IDS
    if uid in UNLIMITED_SPIN_IDS:
        return True
    row = await db.pool().fetchval("SELECT last_day FROM rb_boo_cd WHERE tg_id=$1", uid)
    return (not row) or row < _now_msk().date()


async def pending_for(uid: int) -> dict | None:
    r = await db.pool().fetchrow(
        "SELECT * FROM rb_boo WHERE target=$1 AND status='pending' "
        "AND deadline>now() LIMIT 1", uid)
    return dict(r) if r else None


async def start_boo(caster: int, target: int, chat_id: int) -> tuple[dict | None, str]:
    """Сказать «бу!» цели. Проверки, окно 5 минут."""
    if caster == target:
        return None, "Самого себя не напугаешь 🙂"
    if await db.is_banned(caster):
        return None, "Аккаунт заблокирован."
    if not await can_boo_today(caster):
        return None, "Ты уже пугал сегодня. Следующий «бу!» — после 00:00 по МСК."
    if await pending_for(target):
        return None, "На эту жертву уже висит «бу!» — дождись, пока ответит."
    dl = datetime.now(timezone.utc) + timedelta(minutes=BOO_WINDOW_MIN)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO rb_boo_cd (tg_id, last_day) VALUES ($1,$2) "
                "ON CONFLICT (tg_id) DO UPDATE SET last_day=$2", caster, _now_msk().date())
            bid = await conn.fetchval(
                "INSERT INTO rb_boo (caster, target, chat_id, deadline) "
                "VALUES ($1,$2,$3,$4) RETURNING id", caster, target, chat_id, dl)
            m = await conn.fetchrow("SELECT * FROM rb_boo WHERE id=$1", bid)
    return dict(m), ""


async def treat(uid: int) -> tuple[dict | None, str]:
    """Цель выбрала !Сладость. Выдаём сладости по шансам !шайн (валюта игрока)."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            r = await conn.fetchrow(
                "SELECT * FROM rb_boo WHERE target=$1 AND status='pending' AND deadline>now() "
                "FOR UPDATE", uid)
            if not r:
                return None, "Тебя сейчас никто не пугал."
            user = await db.get_user(uid)
            cur = user["currency"]
            amount, _mega = roulette.roll(cur)
            amount = max(1, amount)
            await db.apply(conn, uid, cur, amount, "boo_treat", f"boo:{r['id']}:treat")
            await conn.execute(
                "UPDATE rb_boo SET status='treat', finished_at=now() WHERE id=$1", r["id"])
            m = dict(r)
            m["currency"] = cur
            m["amount"] = amount
    return m, ""


async def trick(uid: int, deliberate: bool) -> tuple[dict | None, str]:
    """Проклясть цель: !Гадость (deliberate=True) или истёкшее окно (False)."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            if deliberate:
                r = await conn.fetchrow(
                    "SELECT * FROM rb_boo WHERE target=$1 AND status='pending' AND deadline>now() "
                    "FOR UPDATE", uid)
            else:
                r = await conn.fetchrow(
                    "SELECT * FROM rb_boo WHERE id=$1 AND status='pending' FOR UPDATE", uid)
            if not r:
                return None, "Нечего проклинать."
            await conn.execute(
                "UPDATE rb_boo SET status='trick', finished_at=now() WHERE id=$1", r["id"])
    curse = await apply_curse(r["target"])
    m = dict(r)
    m["curse"] = curse
    return m, ""


async def expire_boos() -> list[dict]:
    """Истёкшие «бу!» → гарантированное проклятье. Возвращает завершённые."""
    out = []
    rows = await db.pool().fetch(
        "SELECT * FROM rb_boo WHERE status='pending' AND deadline<now()")
    for r in rows:
        m, err = await trick(r["id"], deliberate=False)
        if m:
            out.append(m)
    return out
