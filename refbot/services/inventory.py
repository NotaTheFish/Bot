"""
Инвентарь и активные бонусы: удача (×2 к шансам), скидка.
Удача суммирует время, множитель = максимум для scope.
Проверка активной удачи для игр — luck_multiplier(uid, scope).
"""
import json
from datetime import datetime, timezone, timedelta

import db


async def inventory(uid: int) -> list[dict]:
    """Неиспользованные предметы инвентаря."""
    rows = await db.pool().fetch(
        "SELECT * FROM rb_inventory WHERE tg_id=$1 AND NOT used ORDER BY got_at", uid)
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = d["payload"] if isinstance(d["payload"], dict) else json.loads(d["payload"])
        out.append(d)
    return out


async def activate(uid: int, inv_id: int) -> tuple[bool, str]:
    """Активировать предмет из инвентаря (удача/скидка)."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            item = await conn.fetchrow(
                "SELECT * FROM rb_inventory WHERE id=$1 AND tg_id=$2 AND NOT used FOR UPDATE",
                inv_id, uid)
            if not item:
                return False, "Предмет недоступен."
            p = item["payload"] if isinstance(item["payload"], dict) else json.loads(item["payload"])
            t = item["item_type"]
            if t == "luck":
                await _add_luck_conn(conn, uid, p.get("scope", "all"),
                                     float(p.get("mult", 2)), int(p.get("minutes", 15)))
            elif t == "discount":
                await conn.execute(
                    "INSERT INTO rb_active_bonuses (tg_id, bonus_type, scope, multiplier, payload) "
                    "VALUES ($1,'discount',$2,1,$3)", uid, p.get("target", "shop"),
                    json.dumps({"percent": p.get("percent", 10)}))
            await conn.execute(
                "UPDATE rb_inventory SET used=true, used_at=now() WHERE id=$1", inv_id)
    # счётчик активаций удачи
    if t == "luck":
        try:
            from services import counters as _cnt
            await _cnt.bump(uid, _cnt.C_LUCK_USED)
        except Exception:
            pass
    return True, ""


async def _add_luck_conn(conn, uid: int, scope: str, mult: float, minutes: int):
    """Добавить/продлить удачу в существующей транзакции. Суммирует время, макс множитель."""
    ex = await conn.fetchrow(
        "SELECT * FROM rb_active_bonuses WHERE tg_id=$1 AND bonus_type='luck' AND scope=$2 "
        "FOR UPDATE", uid, scope)
    now = datetime.now(timezone.utc)
    if ex and ex["expires_at"] and ex["expires_at"] > now:
        new_exp = ex["expires_at"] + timedelta(minutes=minutes)
        new_mult = max(float(ex["multiplier"]), mult)
        await conn.execute(
            "UPDATE rb_active_bonuses SET expires_at=$1, multiplier=$2 WHERE id=$3",
            new_exp, new_mult, ex["id"])
    else:
        await conn.execute(
            "INSERT INTO rb_active_bonuses (tg_id, bonus_type, scope, multiplier, expires_at) "
            "VALUES ($1,'luck',$2,$3,$4)", uid, scope, mult, now + timedelta(minutes=minutes))


async def luck_multiplier(uid: int, scope: str) -> float:
    """Активный множитель удачи для игры этого scope. Берёт МАКС из подходящих
    (конкретный scope + 'all'). 1.0 если удачи нет. Просроченные игнорируются."""
    rows = await db.pool().fetch(
        "SELECT multiplier FROM rb_active_bonuses WHERE tg_id=$1 AND bonus_type='luck' "
        "AND scope IN ($2,'all') AND (expires_at IS NULL OR expires_at > now())",
        uid, scope)
    if not rows:
        return 1.0
    return max(float(r["multiplier"]) for r in rows)


async def active_bonuses(uid: int) -> list[dict]:
    """Активные (не просроченные) бонусы для показа игроку."""
    rows = await db.pool().fetch(
        "SELECT * FROM rb_active_bonuses WHERE tg_id=$1 "
        "AND (expires_at IS NULL OR expires_at > now()) ORDER BY expires_at", uid)
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = d["payload"] if isinstance(d["payload"], dict) else json.loads(d["payload"] or "{}")
        out.append(d)
    return out


async def cleanup_expired():
    """Удалить просроченные бонусы (вызывать периодически)."""
    await db.pool().execute(
        "DELETE FROM rb_active_bonuses WHERE expires_at IS NOT NULL AND expires_at < now()")
