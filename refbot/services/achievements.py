"""
Достижения: проверка прогресса и выдача (Блок 5).
Пока — обновление прогресса достижений по счётчику (без UI выдачи).
"""
import db


async def check(uid: int, counter_type: str, value: int):
    """При изменении счётчика — обновить прогресс достижений с этим триггером.
    Помечает completed при достижении порога (claim — отдельно, в UI)."""
    try:
        achs = await db.pool().fetch(
            "SELECT id, trigger_target FROM rb_achievements "
            "WHERE active AND trigger_type=$1", counter_type)
        for a in achs:
            completed = value >= a["trigger_target"]
            prog = min(value, a["trigger_target"])
            await db.pool().execute(
                "INSERT INTO rb_user_achievements (tg_id, ach_id, progress, completed, completed_at) "
                "VALUES ($1,$2,$3,$4, CASE WHEN $4 THEN now() ELSE NULL END) "
                "ON CONFLICT (tg_id, ach_id) DO UPDATE "
                "SET progress = GREATEST(rb_user_achievements.progress, $3), "
                "    completed = rb_user_achievements.completed OR $4, "
                "    completed_at = COALESCE(rb_user_achievements.completed_at, "
                "                            CASE WHEN $4 THEN now() ELSE NULL END)",
                uid, a["id"], prog, completed)
    except Exception:
        import logging
        logging.getLogger("refbot").warning("achievements.check failed for %s/%s", uid, counter_type)


async def list_for_user(uid: int, only_public: bool = False) -> list[dict]:
    """Все активные достижения с прогрессом игрока. Скрытые тоже (но условия прячем в UI)."""
    rows = await db.pool().fetch(
        "SELECT a.*, COALESCE(ua.progress,0) AS progress, "
        "COALESCE(ua.completed,false) AS completed, COALESCE(ua.claimed,false) AS claimed "
        "FROM rb_achievements a "
        "LEFT JOIN rb_user_achievements ua ON ua.ach_id=a.id AND ua.tg_id=$1 "
        "WHERE a.active " + ("AND NOT a.hidden " if only_public else "") +
        "ORDER BY a.id", uid)
    return [dict(r) for r in rows]


async def unclaimed_count(uid: int) -> int:
    """Сколько выполненных, но не собранных достижений (для бейджа «Достижения (N)»)."""
    return await db.pool().fetchval(
        "SELECT count(*) FROM rb_user_achievements "
        "WHERE tg_id=$1 AND completed AND NOT claimed", uid) or 0


async def get_ach(ach_id: int) -> dict | None:
    row = await db.pool().fetchrow("SELECT * FROM rb_achievements WHERE id=$1", ach_id)
    return dict(row) if row else None


async def claim(uid: int, ach_id: int) -> tuple[bool, str, list]:
    """Собрать награду за выполненное достижение. Идемпотентно.
    Возвращает (успех, ошибка, список_выданных_наград для показа)."""
    import json
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            ua = await conn.fetchrow(
                "SELECT * FROM rb_user_achievements WHERE tg_id=$1 AND ach_id=$2 FOR UPDATE",
                uid, ach_id)
            if not ua or not ua["completed"]:
                return False, "Достижение ещё не выполнено.", []
            if ua["claimed"]:
                return False, "Награда уже собрана.", []
            ach = await conn.fetchrow("SELECT * FROM rb_achievements WHERE id=$1", ach_id)
            rewards = ach["rewards"] if isinstance(ach["rewards"], list) else json.loads(ach["rewards"])
            await conn.execute(
                "UPDATE rb_user_achievements SET claimed=true, claimed_at=now() "
                "WHERE tg_id=$1 AND ach_id=$2", uid, ach_id)
    # выдаём награды вне транзакции достижения (каждая своя идемпотентность)
    given = await _grant_rewards(uid, ach_id, rewards)
    # мета-счётчик: достижение выполнено (собрано)
    try:
        from services import counters as _cnt
        await _cnt.bump(uid, _cnt.C_ACH_DONE)
    except Exception:
        pass
    return True, "", given


async def _grant_rewards(uid: int, ach_id: int, rewards: list) -> list[str]:
    """Выдать список наград. Возвращает человекочитаемые строки выданного."""
    from datetime import datetime, timezone, timedelta
    out = []
    for i, rw in enumerate(rewards):
        t = rw.get("type")
        idem = f"ach:{ach_id}:{uid}:{i}"
        try:
            if t in ("mushrooms", "coins", "shimcoins", "revive", "max", "partials"):
                amount = int(rw.get("amount", 0))
                await db.apply_admin(uid, t, amount, "achievement", idem)
                out.append(f"+{amount} {t}")
            elif t == "title":
                from services import titles
                tid = rw.get("title_id")
                if not tid and rw.get("title_name"):
                    tid = await titles.create_title(rw["title_name"], False, 0)
                if tid:
                    await titles.grant_title(uid, tid, None)
                    out.append("🏅 титул")
            elif t == "emoji":
                emo = rw.get("emoji")
                if emo:
                    await db.pool().execute(
                        "INSERT INTO rb_user_emojis (tg_id, emoji, source) VALUES ($1,$2,'achievement') "
                        "ON CONFLICT (tg_id, emoji) DO NOTHING", uid, emo)
                    out.append(f"😎 эмодзи {emo}")
            elif t == "luck":
                minutes = int(rw.get("minutes", 15))
                scope = rw.get("scope", "all")
                mult = float(rw.get("mult", 2))
                await _add_luck(uid, scope, mult, minutes)
                out.append(f"🍀 удача ×{mult:g} на {minutes} мин")
            elif t == "discount":
                percent = int(rw.get("percent", 10))
                target = rw.get("target", "shop")
                await db.pool().execute(
                    "INSERT INTO rb_active_bonuses (tg_id, bonus_type, scope, multiplier, payload) "
                    "VALUES ($1,'discount',$2,1,$3)", uid, target,
                    __import__("json").dumps({"percent": percent}))
                out.append(f"🏷 скидка {percent}%")
        except Exception:
            import logging
            logging.getLogger("refbot").warning("reward grant failed: %s", t)
    return out


async def _add_luck(uid: int, scope: str, mult: float, minutes: int):
    """Добавить/продлить удачу. Одна запись на scope — суммируем ВРЕМЯ, множитель = макс."""
    import json
    from datetime import datetime, timezone, timedelta
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            ex = await conn.fetchrow(
                "SELECT * FROM rb_active_bonuses WHERE tg_id=$1 AND bonus_type='luck' AND scope=$2 "
                "FOR UPDATE", uid, scope)
            now = datetime.now(timezone.utc)
            if ex and ex["expires_at"] and ex["expires_at"] > now:
                # суммируем время, множитель — максимум
                new_exp = ex["expires_at"] + timedelta(minutes=minutes)
                new_mult = max(float(ex["multiplier"]), mult)
                await conn.execute(
                    "UPDATE rb_active_bonuses SET expires_at=$1, multiplier=$2 WHERE id=$3",
                    new_exp, new_mult, ex["id"])
            else:
                await conn.execute(
                    "INSERT INTO rb_active_bonuses (tg_id, bonus_type, scope, multiplier, expires_at) "
                    "VALUES ($1,'luck',$2,$3,$4)", uid, scope, mult,
                    now + timedelta(minutes=minutes))
