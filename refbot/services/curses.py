"""
Хеллоуин: проклятья и трик-о-трит (!буу).

Проклятья (каждое — ОТДЕЛЬНЫЙ организм, НЕ суммируются в один эффект; можно налепить
сколько угодно, хоть сто дыр — каждая работает сама по себе):
  • leak    — «Дырявый мешок»: из баланса утекают грибы случайными порциями,
              суммарно не больше 5% от баланса на момент наложения (у каждой дыры —
              свой независимый счётчик и свой лимит).
  • hex     — «Порча»: ×2 неудача на всё (казино/рулетка/колесо берут худшее).
  • fit     — «Припадок»: нельзя крутить !шайн, сообщения не идут в зачёт конкурса.
  • pumpkin — «Тыквоголовый»: имя принудительно «Тыквоголовый», эмодзи 🎃, сменить
              нельзя. После снятия прежние имя и эмодзи возвращаются сами.
  • evil    — «Сглаз»: максимальный хардкор. Может висеть ТОЛЬКО ОДИН. Снимается не
              одним эликсиром, а ПЯТЬЮ. Пока висит: щит не работает, ×2 неудача на всё,
              тебя можно обворовывать сверх лимита (один вор — не больше 2 раз/день),
              от !буу не защититься (сладостей не выпросить), а если сам пугаешь —
              50% проклясть себя и 70% (вместо 20%) что жертва вернёт проклятье тебе.

Снятие: в 00:00 по МСК (expires_at — ближайшая полночь МСК) или эликсиром из магазина
(снимает ОДНО случайное; по Сглазу — снимает один «заряд» из пяти).
"""
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone

import asyncpg

import db
import roulette
from services.profile import PUMPKIN_NICK, PUMPKIN_EMOJI

log = logging.getLogger("refbot")

MSK = timezone(timedelta(hours=3))

BOO_WINDOW_MIN = 5          # окно на ответ цели
LEAK_CAP_PCT = 0.05         # «дырявый мешок»: до 5% баланса за всё проклятье
LEAK_HARD_CAP = 10_000      # но в абсолюте — не больше 10к грибов за одну дыру
LEAK_MAX_DRIP = 500         # и за один «капёж» — не больше 500 (чтобы текло незаметно)
LEAK_DRIP_PCT = 25          # шанс, что дыра капнет за проход воркера
EVIL_HP = 5                 # сколько эликсиров нужно, чтобы снять Сглаз
TREAT_BONUS_PCT = 20        # шанс выбить эликсир в довесок к сладостям
REFLECT_PCT = 20            # шанс вернуть проклятье отправителю при !Гадость
REFLECT_PCT_EVIL = 70       # то же, если отправитель под Сглазом
SELF_CURSE_PCT = 50         # шанс проклясть себя, если пугаешь под Сглазом
EVIL_STEAL_CAP = 2          # сколько раз один вор может обворовать сглаженного за день
BOO_MAX_DAILY = 15          # потолок дневных !буу (зелья жажды поднимают лимит до него)
BOO_REWARD_PCT = 20         # шанс выпасть зелью жажды при осознанной !Гадость

# код -> (эмодзи, название, короткое описание)
CURSE_KINDS = {
    "leak":    ("🕳", "Дырявый мешок", "из баланса утекают грибы (до 5% за проклятие)"),
    "hex":     ("💀", "Порча", "×2 неудача — казино, рулетка, колесо дают худшее"),
    "fit":     ("😵", "Припадок", "нельзя крутить !шайн, сообщения не в зачёт конкурса"),
    "pumpkin": ("🎃", "Тыквоголовый", "имя и эмодзи принудительно меняются до снятия"),
    "glut":    ("🤤", "Обжора", "съедает 1% с каждого выигрыша и начисления"),
    "evil":    ("👁", "Сглаз", "хардкор: щит не работает, ×2 неудача, снимается 5 эликсирами"),
}

# веса для случайного проклятья от !буу (Сглаз — редкий)
_WEIGHTS = {"leak": 22, "hex": 22, "fit": 22, "pumpkin": 17, "glut": 12, "evil": 5}


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


def _weighted_kind(exclude: tuple = ()) -> str:
    pool = [(k, w) for k, w in _WEIGHTS.items() if k not in exclude]
    total = sum(w for _, w in pool)
    r = secrets.randbelow(total) + 1
    acc = 0
    for k, w in pool:
        acc += w
        if r <= acc:
            return k
    return pool[-1][0]


# ---------------- наложение ----------------
async def apply_curse(uid: int, kind: str | None = None) -> dict:
    """Наложить проклятье (случайное, если kind не задан). Возвращает {kind, ...}.
    Сглаз — только один: если уже висит, выпадает другое проклятье (удар не пропадает)."""
    if kind is None:
        kind = _weighted_kind()
    if kind == "evil" and await has(uid, "evil"):
        kind = _weighted_kind(exclude=("evil",))   # второй Сглаз нельзя — берём другое

    meta: dict = {}
    if kind == "leak":
        bal = (await db.balances(uid)).get("mushrooms", 0)
        cap = min(int(bal * LEAK_CAP_PCT), LEAK_HARD_CAP)
        meta = {"cap": cap, "leaked": 0, "drip_no": 0}
    elif kind == "evil":
        meta = {"hp": EVIL_HP}
    souvenir = False
    if kind == "pumpkin":
        meta, souvenir = await _pumpkin_on(uid)

    exp = next_midnight_msk()
    cid = await db.pool().fetchval(
        "INSERT INTO rb_curses (tg_id, kind, meta, expires_at) VALUES ($1,$2,$3,$4) RETURNING id",
        uid, kind, json.dumps(meta), exp)
    res = {"id": cid, "kind": kind, "meta": meta, "expires_at": exp}
    if kind == "pumpkin":
        res["souvenir"] = souvenir
    return res


async def _pumpkin_on(uid: int) -> tuple[dict, bool]:
    """Включить тыквоголовость: сохранить прежние имя/эмодзи и подменить их.
    Если тыква уже висит — берём сохранённое из неё (чтобы не потерять оригинал).
    За ПЕРВУЮ в жизни тыкву выдаём сувенир: титул «Тыквоголовый» + эмодзи 🎃 навсегда.
    Возвращает (meta, выдан_ли_сувенир)."""
    rows = await db.pool().fetch(
        "SELECT meta FROM rb_curses WHERE tg_id=$1 AND kind='pumpkin' AND expires_at>now() LIMIT 1",
        uid)
    if rows:
        m = rows[0]["meta"]
        m = m if isinstance(m, dict) else json.loads(m or "{}")
        saved_nick = m.get("saved_nick")
        saved_emoji = m.get("saved_emoji")
        saved_set = m.get("saved_nick_set", True)
    else:
        r = await db.pool().fetchrow(
            "SELECT nickname, nickname_set, active_emoji FROM rb_users WHERE tg_id=$1", uid)
        saved_nick = r["nickname"] if r else None
        saved_emoji = r["active_emoji"] if r else None
        saved_set = bool(r["nickname_set"]) if r else False

    # сувенир за первую тыкву — по владению титулом «Тыквоголовый»
    souvenir = False
    try:
        from services import titles
        tid = await titles.create_title(PUMPKIN_NICK, False, 0)
        has_title = await db.pool().fetchval(
            "SELECT 1 FROM rb_user_titles WHERE tg_id=$1 AND title_id=$2", uid, tid)
        if not has_title:
            await titles.grant_title(uid, tid, None)
            await db.pool().execute(
                "INSERT INTO rb_user_emojis (tg_id, emoji, source) VALUES ($1,$2,'curse') "
                "ON CONFLICT (tg_id, emoji) DO NOTHING", uid, PUMPKIN_EMOJI)
            souvenir = True
    except Exception as e:
        log.warning("pumpkin souvenir %s: %s", uid, e)

    await db.pool().execute(
        "UPDATE rb_users SET nickname=$1, nickname_set=TRUE, active_emoji=$2 WHERE tg_id=$3",
        PUMPKIN_NICK, PUMPKIN_EMOJI, uid)
    return {"saved_nick": saved_nick, "saved_emoji": saved_emoji, "saved_nick_set": saved_set}, souvenir


async def _pumpkin_restore(conn, uid: int, meta: dict) -> None:
    """Вернуть прежние имя, флаг nickname_set и эмодзи (после снятия последней тыквы)."""
    await conn.execute(
        "UPDATE rb_users SET nickname=$1, nickname_set=$2, active_emoji=$3 WHERE tg_id=$4",
        meta.get("saved_nick"), bool(meta.get("saved_nick_set", False)),
        meta.get("saved_emoji"), uid)


# ---------------- чтение ----------------
async def active(uid: int) -> list[dict]:
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


async def jinxed(uid: int) -> bool:
    """Действует ли на игрока ×2 неудача (Порча ИЛИ Сглаз)."""
    try:
        return bool(await db.pool().fetchval(
            "SELECT 1 FROM rb_curses WHERE tg_id=$1 AND kind IN ('hex','evil') "
            "AND expires_at>now() LIMIT 1", uid))
    except asyncpg.UndefinedTableError:
        return False


async def summary(uid: int) -> str:
    """Короткий список активных проклятий для профиля/инвентаря (или пусто)."""
    rows = await active(uid)
    if not rows:
        return ""
    from collections import Counter
    cnt = Counter(r["kind"] for r in rows)
    parts = []
    for kind, n in cnt.items():
        lbl = kind_label(kind)
        parts.append(f"{lbl} ×{n}" if n > 1 else lbl)
    return ", ".join(parts)


# ---------------- снятие ----------------
async def elixir_remove(conn, uid: int) -> str | None:
    """Снять одно случайное проклятье (эликсир), работает в переданной транзакции.
    Сглаз снимает по одному заряду (нужно 5). Тыкву снимает с возвратом имени/эмодзи.
    Возвращает: kind снятого, 'evil_chip' (заряд Сглаза), или None (снимать нечего)."""
    r = await conn.fetchrow(
        "SELECT * FROM rb_curses WHERE tg_id=$1 AND expires_at>now() "
        "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED", uid)
    if not r:
        return None
    kind = r["kind"]
    meta = r["meta"] if isinstance(r["meta"], dict) else json.loads(r["meta"] or "{}")
    if kind == "evil":
        hp = int(meta.get("hp", EVIL_HP)) - 1
        if hp > 0:
            meta["hp"] = hp
            await conn.execute("UPDATE rb_curses SET meta=$1 WHERE id=$2",
                               json.dumps(meta), r["id"])
            return "evil_chip"
        await conn.execute("DELETE FROM rb_curses WHERE id=$1", r["id"])
        return "evil"
    await conn.execute("DELETE FROM rb_curses WHERE id=$1", r["id"])
    if kind == "pumpkin":
        left = await conn.fetchval(
            "SELECT count(*) FROM rb_curses WHERE tg_id=$1 AND kind='pumpkin' AND expires_at>now()",
            uid)
        if not left:
            await _pumpkin_restore(conn, uid, meta)
    return kind


async def evil_hp(uid: int) -> int:
    """Сколько зарядов осталось у Сглаза (0 — Сглаза нет)."""
    r = await db.pool().fetchrow(
        "SELECT meta FROM rb_curses WHERE tg_id=$1 AND kind='evil' AND expires_at>now() LIMIT 1",
        uid)
    if not r:
        return 0
    m = r["meta"] if isinstance(r["meta"], dict) else json.loads(r["meta"] or "{}")
    return int(m.get("hp", EVIL_HP))


async def clear_expired() -> None:
    """Снять проклятья с вышедшим сроком (полночь МСК). Тыквам возвращаем имя/эмодзи."""
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                # тыквы, которые истекают — вернуть оригинал тем, у кого не осталось живых тыкв
                exp_pk = await conn.fetch(
                    "SELECT DISTINCT ON (tg_id) tg_id, meta FROM rb_curses "
                    "WHERE kind='pumpkin' AND expires_at<=now() ORDER BY tg_id, id")
                for r in exp_pk:
                    still = await conn.fetchval(
                        "SELECT count(*) FROM rb_curses WHERE tg_id=$1 AND kind='pumpkin' "
                        "AND expires_at>now()", r["tg_id"])
                    if not still:
                        m = r["meta"] if isinstance(r["meta"], dict) else json.loads(r["meta"] or "{}")
                        await _pumpkin_restore(conn, r["tg_id"], m)
                await conn.execute("DELETE FROM rb_curses WHERE expires_at<=now()")
    except asyncpg.UndefinedTableError:
        pass


async def leak_tick() -> list[tuple[int, int]]:
    """Проход всех «дырявых мешков»: у каждой дыры свой независимый слив (свой cap)."""
    drained: list[tuple[int, int]] = []
    try:
        rows = await db.pool().fetch(
            "SELECT * FROM rb_curses WHERE kind='leak' AND expires_at>now()")
    except asyncpg.UndefinedTableError:
        return drained
    for r in rows:
        meta = r["meta"] if isinstance(r["meta"], dict) else json.loads(r["meta"] or "{}")
        # жёсткий потолок применяем и к уже висящим дырам (старые могли иметь большой cap)
        cap = min(int(meta.get("cap", 0)), LEAK_HARD_CAP)
        leaked = int(meta.get("leaked", 0))
        remaining = cap - leaked
        if remaining <= 0:
            continue
        if secrets.randbelow(100) >= LEAK_DRIP_PCT:   # дыра капает редко
            continue
        uid = r["tg_id"]
        step_cap = max(1, remaining // 12)            # мелкими порциями
        drip = 1 + secrets.randbelow(step_cap)
        drip = min(drip, LEAK_MAX_DRIP, remaining)    # и не больше 500 за раз
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
async def boo_cap(uid: int) -> int:
    """Дневной лимит !буу: 1 база + зелья жажды (boo_bonus), максимум BOO_MAX_DAILY."""
    bonus = await db.pool().fetchval("SELECT boo_bonus FROM rb_users WHERE tg_id=$1", uid) or 0
    return min(1 + int(bonus), BOO_MAX_DAILY)


async def can_boo_today(uid: int) -> bool:
    from config import UNLIMITED_SPIN_IDS
    if uid in UNLIMITED_SPIN_IDS:
        return True
    row = await db.pool().fetchrow(
        "SELECT last_day, used FROM rb_boo_cd WHERE tg_id=$1", uid)
    if not row or row["last_day"] < _now_msk().date():
        return True                       # новый день — счётчик обнулится при использовании
    return row["used"] < await boo_cap(uid)


async def pending_for(uid: int) -> dict | None:
    r = await db.pool().fetchrow(
        "SELECT * FROM rb_boo WHERE target=$1 AND status='pending' AND deadline>now() LIMIT 1",
        uid)
    return dict(r) if r else None


async def _mark_boo_used(conn, caster: int) -> None:
    today = _now_msk().date()
    # новый день — счётчик с 1, тот же день — +1
    await conn.execute(
        "INSERT INTO rb_boo_cd (tg_id, last_day, used) VALUES ($1,$2,1) "
        "ON CONFLICT (tg_id) DO UPDATE SET "
        "used = CASE WHEN rb_boo_cd.last_day < $2 THEN 1 ELSE rb_boo_cd.used + 1 END, "
        "last_day = $2", caster, today)


async def start_boo(caster: int, target: int, chat_id: int) -> tuple[dict | None, str]:
    """Сказать «бу!» цели. Если пугающий под Сглазом — 50% проклясть себя."""
    if caster == target:
        return None, "Самого себя не напугаешь 🙂"
    if await db.is_banned(caster):
        return None, "Аккаунт заблокирован."
    if not await can_boo_today(caster):
        cap = await boo_cap(caster)
        return None, (f"На сегодня !буу закончились (лимит {cap}/день). "
                      f"Приходи после 00:00 по МСК.")
    if await pending_for(target):
        return None, "На эту жертву уже висит «бу!» — дождись, пока ответит."

    # Сглаз у пугающего: 50% обернётся против него самого.
    if await has(caster, "evil") and secrets.randbelow(100) < SELF_CURSE_PCT:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await _mark_boo_used(conn, caster)
        curse = await apply_curse(caster)
        return {"backfire": True, "caster": caster, "curse": curse}, ""

    # Сглаз у цели: от !буу не защититься — проклятие МГНОВЕННОЕ, без окна и выбора.
    if await has(target, "evil"):
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await _mark_boo_used(conn, caster)
        curse = await apply_curse(target)
        return {"instant_curse": True, "caster": caster, "target": target, "curse": curse}, ""

    dl = datetime.now(timezone.utc) + timedelta(minutes=BOO_WINDOW_MIN)
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await _mark_boo_used(conn, caster)
            bid = await conn.fetchval(
                "INSERT INTO rb_boo (caster, target, chat_id, deadline) "
                "VALUES ($1,$2,$3,$4) RETURNING id", caster, target, chat_id, dl)
            m = await conn.fetchrow("SELECT * FROM rb_boo WHERE id=$1", bid)
    return dict(m), ""


async def treat(uid: int) -> tuple[dict | None, str]:
    """Цель выбрала !Сладость. Под Сглазом нельзя — только проклятье.
    Выдаём сладости по шансам !шайн + 20% шанс эликсира в довесок."""
    if await has(uid, "evil"):
        return None, "Под 👁 Сглазом не выпросить сладости — только проклятие."
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
            bonus_elixir = secrets.randbelow(100) < TREAT_BONUS_PCT
            if bonus_elixir:
                await conn.execute(
                    "INSERT INTO rb_inventory (tg_id, item_type, payload) "
                    "VALUES ($1,'uncurse',$2)", uid, json.dumps({"count": 1}))
            await conn.execute(
                "UPDATE rb_boo SET status='treat', finished_at=now() WHERE id=$1", r["id"])
            m = dict(r)
            m["currency"] = cur
            m["amount"] = amount
            m["bonus_elixir"] = bonus_elixir
    return m, ""


async def trick(uid_or_bid: int, deliberate: bool) -> tuple[dict | None, str]:
    """Проклясть по «бу!»: !Гадость (deliberate=True, по target) или таймаут (по id).
    !Гадость: с шансом вернуть проклятье отправителю (20%, под Сглазом отправителя 70%)."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            if deliberate:
                r = await conn.fetchrow(
                    "SELECT * FROM rb_boo WHERE target=$1 AND status='pending' AND deadline>now() "
                    "FOR UPDATE", uid_or_bid)
            else:
                r = await conn.fetchrow(
                    "SELECT * FROM rb_boo WHERE id=$1 AND status='pending' FOR UPDATE", uid_or_bid)
            if not r:
                return None, "Нечего проклинать."
            await conn.execute(
                "UPDATE rb_boo SET status='trick', finished_at=now() WHERE id=$1", r["id"])
    caster, target = r["caster"], r["target"]
    reflected = False
    reward = False
    if deliberate:
        chance = REFLECT_PCT_EVIL if await has(caster, "evil") else REFLECT_PCT
        reflected = secrets.randbelow(100) < chance
        # за смелость (осознанная !Гадость) — шанс на зелье жажды тому, кто выбрал Гадость
        if secrets.randbelow(100) < BOO_REWARD_PCT:
            await db.pool().execute(
                "INSERT INTO rb_inventory (tg_id, item_type, payload) VALUES ($1,'boo_charge','{}')",
                target)
            reward = True
    victim = caster if reflected else target
    curse = await apply_curse(victim)
    m = dict(r)
    m.update(caster=caster, target=target, reflected=reflected, victim=victim,
             curse=curse, reward=reward)
    return m, ""


async def expire_boos() -> list[dict]:
    """Истёкшие «бу!» → гарантированное проклятье цели (без рефлексии)."""
    out = []
    try:
        rows = await db.pool().fetch(
            "SELECT id FROM rb_boo WHERE status='pending' AND deadline<now()")
    except asyncpg.UndefinedTableError:
        return out
    for r in rows:
        m, err = await trick(r["id"], deliberate=False)
        if m:
            out.append(m)
    return out


# ---------------- Сглаз: воровство ----------------
async def evil_steal_count_today(thief: int, victim: int) -> int:
    """Сколько раз этот вор уже обворовывал сглаженную жертву сегодня (МСК)."""
    start = datetime(_now_msk().year, _now_msk().month, _now_msk().day, tzinfo=MSK)
    return await db.pool().fetchval(
        "SELECT count(*) FROM rb_steal WHERE thief=$1 AND victim=$2 AND created_at>=$3",
        thief, victim, start.astimezone(timezone.utc)) or 0
