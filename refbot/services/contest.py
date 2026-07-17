"""
Еженедельный конкурс по активности.

Ключевое решение: НИЧЕГО не обнуляется. Счётчики пишутся с ключом period_start,
поэтому новая неделя — просто новая строка, а старая лежит нетронутой сколько угодно.
Иначе «обнуляем в 00:00, но билеты живут до нажатия кнопки» было бы неразрешимо:
данные для розыгрыша умирали бы раньше, чем админ дойдёт до кнопки.

Периоды: пн 00:00 .. вс 23:59:59 в зоне CONTEST_TZ.
Для тестов CONTEST_TEST_MINUTES>0 делает «неделю» длиной в N минут — вся остальная
логика при этом не меняется, поэтому тест проверяет боевой код, а не свою копию.
"""
import logging
import secrets
import time as _time
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import asyncpg

import db

log = logging.getLogger(__name__)
from config import (CONTEST_MIN_MSGS, CONTEST_MSGS_PER_TICKET, CONTEST_PRIZE,
                    CONTEST_TEST_MINUTES, CONTEST_TZ)

TZ = ZoneInfo(CONTEST_TZ)

# --- готовность схемы ---
# Тот же урок, что с rb_settings: отсутствие таблицы не должно убивать хендлер
# молча. Проверяем один раз, при отсутствии перепроверяем раз в минуту — накатил
# setup.sql, и конкурс заработал без перезапуска бота.
_ready: bool | None = None
_last_check = 0.0


async def tables_ready() -> bool:
    global _ready, _last_check
    if _ready:
        return True
    now_m = _time.monotonic()
    if _ready is False and now_m - _last_check < 60:
        return False
    _last_check = now_m
    try:
        await db.pool().fetchval("SELECT 1 FROM rb_contest_chats LIMIT 1")
        if _ready is False:
            log.warning("Таблицы конкурса появились — конкурс включён.")
        _ready = True
    except asyncpg.UndefinedTableError:
        if _ready is None:
            log.warning("Таблиц конкурса нет (rb_contest_chats) — конкурс выключен. "
                        "Накати setup.sql.")
        _ready = False
    except Exception as e:
        log.warning("не смог проверить таблицы конкурса: %s", e)
        _ready = False
    return _ready


def now() -> datetime:
    return datetime.now(timezone.utc)


def period_bounds(at: datetime | None = None) -> tuple[datetime, datetime]:
    """(начало, конец) периода, в который попадает at. Конец не включается."""
    at = at or now()
    if CONTEST_TEST_MINUTES > 0:
        step = CONTEST_TEST_MINUTES * 60
        epoch = int(at.timestamp()) // step * step
        start = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return start, start + timedelta(seconds=step)
    local = at.astimezone(TZ)
    monday = local.date() - timedelta(days=local.weekday())
    start = datetime.combine(monday, time.min, tzinfo=TZ)
    return start.astimezone(timezone.utc), (start + timedelta(days=7)).astimezone(timezone.utc)


def prev_period(start: datetime) -> tuple[datetime, datetime]:
    """Период, предшествующий тому, что начинается в start."""
    if CONTEST_TEST_MINUTES > 0:
        step = timedelta(seconds=CONTEST_TEST_MINUTES * 60)
        return start - step, start
    return (start.astimezone(TZ) - timedelta(days=7)).astimezone(timezone.utc), start


def tickets(msgs: int) -> int:
    """До порога билетов нет. На пороге — сразу msgs//50."""
    if msgs < CONTEST_MIN_MSGS:
        return 0
    return msgs // CONTEST_MSGS_PER_TICKET


def pick_winner(rows) -> tuple[int | None, int]:
    """
    Взвешенный случайный выбор: больше билетов — выше шанс, но не гарантия.
    rows: [(user_id, tickets), ...]. -> (user_id, его_билеты)
    """
    pool = [(u, t) for u, t in rows if t > 0]
    if not pool:
        return None, 0
    total = sum(t for _, t in pool)
    r = secrets.randbelow(total) + 1  # 1..total, криптостойко
    acc = 0
    for u, t in pool:
        acc += t
        if r <= acc:
            return u, t
    return pool[-1]


def prize(currency: str) -> int:
    return CONTEST_PRIZE[currency]


# ---------- чаты ----------
async def bind(chat_id: int, title: str, owner_id: int) -> bool:
    """False — таблиц нет, нужен setup.sql."""
    if not await tables_ready():
        return False
    await db.pool().execute(
        """
        INSERT INTO rb_contest_chats (chat_id, title, owner_id) VALUES ($1,$2,$3)
        ON CONFLICT (chat_id) DO UPDATE
        SET title=EXCLUDED.title, owner_id=EXCLUDED.owner_id, active=TRUE,
            deactivated_at=NULL, deactivated_by=NULL
        """, chat_id, title, owner_id)
    return True


async def unbind(chat_id: int, by: int):
    await db.pool().execute(
        "UPDATE rb_contest_chats SET active=FALSE, deactivated_at=now(), deactivated_by=$1 "
        "WHERE chat_id=$2", by, chat_id)


async def is_contest_chat(chat_id: int) -> bool:
    if not await tables_ready():
        return False
    return bool(await db.pool().fetchval(
        "SELECT 1 FROM rb_contest_chats WHERE chat_id=$1 AND active", chat_id))


async def active_chats():
    if not await tables_ready():
        return []
    return await db.pool().fetch("SELECT * FROM rb_contest_chats WHERE active")


# ---------- счётчик ----------
async def count_message(chat_id: int, user_id: int) -> None:
    """
    +1 к счётчику текущего периода.

    Ограничителя частоты тут нет намеренно: за флудом в чате следит отдельный бот,
    дублировать его — значит спорить с ним за то, что считать сообщением.
    """
    start, _ = period_bounds()
    await db.pool().execute(
        """
        INSERT INTO rb_week_msgs (chat_id, period_start, user_id, msgs, last_msg_at)
        VALUES ($1,$2,$3,1, now())
        ON CONFLICT (chat_id, period_start, user_id) DO UPDATE
        SET msgs = rb_week_msgs.msgs + 1, last_msg_at = now()
        """, chat_id, start, user_id)


async def due_draw(chat_id: int, cur_start: datetime):
    """Самый свежий закрытый период без розыгрыша. None — всё разыграно."""
    return await db.pool().fetchval(
        """
        SELECT DISTINCT w.period_start FROM rb_week_msgs w
        WHERE w.chat_id = $1 AND w.period_start < $2
          AND NOT EXISTS (SELECT 1 FROM rb_week_draws d
                          WHERE d.chat_id = w.chat_id AND d.period_start = w.period_start)
        ORDER BY w.period_start DESC LIMIT 1
        """, chat_id, cur_start)


async def create_draw(chat_id: int, start: datetime):
    """
    Создаёт запись розыгрыша. UNIQUE (chat_id, period_start) — вторая попытка
    (два инстанса, ретрай планировщика) молча ничего не сделает.
    """
    _, end = period_bounds(start)
    rows = await standings(chat_id, start)
    players = [r for r in rows if r["tickets"] > 0]
    total = sum(r["tickets"] for r in players)
    return await db.pool().fetchrow(
        """
        INSERT INTO rb_week_draws (chat_id, period_start, period_end, status,
                                   tickets_total, players)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (chat_id, period_start) DO NOTHING
        RETURNING *
        """, chat_id, start, end, "pending" if players else "empty", total, len(players))


async def standings(chat_id: int, start: datetime):
    rows = await db.pool().fetch(
        """
        SELECT w.user_id, w.msgs, u.username, u.first_name
        FROM rb_week_msgs w LEFT JOIN rb_users u ON u.tg_id = w.user_id
        WHERE w.chat_id=$1 AND w.period_start=$2
        ORDER BY w.msgs DESC
        """, chat_id, start)
    return [dict(r, tickets=tickets(r["msgs"])) for r in rows]
