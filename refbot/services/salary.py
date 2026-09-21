"""
Зарплаты: ежемесячные выплаты, назначает админ через карточку пользователя.
Воркер раз в день платит тем, у кого сегодня день выплаты и ещё не платили в этом месяце.
"""
from datetime import datetime, timezone, timedelta

import db

MSK = timezone(timedelta(hours=3))


def _today_msk():
    return datetime.now(MSK).date()


async def set_salary(tg_id: int, amount: int, currency: str, pay_day: int, by: int,
                     pay_now: bool = False):
    pay_day = max(1, min(28, pay_day))   # 1-28, чтобы день был в любом месяце
    today = _today_msk()
    last_paid = None
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            if pay_now:
                import time
                idem = f"salary:{tg_id}:{today.year}-{today.month}"
                bal = await db.apply(conn, tg_id, currency, amount, "salary", idem)
                if bal is not None:
                    last_paid = today   # чтобы воркер не заплатил повторно в этом месяце
            await conn.execute(
                "INSERT INTO rb_salary (tg_id, amount, currency, pay_day, active, set_by, last_paid) "
                "VALUES ($1,$2,$3,$4,TRUE,$5,$6) "
                "ON CONFLICT (tg_id) DO UPDATE SET amount=$2, currency=$3, pay_day=$4, "
                "active=TRUE, set_by=$5, last_paid=COALESCE($6, rb_salary.last_paid)",
                tg_id, amount, currency, pay_day, by, last_paid)


async def stop_salary(tg_id: int):
    await db.pool().execute("UPDATE rb_salary SET active=FALSE WHERE tg_id=$1", tg_id)


async def get_salary(tg_id: int) -> dict | None:
    r = await db.pool().fetchrow("SELECT * FROM rb_salary WHERE tg_id=$1", tg_id)
    return dict(r) if r else None


async def pay_due() -> list[dict]:
    """Выплатить зарплаты, у кого сегодня день выплаты и в этом месяце ещё не платили.
    Возвращает список выплаченных для уведомлений."""
    today = _today_msk()
    out = []
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM rb_salary WHERE active AND pay_day=$1 "
                "AND (last_paid IS NULL OR (EXTRACT(YEAR FROM last_paid), EXTRACT(MONTH FROM last_paid)) "
                "     <> ($2, $3)) FOR UPDATE",
                today.day, today.year, today.month)
            for r in rows:
                import time
                idem = f"salary:{r['tg_id']}:{today.year}-{today.month}"
                bal = await db.apply(conn, r["tg_id"], r["currency"], r["amount"],
                                     "salary", idem)
                if bal is not None:
                    await conn.execute(
                        "UPDATE rb_salary SET last_paid=$1 WHERE tg_id=$2", today, r["tg_id"])
                    out.append({"tg_id": r["tg_id"], "amount": r["amount"],
                                "currency": r["currency"]})
    return out


def next_payment_date(sal: dict):
    """Дата следующей выплаты по pay_day и last_paid."""
    from datetime import date
    import calendar
    today = _today_msk()
    day = sal["pay_day"]
    last = sal.get("last_paid")
    # уже платили в этом месяце? тогда следующая — в следующем месяце
    paid_this_month = last and last.year == today.year and last.month == today.month
    y, mth = today.year, today.month
    if today.day > day or paid_this_month:
        # переносим на следующий месяц
        mth += 1
        if mth > 12:
            mth = 1; y += 1
    d = min(day, calendar.monthrange(y, mth)[1])
    return date(y, mth, d)
