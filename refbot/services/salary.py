"""
Зарплаты: ежемесячные выплаты, назначает админ через карточку пользователя.
Воркер раз в день платит тем, у кого сегодня день выплаты и ещё не платили в этом месяце.
"""
from datetime import datetime, timezone, timedelta

import db

MSK = timezone(timedelta(hours=3))


def _today_msk():
    return datetime.now(MSK).date()


async def set_salary(tg_id: int, amount: int, currency: str, pay_day: int, by: int):
    pay_day = max(1, min(28, pay_day))   # 1-28, чтобы день был в любом месяце
    await db.pool().execute(
        "INSERT INTO rb_salary (tg_id, amount, currency, pay_day, active, set_by) "
        "VALUES ($1,$2,$3,$4,TRUE,$5) "
        "ON CONFLICT (tg_id) DO UPDATE SET amount=$2, currency=$3, pay_day=$4, "
        "active=TRUE, set_by=$5", tg_id, amount, currency, pay_day, by)


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
