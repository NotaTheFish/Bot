"""
Копилки: личные хранилища валюты. До 5 на игрока.
Операции: создать, пополнить (баланс->копилка), снять (копилка->баланс),
разбить (всё обратно на баланс + удалить копилку).
"""
import db

MAX_PIGGY = 5


async def list_piggy(uid: int) -> list[dict]:
    rows = await db.pool().fetch(
        "SELECT * FROM rb_piggy WHERE tg_id=$1 ORDER BY created_at", uid)
    return [dict(r) for r in rows]


async def count_piggy(uid: int) -> int:
    return await db.pool().fetchval(
        "SELECT count(*) FROM rb_piggy WHERE tg_id=$1", uid) or 0


async def get_piggy(pid: int, uid: int) -> dict | None:
    r = await db.pool().fetchrow(
        "SELECT * FROM rb_piggy WHERE id=$1 AND tg_id=$2", pid, uid)
    return dict(r) if r else None


async def create_piggy(uid: int, name: str, currency: str) -> tuple[int | None, str]:
    if await count_piggy(uid) >= MAX_PIGGY:
        return None, f"У тебя уже {MAX_PIGGY} копилок — разбей одну, чтобы создать новую."
    pid = await db.pool().fetchval(
        "INSERT INTO rb_piggy (tg_id, name, currency) VALUES ($1,$2,$3) RETURNING id",
        uid, name, currency)
    return pid, ""


async def deposit(uid: int, pid: int, amount: int) -> tuple[bool, str]:
    """Положить в копилку: списать с баланса, добавить в копилку."""
    if amount <= 0:
        return False, "Сумма должна быть больше нуля."
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            p = await conn.fetchrow(
                "SELECT * FROM rb_piggy WHERE id=$1 AND tg_id=$2 FOR UPDATE", pid, uid)
            if not p:
                return False, "Копилка не найдена."
            import time
            idem = f"piggy_in:{pid}:{int(time.time()*1000)}"
            spent = await db.apply(conn, uid, p["currency"], -amount, "piggy_deposit", idem)
            if spent is None:
                return False, "Недостаточно средств на балансе."
            await conn.execute("UPDATE rb_piggy SET amount=amount+$1 WHERE id=$2", amount, pid)
    return True, ""


async def withdraw(uid: int, pid: int, amount: int) -> tuple[bool, str]:
    """Снять из копилки: вернуть на баланс."""
    if amount <= 0:
        return False, "Сумма должна быть больше нуля."
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            p = await conn.fetchrow(
                "SELECT * FROM rb_piggy WHERE id=$1 AND tg_id=$2 FOR UPDATE", pid, uid)
            if not p:
                return False, "Копилка не найдена."
            if p["amount"] < amount:
                return False, f"В копилке только {p['amount']}."
            import time
            idem = f"piggy_out:{pid}:{int(time.time()*1000)}"
            await conn.execute("UPDATE rb_piggy SET amount=amount-$1 WHERE id=$2", amount, pid)
            await db.apply(conn, uid, p["currency"], amount, "piggy_withdraw", idem)
    return True, ""


async def smash(uid: int, pid: int) -> tuple[dict | None, str]:
    """Разбить копилку: всё содержимое на баланс, копилка удаляется."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            p = await conn.fetchrow(
                "SELECT * FROM rb_piggy WHERE id=$1 AND tg_id=$2 FOR UPDATE", pid, uid)
            if not p:
                return None, "Копилка не найдена."
            if p["amount"] > 0:
                import time
                idem = f"piggy_smash:{pid}:{int(time.time()*1000)}"
                await db.apply(conn, uid, p["currency"], p["amount"], "piggy_smash", idem)
            await conn.execute("DELETE FROM rb_piggy WHERE id=$1", pid)
    return dict(p), ""
