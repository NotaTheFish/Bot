"""
Выводы.

Опасное место всей системы. Защита:
  1. Один pending на юзера — UNIQUE INDEX в БД, а не проверка в коде.
  2. version растёт при каждом изменении суммы. В callback_data админа зашит
     version. Пришёл старый version -> кнопка мертва. Так админ не может
     подтвердить сумму, которую юзер уже поменял.
  3. SELECT ... FOR UPDATE на строке вывода — два админа не подтвердят дважды.
  4. Списание идёт через db.apply с idem-ключом wd:<id>. CHECK(amount>=0) на
     балансе — последняя линия обороны.
  5. Баланс перепроверяется в момент подтверждения, а не в момент заявки.
"""
import asyncpg

import db
from config import MIN_WITHDRAW


async def create(tg_id: int, chat_id: int, currency: str, amount: int) -> tuple[int | None, str]:
    if amount < MIN_WITHDRAW[currency]:
        return None, f"Минимум для вывода: {MIN_WITHDRAW[currency]:,}".replace(",", " ")
    if await db.is_banned(tg_id):
        return None, "Аккаунт заблокирован."

    # Замораживаем средства СРАЗУ: списываем с баланса при создании заявки, чтобы их
    # нельзя было потратить в казино, пока админ думает. Возврат — при отмене/отклонении.
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            bal = await conn.fetchval(
                "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2",
                tg_id, currency) or 0
            if amount > bal:
                return None, f"Недостаточно средств. Баланс: {bal:,}".replace(",", " ")
            try:
                wid = await conn.fetchval(
                    "INSERT INTO rb_withdrawals (tg_id, chat_id, currency, amount) "
                    "VALUES ($1,$2,$3,$4) RETURNING id", tg_id, chat_id, currency, amount)
            except asyncpg.UniqueViolationError:
                return None, "У тебя уже есть активная заявка. Измени её или отмени."
            # замораживаем (списываем) под этот wid
            await db.apply(conn, tg_id, currency, -amount,
                           "wd_hold", f"wdhold:{wid}", wid)
    await db.audit(tg_id, "wd_create", {"id": wid, "amount": amount, "currency": currency})
    return wid, ""


async def change_amount(tg_id: int, amount: int) -> tuple[dict | None, str]:
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            wd = await conn.fetchrow(
                "SELECT * FROM rb_withdrawals WHERE tg_id=$1 AND status='pending' FOR UPDATE", tg_id)
            if not wd:
                return None, "Активной заявки нет."
            if amount < MIN_WITHDRAW[wd["currency"]]:
                return None, f"Минимум: {MIN_WITHDRAW[wd['currency']]:,}".replace(",", " ")
            # старая сумма уже заморожена. Доступно для новой = текущий баланс + старая заморозка.
            bal = await conn.fetchval(
                "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2",
                tg_id, wd["currency"]) or 0
            available = bal + wd["amount"]
            if amount > available:
                return None, f"Недостаточно средств. Доступно: {available:,}".replace(",", " ")
            # вернём старую заморозку и заморозим новую сумму
            await db.apply(conn, tg_id, wd["currency"], wd["amount"],
                           "wd_hold_release", f"wdrel:{wd['id']}:v{wd['version']}", wd["id"])
            await db.apply(conn, tg_id, wd["currency"], -amount,
                           "wd_hold", f"wdhold:{wd['id']}:v{wd['version']+1}", wd["id"])
            row = await conn.fetchrow(
                "UPDATE rb_withdrawals SET amount=$1, version=version+1 WHERE id=$2 RETURNING *",
                amount, wd["id"])
    await db.audit(tg_id, "wd_change", {"id": row["id"], "amount": amount})
    return dict(row), ""


async def cancel(tg_id: int) -> dict | None:
    """Игрок отменяет заявку — замороженные средства возвращаются на баланс."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            wd = await conn.fetchrow(
                "SELECT * FROM rb_withdrawals WHERE tg_id=$1 AND status='pending' FOR UPDATE", tg_id)
            if not wd:
                return None
            # вернуть заморозку
            await db.apply(conn, tg_id, wd["currency"], wd["amount"],
                           "wd_hold_return", f"wdret:{wd['id']}", wd["id"])
            row = await conn.fetchrow(
                "UPDATE rb_withdrawals SET status='cancelled', decided_at=now() "
                "WHERE id=$1 RETURNING *", wd["id"])
    await db.audit(tg_id, "wd_cancel", {"id": row["id"]})
    return dict(row)


async def confirm(admin_id: int, wid: int, version: int) -> tuple[dict | None, str]:
    """
    Админ отдал валюту в игре и подтверждает. Средства УЖЕ списаны (заморожены) при
    создании заявки — здесь просто помечаем выполненной, повторно НЕ списываем.
    """
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            wd = await conn.fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1 FOR UPDATE", wid)
            if not wd:
                return None, "Заявка не найдена."
            if wd["status"] != "pending":
                return None, f"Заявка уже обработана ({wd['status']})."
            if wd["version"] != version:
                return None, "Сумма изменилась. Обнови сообщение — придёт новое."
            from config import PAYOUT_ADMINS, SUPER_ADMINS
            is_chat_admin = await conn.fetchval(
                "SELECT 1 FROM rb_admins WHERE chat_id=$1 AND tg_id=$2", wd["chat_id"], admin_id)
            if not (is_chat_admin or admin_id in PAYOUT_ADMINS or admin_id in SUPER_ADMINS):
                return None, "Нет прав на подтверждение вывода."
            # деньги уже заморожены при создании — просто фиксируем выдачу
            row = await conn.fetchrow(
                "UPDATE rb_withdrawals SET status='confirmed', decided_at=now(), decided_by=$1 "
                "WHERE id=$2 RETURNING *", admin_id, wid)
    await db.audit(admin_id, "wd_confirm", {"id": wid, "user": wd["tg_id"], "amount": wd["amount"]})
    return dict(row), ""


async def reject(admin_id: int, wid: int, reason: str = "") -> tuple[dict | None, str]:
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            wd = await conn.fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1 FOR UPDATE", wid)
            if not wd or wd["status"] != "pending":
                return None, "Заявка недоступна."
            from config import PAYOUT_ADMINS, SUPER_ADMINS
            is_chat_admin = await conn.fetchval(
                "SELECT 1 FROM rb_admins WHERE chat_id=$1 AND tg_id=$2", wd["chat_id"], admin_id)
            if not (is_chat_admin or admin_id in PAYOUT_ADMINS or admin_id in SUPER_ADMINS):
                return None, "Нет прав."
            # вернуть замороженные средства игроку
            await db.apply(conn, wd["tg_id"], wd["currency"], wd["amount"],
                           "wd_hold_return", f"wdret:{wid}", wid)
            row = await conn.fetchrow(
                "UPDATE rb_withdrawals SET status='rejected', decided_at=now(), decided_by=$1, "
                "comment=$2 WHERE id=$3 RETURNING *", admin_id, reason, wid)
    await db.audit(admin_id, "wd_reject", {"id": wid, "reason": reason})
    return dict(row), ""


async def active(tg_id: int):
    return await db.pool().fetchrow(
        "SELECT * FROM rb_withdrawals WHERE tg_id=$1 AND status='pending'", tg_id)
