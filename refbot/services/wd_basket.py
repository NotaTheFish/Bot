"""
Корзина вывода: одна заявка (rb_withdrawals) -> много позиций (rb_wd_items),
каждая своей валюты. Вариант А: админ подтверждает/отклоняет КАЖДУЮ позицию отдельно.

Денежная модель (как в старых выводах, но по позициям):
  - При ОТПРАВКЕ корзины все позиции замораживаются АТОМАРНО (списываются с баланса),
    чтобы их нельзя было потратить, пока админ думает. Одна транзакция на всю корзину.
  - Отмена игроком -> возврат всех ещё pending-позиций.
  - Админ по позиции: confirm -> заморозка сгорает (выдано в игре);
    reject -> заморозка возвращается игроку.
  - Заявка закрывается (status), когда ВСЕ позиции обработаны.
  - Пока есть хоть одна pending-позиция — новую корзину создать нельзя.

idem-ключи заморозки/возврата привязаны к item.id, поэтому повтор безопасен.
"""
import asyncpg

import db
from config import MIN_WITHDRAW, WITHDRAW_STEP


def validate_item(currency: str, amount: int) -> str | None:
    """Проверка одной позиции: минимум и кратность. None если ок, иначе текст ошибки."""
    if currency not in MIN_WITHDRAW:
        return "Эту валюту вывести нельзя."
    if amount < MIN_WITHDRAW[currency]:
        return f"Минимум для этой валюты: {MIN_WITHDRAW[currency]:,}".replace(",", " ")
    step = WITHDRAW_STEP[currency]
    if amount % step != 0:
        return (f"Кратно {step:,}. Ближайшие: {(amount//step)*step:,} или "
                f"{(amount//step+1)*step:,}.").replace(",", " ")
    return None


async def has_active(tg_id: int) -> bool:
    """Есть ли у игрока заявка с необработанными позициями."""
    row = await db.pool().fetchval(
        "SELECT 1 FROM rb_withdrawals w "
        "WHERE w.tg_id=$1 AND w.status='pending' "
        "AND EXISTS (SELECT 1 FROM rb_wd_items i WHERE i.wid=w.id AND i.status='pending') "
        "LIMIT 1", tg_id)
    return bool(row)


async def create_basket(tg_id: int, chat_id: int, items: list[tuple[str, int]]) -> tuple[int | None, str]:
    """
    items: [(currency, amount), ...]. Создать заявку, заморозить ВСЕ позиции атомарно.
    Возвращает (wid, "") или (None, ошибка).
    """
    if not items:
        return None, "Корзина пуста."
    # схлопнём дубли валют (на всякий случай) и проверим каждую позицию
    merged: dict[str, int] = {}
    for cur, amt in items:
        merged[cur] = merged.get(cur, 0) + amt
    for cur, amt in merged.items():
        err = validate_item(cur, amt)
        if err:
            return None, f"{cur}: {err}"

    if await db.is_banned(tg_id):
        return None, "Аккаунт заблокирован."
    if await has_active(tg_id):
        return None, "У тебя уже есть активная заявка. Дождись обработки или отмени."

    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                # проверяем баланс по каждой валюте и замораживаем
                for cur, amt in merged.items():
                    bal = await conn.fetchval(
                        "SELECT COALESCE(amount,0) FROM rb_balances WHERE tg_id=$1 AND currency=$2 FOR UPDATE",
                        tg_id, cur) or 0
                    if amt > bal:
                        raise _Insufficient(cur, bal)
                wid = await conn.fetchval(
                    "INSERT INTO rb_withdrawals (tg_id, chat_id) VALUES ($1,$2) RETURNING id",
                    tg_id, chat_id)
                for cur, amt in merged.items():
                    item_id = await conn.fetchval(
                        "INSERT INTO rb_wd_items (wid, currency, amount) VALUES ($1,$2,$3) RETURNING id",
                        wid, cur, amt)
                    # заморозка позиции
                    await db.apply(conn, tg_id, cur, -amt, "wd_hold", f"wdhold:item:{item_id}", wid)
    except _Insufficient as e:
        return None, f"Недостаточно {e.currency}. Баланс: {e.balance:,}".replace(",", " ")
    await db.audit(tg_id, "wd_basket_create", {"id": wid, "items": [list(x) for x in merged.items()]})
    return wid, ""


class _Insufficient(Exception):
    def __init__(self, currency, balance):
        self.currency = currency
        self.balance = balance


async def cancel_basket(tg_id: int) -> dict | None:
    """Игрок отменяет всю заявку — возврат всех ещё pending-позиций."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            w = await conn.fetchrow(
                "SELECT * FROM rb_withdrawals WHERE tg_id=$1 AND status='pending' FOR UPDATE", tg_id)
            if not w:
                return None
            items = await conn.fetch(
                "SELECT * FROM rb_wd_items WHERE wid=$1 AND status='pending' FOR UPDATE", w["id"])
            for it in items:
                await db.apply(conn, tg_id, it["currency"], it["amount"],
                               "wd_hold_return", f"wdret:item:{it['id']}", w["id"])
                await conn.execute(
                    "UPDATE rb_wd_items SET status='cancelled', decided_at=now() WHERE id=$1", it["id"])
            await conn.execute(
                "UPDATE rb_withdrawals SET status='cancelled', decided_at=now() WHERE id=$1", w["id"])
    await db.audit(tg_id, "wd_basket_cancel", {"id": w["id"]})
    return dict(w)


async def _maybe_close(conn, wid: int):
    """Если все позиции заявки обработаны — закрыть заявку (status по итогу)."""
    left = await conn.fetchval(
        "SELECT count(*) FROM rb_wd_items WHERE wid=$1 AND status='pending'", wid)
    if left and left > 0:
        return
    # все обработаны: если хоть одна confirmed -> confirmed, иначе rejected
    any_conf = await conn.fetchval(
        "SELECT count(*) FROM rb_wd_items WHERE wid=$1 AND status='confirmed'", wid)
    final = "confirmed" if any_conf and any_conf > 0 else "rejected"
    await conn.execute(
        "UPDATE rb_withdrawals SET status=$1, decided_at=now() WHERE id=$2 AND status='pending'",
        final, wid)


async def _check_admin(conn, admin_id: int, chat_id: int) -> bool:
    from config import PAYOUT_ADMINS, SUPER_ADMINS
    if admin_id in PAYOUT_ADMINS or admin_id in SUPER_ADMINS:
        return True
    return bool(await conn.fetchval(
        "SELECT 1 FROM rb_admins WHERE chat_id=$1 AND tg_id=$2", chat_id, admin_id))


async def confirm_item(admin_id: int, item_id: int) -> tuple[dict | None, str]:
    """Подтвердить позицию: заморозка сгорает (уже списано при создании). Выдано в игре."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            it = await conn.fetchrow("SELECT * FROM rb_wd_items WHERE id=$1 FOR UPDATE", item_id)
            if not it:
                return None, "Позиция не найдена."
            if it["status"] != "pending":
                return None, f"Позиция уже обработана ({it['status']})."
            w = await conn.fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1", it["wid"])
            if not await _check_admin(conn, admin_id, w["chat_id"]):
                return None, "Нет прав."
            await conn.execute(
                "UPDATE rb_wd_items SET status='confirmed', decided_at=now(), decided_by=$1 WHERE id=$2",
                admin_id, item_id)
            await _maybe_close(conn, it["wid"])
    await db.audit(admin_id, "wd_item_confirm",
                   {"item": item_id, "user": w["tg_id"], "cur": it["currency"], "amount": it["amount"]})
    return dict(it), ""


async def reject_item(admin_id: int, item_id: int, reason: str = "") -> tuple[dict | None, str]:
    """Отклонить позицию: заморозка возвращается игроку."""
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            it = await conn.fetchrow("SELECT * FROM rb_wd_items WHERE id=$1 FOR UPDATE", item_id)
            if not it:
                return None, "Позиция не найдена."
            if it["status"] != "pending":
                return None, f"Позиция уже обработана ({it['status']})."
            w = await conn.fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1", it["wid"])
            if not await _check_admin(conn, admin_id, w["chat_id"]):
                return None, "Нет прав."
            # возврат заморозки игроку
            await db.apply(conn, w["tg_id"], it["currency"], it["amount"],
                           "wd_hold_return", f"wdret:item:{item_id}", it["wid"])
            await conn.execute(
                "UPDATE rb_wd_items SET status='rejected', decided_at=now(), decided_by=$1, comment=$2 WHERE id=$3",
                admin_id, reason, item_id)
            await _maybe_close(conn, it["wid"])
    await db.audit(admin_id, "wd_item_reject", {"item": item_id, "user": w["tg_id"], "reason": reason})
    return dict(it), ""


async def active_basket(tg_id: int):
    """Активная заявка игрока + её позиции (для показа/отмены). None если нет."""
    w = await db.pool().fetchrow(
        "SELECT * FROM rb_withdrawals WHERE tg_id=$1 AND status='pending' "
        "AND EXISTS (SELECT 1 FROM rb_wd_items i WHERE i.wid=rb_withdrawals.id AND i.status='pending') "
        "ORDER BY id DESC LIMIT 1", tg_id)
    if not w:
        return None, []
    items = await db.pool().fetch(
        "SELECT * FROM rb_wd_items WHERE wid=$1 ORDER BY id", w["id"])
    return dict(w), [dict(i) for i in items]


async def get_basket(wid: int):
    w = await db.pool().fetchrow("SELECT * FROM rb_withdrawals WHERE id=$1", wid)
    if not w:
        return None, []
    items = await db.pool().fetch("SELECT * FROM rb_wd_items WHERE wid=$1 ORDER BY id", wid)
    return dict(w), [dict(i) for i in items]
