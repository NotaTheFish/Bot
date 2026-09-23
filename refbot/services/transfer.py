"""
Передача валюты и предметов между игроками. Без комиссии.
Валюта/токены — прямой перевод. Предметы (бонусы из инвентаря) — перенос записи.
"""
import json
import db

CURRENCIES = ("mushrooms", "coins", "shimcoins", "revive", "max", "partials")


async def transfer_currency(sender: int, receiver: int, currency: str,
                            amount: int) -> tuple[bool, str]:
    """Передать валюту/токены. Без комиссии. Нельзя во время налёта."""
    if sender == receiver:
        return False, "Нельзя подарить самому себе."
    if amount <= 0:
        return False, "Количество должно быть больше нуля."
    if currency not in CURRENCIES:
        return False, "Неизвестная валюта."
    # блок во время налёта (валюта заморожена)
    try:
        from services import steal as _steal
        if await _steal.active_involving(sender):
            return False, "Идёт налёт — передача недоступна."
    except Exception:
        pass
    import time
    idem = f"gift:{sender}:{receiver}:{int(time.time()*1000)}"
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            spent = await db.apply(conn, sender, currency, -amount, "gift_out", idem + ":o")
            if spent is None:
                return False, "Недостаточно средств."
            await db.apply(conn, receiver, currency, amount, "gift_in", idem + ":i")
    return True, ""


async def transfer_item(sender: int, receiver: int, inv_id: int) -> tuple[bool, str, dict | None]:
    """Передать предмет из инвентаря (бонус) другому игроку."""
    if sender == receiver:
        return False, "Нельзя подарить самому себе.", None
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            it = await conn.fetchrow(
                "SELECT * FROM rb_inventory WHERE id=$1 AND tg_id=$2 AND NOT used FOR UPDATE",
                inv_id, sender)
            if not it:
                return False, "Предмет недоступен.", None
            # переносим запись на получателя
            await conn.execute(
                "UPDATE rb_inventory SET tg_id=$1 WHERE id=$2", receiver, inv_id)
            d = dict(it)
    d["payload"] = d["payload"] if isinstance(d["payload"], dict) else json.loads(d["payload"])
    return True, "", d
