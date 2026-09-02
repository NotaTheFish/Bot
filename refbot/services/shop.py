"""
Магазин: товары, покупка. Товары: удача, скидка, титул, эмодзи.
Покупка списывает валюту, выдаёт товар (в инвентарь для удачи/скидки, сразу для титула/эмодзи).
"""
import json
import db

ITEM_TYPES = ("luck", "discount", "title", "emoji")
LUCK_SCOPES = ("all", "roulette", "cases", "shine", "giveaway", "contest")
DISCOUNT_TARGETS = ("shop", "bank", "all")


async def add_item(name: str, desc: str, item_type: str, price: int, currency: str,
                   payload: dict, stock: int | None, by: int) -> int:
    return await db.pool().fetchval(
        "INSERT INTO rb_shop (name, description, item_type, price, currency, payload, stock, created_by) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
        name, desc, item_type, price, currency, json.dumps(payload, ensure_ascii=False), stock, by)


async def remove_item(item_id: int):
    await db.pool().execute("UPDATE rb_shop SET active=false WHERE id=$1", item_id)


async def list_items(active_only: bool = True) -> list[dict]:
    q = "SELECT * FROM rb_shop"
    if active_only:
        q += " WHERE active"
    q += " ORDER BY id"
    rows = await db.pool().fetch(q)
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = d["payload"] if isinstance(d["payload"], dict) else json.loads(d["payload"])
        out.append(d)
    return out


async def get_item(item_id: int) -> dict | None:
    r = await db.pool().fetchrow("SELECT * FROM rb_shop WHERE id=$1", item_id)
    if not r:
        return None
    d = dict(r)
    d["payload"] = d["payload"] if isinstance(d["payload"], dict) else json.loads(d["payload"])
    return d


async def _discount_for(uid: int, target: str) -> int:
    """Активная скидка (%) для цели покупки. Берёт максимальную подходящую."""
    rows = await db.pool().fetch(
        "SELECT payload FROM rb_active_bonuses WHERE tg_id=$1 AND bonus_type='discount' "
        "AND scope IN ($2,'all')", uid, target)
    best = 0
    for r in rows:
        p = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
        best = max(best, int(p.get("percent", 0)))
    return min(best, 90)


async def buy(uid: int, item_id: int) -> tuple[bool, str, dict | None]:
    """Купить товар. Возвращает (успех, сообщение/ошибка, item)."""
    item = await get_item(item_id)
    if not item or not item["active"]:
        return False, "Товар недоступен.", None
    if item["stock"] is not None and item["stock"] <= 0:
        return False, "Товар закончился.", None
    # скидка на магазин
    disc = await _discount_for(uid, "shop")
    price = item["price"]
    if disc:
        price = int(price * (100 - disc) / 100)
    b = await db.balances(uid)
    if b.get(item["currency"], 0) < price:
        return False, "Недостаточно средств.", None

    import time
    idem = f"shop:{item_id}:{uid}:{int(time.time()*1000)}"
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            spent = await db.apply(conn, uid, item["currency"], -price, "shop_buy", idem)
            if spent is None:
                return False, "Не удалось списать средства.", None
            if item["stock"] is not None:
                await conn.execute("UPDATE rb_shop SET stock=stock-1 WHERE id=$1", item_id)
    # выдать товар
    await _deliver(uid, item, disc, price)
    # счётчики
    try:
        from services import counters as _cnt
        await _cnt.bump(uid, _cnt.C_SHOP_BOUGHT)
        await _cnt.bump(uid, _cnt.C_SHOP_SPENT, price)
    except Exception:
        pass
    return True, "", item


async def _deliver(uid: int, item: dict, disc: int, paid: int):
    """Выдать купленный товар."""
    t = item["item_type"]
    p = item["payload"]
    if t == "luck":
        # в инвентарь (активирует игрок сам)
        await db.pool().execute(
            "INSERT INTO rb_inventory (tg_id, item_type, payload) VALUES ($1,'luck',$2)",
            uid, json.dumps({"scope": p.get("scope", "all"), "mult": p.get("mult", 2),
                             "minutes": p.get("minutes", 15)}))
    elif t == "discount":
        await db.pool().execute(
            "INSERT INTO rb_inventory (tg_id, item_type, payload) VALUES ($1,'discount',$2)",
            uid, json.dumps({"target": p.get("target", "shop"), "percent": p.get("percent", 10)}))
    elif t == "title":
        from services import titles
        tid = p.get("title_id")
        if not tid and p.get("title_name"):
            tid = await titles.create_title(p["title_name"], False, 0)
        if tid:
            await titles.grant_title(uid, tid, None)
    elif t == "emoji":
        emo = p.get("emoji")
        if emo:
            await db.pool().execute(
                "INSERT INTO rb_user_emojis (tg_id, emoji, source) VALUES ($1,$2,'shop') "
                "ON CONFLICT (tg_id, emoji) DO NOTHING", uid, emo)
