"""
Розыгрыш и завершение (Блок 6).

При завершении:
1. Финальная проверка подписки ВСЕХ участников.
2. Отписавшиеся -> страйк (глобальный) + исключение из розыгрыша. 3-й страйк -> бан бота.
3. Оставшиеся -> взвешенный случайный выбор победителей по числу мест.
4. Начисление призов на баланс в боте (в выбранной валюте / обеих / никакой для other).
5. Публикация результатов (gw_publish) + личные уведомления каждому.

Веса по страйкам (из старого бота, проверенная система):
  0 страйков -> вес 1.0
  1 страйк   -> вес 0.85
  2 страйка  -> вес 0.5
  3+         -> бан, в розыгрыше не участвует (вес 0)
"""
import logging
import secrets

import db
from services import gw_invites

log = logging.getLogger("giveaway")

STRIKE_WEIGHTS = {0: 1.0, 1: 0.85, 2: 0.5}
BAN_THRESHOLD = 3


def _weight(strikes: int) -> float:
    return STRIKE_WEIGHTS.get(strikes, 0.0)


def weighted_sample(pool: list, weights: list, k: int) -> list:
    """
    Выбрать k уникальных победителей с учётом весов, без повторов.
    secrets для честной случайности. Классический weighted-without-replacement.
    """
    chosen = []
    items = list(zip(pool, weights))
    k = min(k, len(items))
    for _ in range(k):
        total = sum(w for _, w in items)
        if total <= 0:
            break
        # случайная точка на отрезке суммарного веса
        r = secrets.randbelow(10**9) / 10**9 * total
        acc = 0.0
        for idx, (item, w) in enumerate(items):
            acc += w
            if r <= acc:
                chosen.append(item)
                items.pop(idx)
                break
    return chosen


async def finalize(bot, gid: int) -> dict:
    """
    Завершить розыгрыш. Возвращает сводку: winners [(tg_id, place, prize)], struck [tg_id],
    banned [tg_id]. Начисляет призы, ставит страйки/баны, меняет статус.
    """
    gw = await db.gw_get(gid)
    chats = await db.gw_chats(gid)
    members = await db.gw_members(gid)

    # --- 1-2. финальная проверка подписки + страйки ---
    eligible = []      # (tg_id, currency, strikes) кто остался в игре
    struck = []        # кто отписался и получил страйк
    banned = []        # кто добил до 3 страйков
    for m in members:
        tg = m["tg_id"]
        # забаненные ранее — мимо
        if await db.is_banned(tg):
            continue
        missing = await gw_invites.check_all(bot, chats, tg)
        if missing:
            # отписался -> страйк + исключение
            n = await db.gw_strike(tg)
            await db.gw_mark_struck(gid, tg)
            struck.append(tg)
            if n >= BAN_THRESHOLD:
                await db.set_ban(tg, "3 страйка (розыгрыши)", gw["created_by"])
                banned.append(tg)
            continue
        strikes = await db.gw_get_strikes(tg)
        eligible.append((tg, m["currency"], strikes))

    # --- 3. взвешенный выбор победителей ---
    places = gw["places"]
    prizes = gw["prizes"]
    pool = [e[0] for e in eligible]
    weights = [_weight(e[2]) for e in eligible]
    winners_ids = weighted_sample(pool, weights, places)

    # --- 4. начисление призов ---
    winners = []  # (tg_id, place, prize_dict, currency)
    cur_by_id = {e[0]: e[1] for e in eligible}
    for i, tg in enumerate(winners_ids):
        place = i + 1
        prize = prizes[i] if i < len(prizes) else prizes[-1]
        await db.gw_mark_winner(gid, tg, place)
        await _award(gid, tg, gw["reward_mode"], prize, cur_by_id.get(tg))
        winners.append((tg, place, prize, cur_by_id.get(tg)))

    await db.gw_set_status(gid, "finished")
    return {"winners": winners, "struck": struck, "banned": banned,
            "eligible_count": len(eligible)}


async def _award(gid, tg_id, mode, prize, chosen_cur):
    """Начислить приз победителю на баланс в боте. other — не начисляем (лично)."""
    if mode == "other":
        return
    idem = f"gw:{gid}:{tg_id}"
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            if mode == "mushrooms":
                await db.apply(conn, tg_id, "mushrooms", prize["mushrooms"], "giveaway", idem)
            elif mode == "coins":
                await db.apply(conn, tg_id, "coins", prize["coins"], "giveaway", idem)
            elif mode == "both":
                await db.apply(conn, tg_id, "mushrooms", prize["mushrooms"], "giveaway", idem + ":m")
                await db.apply(conn, tg_id, "coins", prize["coins"], "giveaway", idem + ":c")
            elif mode == "choice":
                cur = chosen_cur or "mushrooms"
                amount = prize["mushrooms"] if cur == "mushrooms" else prize["coins"]
                await db.apply(conn, tg_id, cur, amount, "giveaway", idem)
