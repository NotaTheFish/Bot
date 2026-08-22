"""
Банк — обмен валют. ВСЯ математика идёт через шимкоины как промежуточную меру.

Курсы (в rb_settings, задаёт админ):
  bank.price_mush — сколько ШК стоит BANK_MUSH_UNIT (1 млн) грибов
  bank.price_coin — сколько ШК стоит BANK_COIN_UNIT (10 млн) коинов
  bank.fee        — комиссия банка, % (по умолчанию 5)
  bank.stop       — '1' = обмены грибы<->коины ОСТАНОВЛЕНЫ (тумблер)

Обмены:
  ШК -> грибы / ШК -> коины     — тратит реальные шимкоины, БЕЗ лимита, работает даже при stop
  грибы <-> коины                — через шкалу ШК, лимит BANK_EXCH_DAILY_LIMIT/день, блокируется stop
  грибы/коины -> ШК              — ЗАПРЕЩЕНО

Почему нет арбитража: у каждой валюты одна цена в ШК. Цикл A->B->A по одной шкале
возвращает ровно столько же (минус комиссия). Округление — всегда ВНИЗ (в пользу казны).

Безопасность обмена: списание + начисление + счётчик — ОДНА транзакция. Проверка
stop-флага и лимита идёт ВНУТРИ транзакции. Стоп в момент обмена не создаёт дюпа
и не теряет валюту: транзакция либо прошла целиком, либо не начиналась.
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import db
from config import (BANK_MUSH_UNIT, BANK_COIN_UNIT, BANK_DEFAULT_PRICE_MUSH,
                    BANK_DEFAULT_PRICE_COIN, BANK_DEFAULT_FEE, BANK_EXCH_DAILY_LIMIT,
                    ROULETTE_TZ)
from services import settings

log = logging.getLogger("bank")
_TZ = ZoneInfo(ROULETTE_TZ)


def _today() -> str:
    return datetime.now(_TZ).strftime("%Y-%m-%d")


# ---------------- курсы / настройки ----------------
async def price_mush() -> float:
    return float(await settings.get("bank.price_mush", str(BANK_DEFAULT_PRICE_MUSH)))


async def price_coin() -> float:
    return float(await settings.get("bank.price_coin", str(BANK_DEFAULT_PRICE_COIN)))


async def fee_pct() -> float:
    return float(await settings.get("bank.fee", str(BANK_DEFAULT_FEE)))


async def is_stopped() -> bool:
    return await settings.get("bank.stop", "0") == "1"


async def set_price_mush(v: float, actor: int) -> None:
    await settings.set("bank.price_mush", str(v), actor)


async def set_price_coin(v: float, actor: int) -> None:
    await settings.set("bank.price_coin", str(v), actor)


async def set_fee(v: float, actor: int) -> None:
    await settings.set("bank.fee", str(v), actor)


async def toggle_stop(actor: int) -> bool:
    """Переключить стоп. Возвращает новое состояние (True = остановлено)."""
    new = "0" if await is_stopped() else "1"
    await settings.set("bank.stop", new, actor)
    return new == "1"


# ---------------- вкл/выкл конкретного товара ----------------
# Если товар выключен, он пропадает из ВСЕХ операций банка: нельзя купить его
# за шимкоины и нельзя получить/отдать его в обмене грибы<->коины.
async def item_enabled(item: str) -> bool:
    """item из {mushrooms, coins}. По умолчанию включён."""
    return await settings.get(f"bank.item_off.{item}", "0") != "1"


async def toggle_item(item: str, actor: int) -> bool:
    """Переключить доступность товара. Возвращает новое состояние (True = включён)."""
    now_on = await item_enabled(item)
    await settings.set(f"bank.item_off.{item}", "1" if now_on else "0", actor)
    return not now_on


# ---------------- цена валюты в ЦЕНТАХ шимкоина (за 1 единицу) ----------------
# Шимкоин хранится в центах (1 ШК = 100 центов). Курс price_mush = ШК за 1 млн грибов,
# значит в центах за 1 гриб: price_mush * 100 / BANK_MUSH_UNIT.
async def _cents_per_mushroom() -> float:
    return (await price_mush()) * 100.0 / BANK_MUSH_UNIT


async def _cents_per_coin() -> float:
    return (await price_coin()) * 100.0 / BANK_COIN_UNIT


# ---------------- расчёт обмена (без записи) ----------------
async def quote(src: str, dst: str, amount: int) -> tuple[int, int, str]:
    """
    Рассчитать обмен amount единиц src -> dst.
    ВАЖНО: для src/dst = shimcoins величина в ЦЕНТАХ (шимкоин хранится в центах).
    Для грибов/коинов — в обычных единицах.
    Возвращает (получит_dst, стоимость_в_центах_после_комиссии, ошибка|"").
    Округление ВНИЗ (в пользу казны). Комиссия только грибы<->коины.
    """
    if amount <= 0:
        return 0, 0, "Сумма должна быть больше нуля."
    if dst == "shimcoins":
        return 0, 0, "Менять на шимкоины нельзя."
    if src == dst:
        return 0, 0, "Одинаковые валюты."
    # выключенный товар недоступен ни как получаемый (dst), ни как отдаваемый (src)
    for side in (src, dst):
        if side in ("mushrooms", "coins") and not await item_enabled(side):
            name = "Грибы" if side == "mushrooms" else "Коины"
            return 0, 0, f"{name} сейчас недоступны в банке."

    fee = await fee_pct() / 100.0
    is_gm = {src, dst} == {"mushrooms", "coins"}
    if not is_gm:
        fee = 0.0

    # стоимость src в ЦЕНТАХ
    if src == "mushrooms":
        cents = amount * await _cents_per_mushroom()
    elif src == "coins":
        cents = amount * await _cents_per_coin()
    elif src == "shimcoins":
        cents = float(amount)   # amount уже в центах
    else:
        return 0, 0, "Неизвестная валюта."

    # комиссия срезает часть стоимости
    cents_after = cents * (1.0 - fee)

    # конвертируем центы -> dst
    if dst == "mushrooms":
        got = int(cents_after / await _cents_per_mushroom())
    elif dst == "coins":
        got = int(cents_after / await _cents_per_coin())
    else:
        return 0, 0, "Неизвестная валюта."

    if got <= 0:
        return 0, 0, "Слишком мало — на выходе ноль. Увеличь сумму."
    # стоимость в центах округляем ВНИЗ до целого цента (списываем целые центы)
    return got, int(cents_after), ""


async def quote_reverse(dst: str, want: int) -> tuple[int, int, str]:
    """
    Обратный расчёт для ПОКУПКИ за шимкоины: игрок хочет получить `want` единиц dst
    (грибы/коины) — сколько ЦЕНТОВ шимкоина отдать и сколько реально получит.
    Возвращает (нужно_центов, реально_получит_dst, ошибка|"").
    Без комиссии (покупка за ШК). Центы округляем ВВЕРХ — игрок получит не меньше
    запрошенного; фактическая выдача пересчитывается от округлённых центов.
    """
    import math
    if want <= 0:
        return 0, 0, "Сумма должна быть больше нуля."
    if dst in ("mushrooms", "coins") and not await item_enabled(dst):
        name = "Грибы" if dst == "mushrooms" else "Коины"
        return 0, 0, f"{name} сейчас недоступны в банке."
    if dst == "mushrooms":
        per = await _cents_per_mushroom()
    elif dst == "coins":
        per = await _cents_per_coin()
    else:
        return 0, 0, "Купить можно грибы или коины."
    if per <= 0:
        return 0, 0, "Курс не задан."
    need_cents = math.ceil(want * per)
    if need_cents <= 0:
        need_cents = 1
    got = int(need_cents / per)
    return need_cents, got, ""


# ---------------- дневной лимит грибы<->коины ----------------
async def exch_left(tg_id: int) -> int:
    """Сколько обменов грибы<->коины осталось сегодня."""
    cnt = await db.pool().fetchval(
        "SELECT cnt FROM rb_bank_exch WHERE tg_id=$1 AND exch_day=$2", tg_id, _today()) or 0
    return max(0, BANK_EXCH_DAILY_LIMIT - cnt)


class BankError(Exception):
    pass


# ---------------- выполнить обмен (атомарно) ----------------
async def exchange(tg_id: int, src: str, dst: str, amount: int) -> tuple[int, str]:
    """
    Выполнить обмен. Возвращает (получено_dst, ошибка|"").
    ВСЁ в одной транзакции: проверка стопа/лимита + списание + начисление + счётчик.
    """
    is_gm = {src, dst} == {"mushrooms", "coins"}   # обмен грибы<->коины?
    got, shk_after, err = await quote(src, dst, amount)
    if err:
        return 0, err

    day = _today()
    idem = f"bank:{tg_id}:{src}:{dst}:{amount}:{datetime.now(_TZ).timestamp()}"

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            # стоп грибы<->коины проверяем ВНУТРИ транзакции (свежее значение из БД)
            if is_gm:
                stop = await conn.fetchval("SELECT value FROM rb_settings WHERE key='bank.stop'")
                if stop == "1":
                    raise BankError("🛑 Обмен грибы↔коины временно остановлен. Загляни позже.")
                # дневной лимит — берём строку под блокировкой
                row = await conn.fetchrow(
                    "SELECT cnt FROM rb_bank_exch WHERE tg_id=$1 AND exch_day=$2 FOR UPDATE",
                    tg_id, day)
                used = row["cnt"] if row else 0
                if used >= BANK_EXCH_DAILY_LIMIT:
                    raise BankError(f"Лимит обменов грибы↔коины на сегодня исчерпан "
                                    f"({BANK_EXCH_DAILY_LIMIT}/день).")

            # списываем src (не в минус — обычная защита овердрафта)
            try:
                await db.apply(conn, tg_id, src, -amount, "bank_exch_out", idem + ":out")
            except Exception:
                raise BankError("Недостаточно средств для обмена.")
            # начисляем dst
            await db.apply(conn, tg_id, dst, got, "bank_exch_in", idem + ":in")

            # увеличиваем счётчик только для грибы<->коины
            if is_gm:
                await conn.execute(
                    "INSERT INTO rb_bank_exch (tg_id, exch_day, cnt) VALUES ($1,$2,1) "
                    "ON CONFLICT (tg_id, exch_day) DO UPDATE SET cnt = rb_bank_exch.cnt + 1",
                    tg_id, day)

    await db.audit(tg_id, "bank_exchange",
                   {"src": src, "dst": dst, "amount": amount, "got": got})
    return got, ""
