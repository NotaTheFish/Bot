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


# ---------------- цена валюты в ШК (за 1 единицу) ----------------
async def _shk_per_mushroom() -> float:
    return (await price_mush()) / BANK_MUSH_UNIT


async def _shk_per_coin() -> float:
    return (await price_coin()) / BANK_COIN_UNIT


# ---------------- расчёт обмена (без записи) ----------------
async def quote(src: str, dst: str, amount: int) -> tuple[int, float, str]:
    """
    Рассчитать обмен amount единиц src -> dst.
    Возвращает (получит_dst, стоимость_в_ШК_после_комиссии, ошибка|"").
    src/dst из {mushrooms, coins, shimcoins}. Всегда округление ВНИЗ.
    Комиссия удерживается со стоимости в ШК (уменьшает выдачу).
    """
    if amount <= 0:
        return 0, 0.0, "Сумма должна быть больше нуля."
    if dst == "shimcoins":
        return 0, 0.0, "Менять на шимкоины нельзя."
    if src == dst:
        return 0, 0.0, "Одинаковые валюты."

    fee = await fee_pct() / 100.0
    # Комиссия ТОЛЬКО на обмен грибы<->коины (спекулятивный). Операции с шимкоинами
    # (трата денег игрока: ШК->грибы, ШК->коины) — БЕЗ комиссии.
    is_gm = {src, dst} == {"mushrooms", "coins"}
    if not is_gm:
        fee = 0.0

    # стоимость src в ШК
    if src == "mushrooms":
        shk = amount * await _shk_per_mushroom()
    elif src == "coins":
        shk = amount * await _shk_per_coin()
    elif src == "shimcoins":
        shk = float(amount)
    else:
        return 0, 0.0, "Неизвестная валюта."

    # комиссия срезает часть стоимости
    shk_after = shk * (1.0 - fee)

    # конвертируем ШК -> dst
    if dst == "mushrooms":
        got = int(shk_after / await _shk_per_mushroom())
    elif dst == "coins":
        got = int(shk_after / await _shk_per_coin())
    else:
        return 0, 0.0, "Неизвестная валюта."

    if got <= 0:
        return 0, 0.0, "Слишком мало — на выходе ноль. Увеличь сумму."
    return got, shk_after, ""


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
