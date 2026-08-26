"""
Токены Revive / Max / Partials.

Свойства:
- целые (без дробей),
- покупаются за шимкоины (в центах) ИЛИ за грибы,
- выводятся (через заявки),
- начисляются/изымаются админом (ваучер + ручная выдача),
- НЕЛЬЗЯ обменивать и тратить в казино.

Цены задаёт админ отдельно для каждого токена: 4 цены на токен —
[шимкоины-розница, шимкоины-опт, грибы-розница, грибы-опт].
Опт начинается с WHOLESALE_FROM токенов за одну покупку.
Цена в шимкоинах хранится/задаётся в ЦЕНТАХ (как весь шимкоин-баланс).
Округление ВСЕГДА в пользу казны.
"""
import math

from services import settings

# machine-код -> отображаемое имя
TOKENS = {
    "revive":   "Revive",
    "max":      "Max",
    "partials": "Partials",
}

# синонимы для ваучера/выдачи: слово (нижний регистр) -> код токена
TOKEN_WORDS = {
    # revive
    "revive": "revive", "ревы": "revive", "ревов": "revive", "рев": "revive",
    "рева": "revive", "реву": "revive", "ревайв": "revive",
    # max
    "max": "max", "максы": "max", "максов": "max", "макс": "max", "макса": "max",
    # partials
    "partials": "partials", "партайлы": "partials", "партиалы": "partials",
    "партиал": "partials", "частицы": "partials", "частиц": "partials",
    "партайл": "partials",
}

WHOLESALE_FROM = 1000   # >= 1000 токенов за раз — опт, иначе розница
BUY_STEP = 50           # покупка токенов всегда кратна 50 (обе валюты)
# ШК-цена задаётся за партию из BUY_STEP штук; грибная — за 1 штуку.
WITHDRAW_STEP = 50      # вывод токенов кратен 50, минимум 50


def is_token(cur: str) -> bool:
    return cur in TOKENS


def match_token_word(word: str):
    """Слово -> код токена или None."""
    return TOKEN_WORDS.get((word or "").strip().lower())


# ---------------- цены (admin задаёт в rb_settings) ----------------
# ключи: token.<code>.<pay>.<tier>  где pay in {shk, mush}, tier in {retail, whole}
# значение — целое: за 1 токен сколько платить (shk в ЦЕНТАХ, mush в грибах).
async def get_price(token: str, pay: str, tier: str) -> int:
    raw = await settings.get(f"token.{token}.{pay}.{tier}", "0")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


async def set_price(token: str, pay: str, tier: str, value: int, actor: int) -> None:
    await settings.set(f"token.{token}.{pay}.{tier}", str(int(value)), actor)


def _tier(qty: int) -> str:
    return "whole" if qty >= WHOLESALE_FROM else "retail"


async def quote_buy(token: str, pay: str, qty: int) -> tuple[int, str]:
    """
    Купить qty токенов за pay ('shk'|'mush').
    Возвращает (стоимость, "") или (0, ошибка). Стоимость: shk — в ЦЕНТАХ, mush — в грибах.

    Покупка ВСЕГДА кратна BUY_STEP (50) — и за шимкоины, и за грибы.
    Цена:
      - shk: задаётся ЗА ПАРТИЮ из BUY_STEP штук (в центах). Стоимость = партий * цена_партии.
      - mush: задаётся ЗА 1 штуку (в грибах). Стоимость = qty * цена_за_штуку.
    Опт/розница по tier от общего qty (>=WHOLESALE_FROM — опт).
    """
    if token not in TOKENS:
        return 0, "Неизвестный токен."
    if qty <= 0:
        return 0, "Количество должно быть больше нуля."
    # кратность 50 для любой покупки
    if qty % BUY_STEP != 0:
        lo = (qty // BUY_STEP) * BUY_STEP
        hi = lo + BUY_STEP
        near = f"{lo} или {hi}" if lo > 0 else f"{hi}"
        return 0, f"Покупка кратна {BUY_STEP} шт. Ближайшее: {near}."
    per = await get_price(token, pay, _tier(qty))
    if per <= 0:
        return 0, "Цена не задана — обратись к админу."
    if pay == "shk":
        cost = (qty // BUY_STEP) * per   # per — цена за партию из BUY_STEP
    else:
        cost = qty * per                 # per — цена за 1 штуку (грибы)
    return cost, ""


async def quote_buy_reverse(token: str, pay: str, budget: int) -> tuple[int, int, str]:
    """
    Обратный расчёт: есть budget (shk-центы или грибы), сколько токенов выйдет.
    Возвращает (кол-во_токенов, реальная_стоимость, ошибка).
    Количество ВСЕГДА кратно BUY_STEP, округляем ВНИЗ (в пользу казны).
    Цена shk — за партию BUY_STEP; mush — за 1 штуку.
    Берём лучший для игрока валидный тариф (опт, если дотянул до порога).
    """
    if token not in TOKENS:
        return 0, 0, "Неизвестный токен."
    if budget <= 0:
        return 0, 0, "Сумма должна быть больше нуля."

    per_retail = await get_price(token, pay, "retail")
    per_whole = await get_price(token, pay, "whole")
    if per_retail <= 0 and per_whole <= 0:
        return 0, 0, "Цена не задана — обратись к админу."

    def _qty_for(per_price):
        # сколько токенов (кратно BUY_STEP) даёт budget по данной цене
        if pay == "shk":
            batches = budget // per_price          # per_price — за партию
            return batches * BUY_STEP, batches * per_price
        else:
            q = budget // per_price                # per_price — за штуку
            q = (q // BUY_STEP) * BUY_STEP          # округлить вниз до партии
            return q, q * per_price

    candidates = []
    if per_retail > 0:
        q, cost = _qty_for(per_retail)
        if q >= BUY_STEP and q < WHOLESALE_FROM:    # розница ниже опта
            candidates.append((q, cost))
    if per_whole > 0:
        q, cost = _qty_for(per_whole)
        if q >= WHOLESALE_FROM:                     # опт от порога
            candidates.append((q, cost))
    if not candidates:
        return 0, 0, f"Недостаточно — минимум {BUY_STEP} шт партией."
    q, cost = max(candidates, key=lambda x: x[0])
    return q, cost, ""


async def owned_lines(bal: dict) -> list[str]:
    """
    Строки для ненулевых токенов в балансе (для профиля/баланса/карточки).
    Возвращает список строк вида '❤️‍🔥 Revive: 1 200'. Пусто, если токенов нет.
    """
    out = []
    for code, name in TOKENS.items():
        v = bal.get(code, 0) or 0
        if v > 0:
            e = await settings.emoji(code)
            out.append(f"{e} {name}: {v:,}".replace(",", " "))
    return out
