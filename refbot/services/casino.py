"""
Казино: кейсы.

Открытие кейса: игрок платит цену, получает приз по взвешенному распределению.
RTP ~85% (казна в плюсе ~14%), игрок всегда что-то получает (минимум ×0.25).
Цена и призы в грибах; за коины всё ×COIN_RATE.

Роллы честные (secrets). Всё внутри одной транзакции: списание цены + начисление
приза идемпотентны, гонки исключены.
"""
import secrets

from config import CASES, COIN_RATE, SUPER_ADMINS


async def enabled() -> bool:
    """Казино включено для всех?"""
    from services import settings
    return (await settings.get("casino.enabled", "0")) == "1"


async def visible(uid: int) -> bool:
    """Видит ли пользователь казино: включено для всех ИЛИ он админ."""
    import db
    if await enabled():
        return True
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


def roll_prize(case_key: str) -> tuple[float, float]:
    """
    Выбрать приз кейса. Возвращает (множитель, вероятность_этого_приза).
    Множитель — доля от цены (0.25..10.0).
    """
    _, _, prizes = CASES[case_key]
    r = secrets.randbelow(10**9) / 10**9
    acc = 0.0
    for mult, p in prizes:
        acc += p
        if r <= acc:
            return mult, p
    return prizes[-1][0], prizes[-1][1]  # фолбэк — последний (недостижимо)


def case_price(case_key: str, currency: str) -> int:
    """Цена кейса в выбранной валюте."""
    _, price_mush, _ = CASES[case_key]
    return price_mush * COIN_RATE if currency == "coins" else price_mush


def prize_amount(case_key: str, currency: str, mult: float) -> int:
    """Сумма выигрыша в валюте по множителю."""
    price = case_price(case_key, currency)
    return int(price * mult)


def case_title(case_key: str) -> str:
    return CASES[case_key][0]


def all_cases() -> list[tuple[str, str, int]]:
    """Список (key, название, цена_в_грибах) для меню."""
    return [(k, v[0], v[1]) for k, v in CASES.items()]
