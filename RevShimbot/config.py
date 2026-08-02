import os
from dataclasses import dataclass


def _env_int(name: str, default: int = 0) -> int:
    """Безопасно читает целочисленную переменную окружения.
    Пустое значение, пробелы или мусор трактуются как default (а не краш).
    Это важно для Railway: там переменную нередко добавляют, но оставляют пустой."""
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "reviewbot")
    CACHE_CHAT_ID: int = _env_int("CACHE_CHAT_ID")
    ADMIN_TG_ID: int = _env_int("ADMIN_TG_ID")
    # Юзернейм админа (без @) — для сообщения забаненным «напишите для разбана»
    ADMIN_USERNAME: str = os.getenv("ADMIN_USERNAME", "")
    # Тестовый режим рекламы: если задан ID — рекламу получит только он.
    # Если 0, пусто или переменная не задана — рекламу получают ВСЕ пользователи.
    AD_TEST_ID: int = _env_int("AD_TEST_ID")
