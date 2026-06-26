import os
from dataclasses import dataclass


@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "reviewbot")
    CACHE_CHAT_ID: int = int(os.getenv("CACHE_CHAT_ID", "0"))
    ADMIN_TG_ID: int = int(os.getenv("ADMIN_TG_ID", "0"))
    # Тестовый режим рекламы: если задан ID — рекламу получит только он.
    # Если 0 — рекламу получают все пользователи бота.
    AD_TEST_ID: int = int(os.getenv("AD_TEST_ID", "0"))
