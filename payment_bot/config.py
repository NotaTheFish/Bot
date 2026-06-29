from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    BOT_TOKEN: str
    DATABASE_URL: str
    MAIN_ADMIN_ID: int
    ENCRYPTION_KEY: str
    EXCHANGE_RATE_API_KEY: Optional[str] = None
    LAVA_API_URL: str = "https://api.lava.ru"
    PAYOK_API_URL: str = "https://payok.io/api"
    FREEKASSA_API_URL: str = "https://api.freekassa.ru/v1"
    DEAL_TTL_MINUTES: int = 15
    EXPIRED_CHECK_INTERVAL: int = 300
    RATE_UPDATE_INTERVAL: int = 1800

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
