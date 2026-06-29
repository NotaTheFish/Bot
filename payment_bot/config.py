from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    BOT_TOKEN: str
    DATABASE_URL: str
    MAIN_ADMIN_ID: int

    # Encryption key for aggregator API keys (32 bytes base64)
    ENCRYPTION_KEY: str

    # Currency rates API
    EXCHANGE_RATE_API_KEY: Optional[str] = None

    # Lava.ru
    LAVA_API_URL: str = "https://api.lava.ru"

    # Payok.io
    PAYOK_API_URL: str = "https://payok.io/api"

    # Freekassa
    FREEKASSA_API_URL: str = "https://api.freekassa.ru/v1"

    # Deal settings
    DEAL_TTL_MINUTES: int = 15
    EXPIRED_CHECK_INTERVAL: int = 300  # seconds
    RATE_UPDATE_INTERVAL: int = 1800   # seconds

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
