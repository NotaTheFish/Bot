from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bot_token: str
    database_url: str
    telegram_api_id: int
    telegram_api_hash: str
    accountant_tg_string_session: str


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def load_settings() -> Settings:
    """Load app settings from environment variables."""
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        pass

    return Settings(
        bot_token=_require_env("ACCOUNTANT_BOT_TOKEN"),
        database_url=_require_env("DATABASE_URL"),
        telegram_api_id=int(_require_env("TELEGRAM_API_ID")),
        telegram_api_hash=_require_env("TELEGRAM_API_HASH"),
        accountant_tg_string_session=_require_env("ACCOUNTANT_TG_STRING_SESSION"),
    )