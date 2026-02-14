import os
from dataclasses import dataclass


def _getenv(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _getint(name: str, default: int = 0) -> int:
    raw = _getenv(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    database_url: str
    telegram_api_id: int
    telegram_api_hash: str
    telethon_session: str
    min_seconds_between_chats: int
    max_seconds_between_chats: int
    worker_poll_seconds: int
    admin_notify_bot_token: str
    admin_id: int


def load_settings() -> Settings:
    settings = Settings(
        database_url=_getenv("DATABASE_URL"),
        telegram_api_id=_getint("TELEGRAM_API_ID", 0),
        telegram_api_hash=_getenv("TELEGRAM_API_HASH"),
        telethon_session=_getenv("TELETHON_SESSION", "userbot.session"),
        min_seconds_between_chats=max(0, _getint("MIN_SECONDS_BETWEEN_CHATS", 20)),
        max_seconds_between_chats=max(0, _getint("MAX_SECONDS_BETWEEN_CHATS", 40)),
        worker_poll_seconds=max(1, _getint("WORKER_POLL_SECONDS", 10)),
        admin_notify_bot_token=_getenv("ADMIN_NOTIFY_BOT_TOKEN"),
        admin_id=_getint("ADMIN_ID", 0),
    )

    if settings.max_seconds_between_chats < settings.min_seconds_between_chats:
        object.__setattr__(settings, "max_seconds_between_chats", settings.min_seconds_between_chats)

    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required")
    if settings.telegram_api_id <= 0:
        raise RuntimeError("TELEGRAM_API_ID is required")
    if not settings.telegram_api_hash:
        raise RuntimeError("TELEGRAM_API_HASH is required")

    return settings