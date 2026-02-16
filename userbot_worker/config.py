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


def _getintset_csv(name: str) -> set[int]:
    raw = _getenv(name)
    if not raw:
        return set()

    values: set[int] = set()
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        try:
            values.add(int(item))
        except ValueError as exc:
            raise RuntimeError(f"{name} contains non-integer value: {item!r}") from exc
    return values




def _getintlist_csv(name: str) -> list[int]:
    raw = _getenv(name)
    if not raw:
        return []

    values: list[int] = []
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError as exc:
            raise RuntimeError(f"{name} contains non-integer value: {item!r}") from exc
    return values

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
    storage_chat_id: int
    blacklist_chat_ids: set[int]
    max_task_attempts: int
    target_chat_ids: list[int]
    targets_sync_seconds: int
    auto_targets_mode: str


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
        storage_chat_id=_getint("STORAGE_CHAT_ID", 0),
        blacklist_chat_ids=_getintset_csv("BLACKLIST_CHAT_IDS"),
        max_task_attempts=max(1, _getint("MAX_TASK_ATTEMPTS", 3)),
        target_chat_ids=_getintlist_csv("TARGET_CHAT_IDS"),
        targets_sync_seconds=max(30, _getint("TARGETS_SYNC_SECONDS", 600)),
        auto_targets_mode=(_getenv("AUTO_TARGETS_MODE", "groups_only").lower() or "groups_only"),
    )

    if settings.max_seconds_between_chats < settings.min_seconds_between_chats:
        object.__setattr__(settings, "max_seconds_between_chats", settings.min_seconds_between_chats)

    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required")
    if settings.telegram_api_id <= 0:
        raise RuntimeError("TELEGRAM_API_ID is required")
    if not settings.telegram_api_hash:
        raise RuntimeError("TELEGRAM_API_HASH is required")
    if settings.auto_targets_mode not in {"groups_only", "groups_and_channels"}:
        raise RuntimeError("AUTO_TARGETS_MODE must be 'groups_only' or 'groups_and_channels'")

    return settings