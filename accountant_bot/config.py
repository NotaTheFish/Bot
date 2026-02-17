from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    ACCOUNTANT_BOT_TOKEN: str
    ACCOUNTANT_ADMIN_IDS: list[int]
    DATABASE_URL: str
    REVIEWS_CHANNEL_ID: int

    TG_API_ID: int
    TG_API_HASH: str
    ACCOUNTANT_TG_STRING_SESSION: str

    ABOUT_TEMPLATE: str = "Отзывов: {count}. Обновлено: {date}"
    ABOUT_DATE_FORMAT: str = "%d.%m.%Y"

    TABOO_CHAT_IDS: list[int] | None = None
    MEDIA_GROUP_BUFFER_SECONDS: float = 1.5
    ABOUT_UPDATE_DEBOUNCE_SECONDS: float = 2.0


def _getenv(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _get_required_env(name: str) -> str:
    value = _getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _get_required_env_any(*names: str) -> str:
    """
    Return the first non-empty environment variable from the given names.
    Useful for backward compatibility: TG_API_ID vs TELEGRAM_API_ID, etc.
    """
    for name in names:
        value = _getenv(name)
        if value:
            return value
    raise RuntimeError(f"One of {', '.join(names)} is required")


def _parse_int(name: str, raw: str) -> int:
    try:
        return int(raw.strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _parse_float(name: str, raw: str) -> float:
    try:
        return float(raw.strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc


def _parse_int_list_csv(name: str, raw: str) -> list[int]:
    if not raw.strip():
        return []

    values: list[int] = []
    for idx, part in enumerate(raw.split(","), start=1):
        item = part.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError as exc:
            raise ValueError(f"{name} has invalid integer at position {idx}: {item!r}") from exc
    return values


def _parse_float_env(name: str, default: float) -> float:
    raw = _getenv(name)
    if not raw:
        return default
    return _parse_float(name, raw)


def _parse_int_list_env(name: str, *, required: bool = False) -> list[int]:
    raw = _get_required_env(name) if required else _getenv(name)
    values = _parse_int_list_csv(name, raw)
    if required and not values:
        raise ValueError(f"{name} must contain at least one integer ID")
    return values


def load_settings() -> Settings:
    """
    Load app settings from environment variables.
    Supports backward-compatible env names:
      - TG_API_ID or TELEGRAM_API_ID
      - TG_API_HASH or TELEGRAM_API_HASH
    """
    # Local dev convenience (does nothing on Railway if dotenv not installed)
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    reviews_channel_id = _parse_int("REVIEWS_CHANNEL_ID", _get_required_env("REVIEWS_CHANNEL_ID"))

    taboo_chat_ids_raw = _getenv("TABOO_CHAT_IDS")
    taboo_chat_ids = (
        _parse_int_list_csv("TABOO_CHAT_IDS", taboo_chat_ids_raw)
        if taboo_chat_ids_raw
        else [reviews_channel_id]
    )

    tg_api_id_raw = _get_required_env_any("TG_API_ID", "TELEGRAM_API_ID")
    tg_api_hash = _get_required_env_any("TG_API_HASH", "TELEGRAM_API_HASH")

    return Settings(
        ACCOUNTANT_BOT_TOKEN=_get_required_env("ACCOUNTANT_BOT_TOKEN"),
        ACCOUNTANT_ADMIN_IDS=_parse_int_list_env("ACCOUNTANT_ADMIN_IDS", required=True),
        DATABASE_URL=_get_required_env("DATABASE_URL"),
        REVIEWS_CHANNEL_ID=reviews_channel_id,

        TG_API_ID=_parse_int("TG_API_ID/TELEGRAM_API_ID", tg_api_id_raw),
        TG_API_HASH=tg_api_hash,
        ACCOUNTANT_TG_STRING_SESSION=_get_required_env("ACCOUNTANT_TG_STRING_SESSION"),

        ABOUT_TEMPLATE=_getenv("ABOUT_TEMPLATE", "Отзывов: {count}. Обновлено: {date}"),
        ABOUT_DATE_FORMAT=_getenv("ABOUT_DATE_FORMAT", "%d.%m.%Y"),

        TABOO_CHAT_IDS=taboo_chat_ids,
        MEDIA_GROUP_BUFFER_SECONDS=_parse_float_env("MEDIA_GROUP_BUFFER_SECONDS", 1.5),
        ABOUT_UPDATE_DEBOUNCE_SECONDS=_parse_float_env("ABOUT_UPDATE_DEBOUNCE_SECONDS", 2.0),
    )
