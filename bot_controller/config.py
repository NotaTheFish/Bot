import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ControllerSettings:
    admin_broadcast_notifications: bool = False


def _get_env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _get_on_off(name: str, default: str = "off") -> bool:
    raw = _get_env_str(name, default).lower()
    if raw not in {"on", "off"}:
        raise RuntimeError(f"{name} must be 'on' or 'off'")
    return raw == "on"


def load_controller_settings() -> ControllerSettings:
    return ControllerSettings(
        admin_broadcast_notifications=_get_on_off("ADMIN_BROADCAST_NOTIFICATIONS", "off"),
    )