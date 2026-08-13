"""
Промокоды: парсинг срока/активаций при создании, форматирование для списка.

Срок: «1д»/«1 день», «24ч»/«24 часа», «безлим». Активации: число или «безлим».
Награда задаётся в грибах; коины выдаются по курсу (COIN_RATE) при активации.
"""
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Moscow")


def parse_unlimited(text: str) -> bool:
    t = (text or "").strip().lower()
    return t in ("безлим", "безлимит", "unlim", "∞", "-")


def parse_acts(text: str) -> int | None | str:
    """
    Количество активаций: число или «безлим».
    Возвращает int (лимит), None (безлим) или 'err'.
    """
    if parse_unlimited(text):
        return None
    t = re.sub(r"[^\d]", "", text or "")
    if not t:
        return "err"
    n = int(t)
    return n if n > 0 else "err"


def parse_expiry(text: str):
    """
    Срок годности. Форматы: «1д», «3 дня», «24ч», «12 часов», «30м», «безлим».
    Возвращает datetime (МСК) окончания, None (бессрочно) или 'err'.
    """
    t = (text or "").strip().lower()
    if parse_unlimited(t):
        return None
    m = re.match(r"^(\d+)\s*(д|дн|день|дня|дней|d|ч|час|часа|часов|h|м|мин|минут|минуты|m)\.?$", t)
    if not m:
        return "err"
    num = int(m.group(1))
    unit = m.group(2)
    now = datetime.now(TZ)
    if unit in ("д", "дн", "день", "дня", "дней", "d"):
        return now + timedelta(days=num)
    if unit in ("ч", "час", "часа", "часов", "h"):
        return now + timedelta(hours=num)
    if unit in ("м", "мин", "минут", "минуты", "m"):
        return now + timedelta(minutes=num)
    return "err"


def is_expired(expires_at) -> bool:
    if expires_at is None:
        return False
    return expires_at < datetime.now(TZ)


def status_line(p) -> str:
    """Строка статуса промокода для списка."""
    parts = []
    if p["max_acts"] is None:
        parts.append(f"активаций: {p['used']}/∞")
    else:
        parts.append(f"активаций: {p['used']}/{p['max_acts']}")
    if p["expires_at"] is None:
        parts.append("бессрочно")
    elif is_expired(p["expires_at"]):
        parts.append("истёк")
    else:
        parts.append("до " + p["expires_at"].astimezone(TZ).strftime("%d.%m %H:%M"))
    return ", ".join(parts)


def is_active(p) -> bool:
    """Промокод ещё рабочий (не истёк и есть активации)?"""
    if is_expired(p["expires_at"]):
        return False
    if p["max_acts"] is not None and p["used"] >= p["max_acts"]:
        return False
    return True
