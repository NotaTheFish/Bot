from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class DateRange:
    start: Optional[datetime]
    end: datetime


def _ensure_utc_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    return value.astimezone(timezone.utc)


def period_to_utc_range(period: str, tz: ZoneInfo, now_utc: datetime | None = None) -> DateRange:
    period_normalized = (period or "").strip().lower()
    end = _ensure_utc_aware(now_utc or datetime.now(timezone.utc))

    if period_normalized == "all":
        return DateRange(start=None, end=end)

    now_local = end.astimezone(tz)

    if period_normalized in {"day", "1day", "today"}:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        return DateRange(start=start_local.astimezone(timezone.utc), end=end)

    if period_normalized in {"7days", "week"}:
        start_local = now_local - timedelta(days=7)
        return DateRange(start=start_local.astimezone(timezone.utc), end=end)

    if period_normalized in {"30days", "month"}:
        start_local = now_local - timedelta(days=30)
        return DateRange(start=start_local.astimezone(timezone.utc), end=end)

    raise ValueError("unsupported period")
