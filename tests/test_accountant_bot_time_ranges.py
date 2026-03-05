from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from accountant_bot.time_ranges import period_to_utc_range


def test_period_all_returns_open_start_and_now_end():
    now_utc = datetime(2026, 3, 1, 12, 34, tzinfo=timezone.utc)

    date_range = period_to_utc_range("all", ZoneInfo("Europe/Moscow"), now_utc=now_utc)

    assert date_range.start is None
    assert date_range.end == now_utc


def test_period_day_uses_local_midnight_start():
    tz = ZoneInfo("Europe/Moscow")
    now_utc = datetime(2026, 3, 1, 12, 34, tzinfo=timezone.utc)

    date_range = period_to_utc_range("today", tz, now_utc=now_utc)

    assert date_range.end == now_utc
    assert date_range.start == datetime(2026, 2, 28, 21, 0, tzinfo=timezone.utc)


def test_period_7days_subtracts_in_local_timezone():
    tz = ZoneInfo("America/New_York")
    now_utc = datetime(2026, 1, 15, 15, 0, tzinfo=timezone.utc)

    date_range = period_to_utc_range("week", tz, now_utc=now_utc)

    assert date_range.start == datetime(2026, 1, 8, 15, 0, tzinfo=timezone.utc)
    assert date_range.end == now_utc


def test_period_30days_subtracts_in_local_timezone():
    tz = ZoneInfo("Asia/Tokyo")
    now_utc = datetime(2026, 2, 15, 1, 30, tzinfo=timezone.utc)

    date_range = period_to_utc_range("30days", tz, now_utc=now_utc)

    assert date_range.start == datetime(2026, 1, 16, 1, 30, tzinfo=timezone.utc)
    assert date_range.end == now_utc


def test_period_raises_for_naive_now_utc():
    with pytest.raises(ValueError, match="timezone-aware"):
        period_to_utc_range("all", ZoneInfo("UTC"), now_utc=datetime(2026, 1, 1, 0, 0))