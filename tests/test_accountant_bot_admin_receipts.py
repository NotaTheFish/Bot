from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
import sys
import types

import asyncio

import pytest

if "openpyxl" not in sys.modules:
    fake_openpyxl = types.ModuleType("openpyxl")
    fake_openpyxl.Workbook = object
    fake_styles = types.ModuleType("openpyxl.styles")

    def _style_stub(*args, **kwargs):
        return object()

    fake_styles.Alignment = _style_stub
    fake_styles.Border = _style_stub
    fake_styles.Font = _style_stub
    fake_styles.PatternFill = _style_stub
    fake_styles.Side = _style_stub

    sys.modules["openpyxl"] = fake_openpyxl
    sys.modules["openpyxl.styles"] = fake_styles

from accountant_bot.admin_bot import (
    START_KEYBOARD,
    STATS_KEYBOARD,
    _receipt_details_text,
    _receipt_list_keyboard,
    add_check_confirm,
    show_stats,
)
from accountant_bot.config import Settings


class _DummyState:
    def __init__(self, data=None):
        self._data = data or {}
        self.clear = AsyncMock()

    async def get_data(self):
        return self._data


class _DummyMessage:
    def __init__(self, text: str):
        self.text = text
        self.bot = object()
        self.chat = SimpleNamespace(id=1)
        self.from_user = SimpleNamespace(id=1)


def test_start_keyboard_has_expected_rows_and_labels():
    rows = [[button.text for button in row] for row in START_KEYBOARD.keyboard]
    assert rows == [
        ["📊 Отзывы", "🧾 Чек"],
        ["📤 Excel", "🔎 Найти чек"],
        ["🧾 Последние чеки"],
    ]


def test_stats_keyboard_has_expected_labels_and_callbacks():
    row = STATS_KEYBOARD.inline_keyboard[0]
    assert [button.text for button in row] == ["📅 Сегодня", "📆 7 дней", "🗓 30 дней"]
    assert [button.callback_data for button in row] == ["stats:day", "stats:week", "stats:month"]


def test_show_stats_uses_expected_message_format(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    callback = SimpleNamespace(
        from_user=SimpleNamespace(id=1),
        data="stats:week",
        message=SimpleNamespace(bot=object(), chat=SimpleNamespace(id=1)),
        answer=AsyncMock(),
    )
    settings = Settings(
        ACCOUNTANT_BOT_TOKEN="token",
        ACCOUNTANT_ADMIN_IDS=[1],
        DATABASE_URL="postgresql://localhost/test",
        REVIEWS_CHANNEL_ID=777,
        TG_API_ID=123,
        TG_API_HASH="hash",
        ACCOUNTANT_TG_STRING_SESSION="session",
    )
    reviews_service = SimpleNamespace(get_stats_reviews=AsyncMock(return_value={"added": 10, "deleted": 3, "active": 7}))

    asyncio.run(show_stats(callback, settings, reviews_service))

    sent_text = safe_send_message.await_args.args[2]
    assert sent_text == (
        "📊 Статистика отзывов (7 дней)\n"
        "➕ Добавлено: 10\n"
        "➖ Удалено: 3\n"
        "✅ Активных: 7"
    )


def test_receipt_details_text_contains_status_sum_and_items():
    receipt = {
        "id": 123,
        "created_at": datetime(2026, 2, 17, 12, 30, tzinfo=timezone.utc),
        "status": "created",
        "currency": "RUB",
        "pay_method": "card",
        "note": "main note",
    }
    items = [
        {
            "category": "VID",
            "item_name": "Item A",
            "qty": Decimal("2"),
            "unit_price": Decimal("100"),
            "line_total": Decimal("200"),
            "note": "item note",
        }
    ]

    text = _receipt_details_text(receipt, items)

    assert "🧾 Чек #123" in text
    assert "Статус: created" in text
    assert "Сумма: 200 RUB" in text
    assert "[VID] Item A" in text
    assert "💬 item note" in text


def test_receipt_list_keyboard_uses_compact_button_title():
    rows = [
        {
            "id": 123,
            "created_at": datetime(2026, 2, 17, 12, 30, tzinfo=timezone.utc),
            "total": Decimal("400"),
            "currency": "RUB",
        }
    ]

    keyboard = _receipt_list_keyboard(rows)

    button = keyboard.inline_keyboard[0][0]
    assert button.text == "#123 17.02 400 RUB"
    assert button.callback_data == "receipt:open:123"


def test_add_check_confirm_cancel_does_not_write_to_db(monkeypatch):
    safe_send_message = AsyncMock()
    add_receipt = AsyncMock()
    cancel = AsyncMock()

    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)
    monkeypatch.setattr("accountant_bot.admin_bot.add_receipt_with_items", add_receipt)
    monkeypatch.setattr("accountant_bot.admin_bot._cancel_add_check", cancel)

    settings = Settings(
        ACCOUNTANT_BOT_TOKEN="token",
        ACCOUNTANT_ADMIN_IDS=[1],
        DATABASE_URL="postgresql://localhost/test",
        REVIEWS_CHANNEL_ID=777,
        TG_API_ID=123,
        TG_API_HASH="hash",
        ACCOUNTANT_TG_STRING_SESSION="session",
    )

    state = _DummyState(data={"items": [{"line_total": "100"}]})
    message = _DummyMessage("Отменить")

    asyncio.run(add_check_confirm(message, state, pool=object(), settings=settings))

    cancel.assert_awaited_once()
    add_receipt.assert_not_called()


def test_add_check_confirm_invalid_button_does_not_write_to_db(monkeypatch):
    safe_send_message = AsyncMock()
    add_receipt = AsyncMock()

    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)
    monkeypatch.setattr("accountant_bot.admin_bot.add_receipt_with_items", add_receipt)

    settings = Settings(
        ACCOUNTANT_BOT_TOKEN="token",
        ACCOUNTANT_ADMIN_IDS=[1],
        DATABASE_URL="postgresql://localhost/test",
        REVIEWS_CHANNEL_ID=777,
        TG_API_ID=123,
        TG_API_HASH="hash",
        ACCOUNTANT_TG_STRING_SESSION="session",
    )

    state = _DummyState(data={"items": [{"line_total": "100"}]})
    message = _DummyMessage("что-то другое")

    asyncio.run(add_check_confirm(message, state, pool=object(), settings=settings))

    add_receipt.assert_not_called()
    assert safe_send_message.await_count == 1
    assert "Подтвердите сохранение" in safe_send_message.await_args.args[2]