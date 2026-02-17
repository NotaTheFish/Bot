from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from accountant_bot.admin_bot import _receipt_details_text, _receipt_list_keyboard, add_check_confirm
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


@pytest.mark.asyncio
async def test_add_check_confirm_cancel_does_not_write_to_db(monkeypatch):
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

    await add_check_confirm(message, state, pool=object(), settings=settings)

    cancel.assert_awaited_once()
    add_receipt.assert_not_called()


@pytest.mark.asyncio
async def test_add_check_confirm_invalid_button_does_not_write_to_db(monkeypatch):
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

    await add_check_confirm(message, state, pool=object(), settings=settings)

    add_receipt.assert_not_called()
    assert safe_send_message.await_count == 1
    assert "Подтвердите сохранение" in safe_send_message.await_args.args[2]