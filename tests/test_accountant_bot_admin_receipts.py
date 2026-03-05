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
    fake_chart = types.ModuleType("openpyxl.chart")
    fake_chart.BarChart = object
    fake_chart.LineChart = object
    fake_chart.Reference = object

    sys.modules["openpyxl"] = fake_openpyxl
    sys.modules["openpyxl.styles"] = fake_styles
    sys.modules["openpyxl.chart"] = fake_chart

from accountant_bot.admin_bot import (
    PRODUCTS_MENU_KEYBOARD,
    ProductCatalogFSM,
    CURRENCY_KEYBOARD,
    GAME_KEYBOARD,
    PAY_METHOD_KEYBOARD,
    START_KEYBOARD,
    STATS_KEYBOARD,
    start_receipt_lookup,
    _receipt_details_text,
    _receipt_list_keyboard,
    add_check_game_callback,
    add_check_currency_callback,
    add_check_currency_custom,
    add_check_pay_method_callback,
    add_check_confirm,
    products_delete_list,
    show_stats,
)
from accountant_bot.config import Settings
from accountant_bot.time_ranges import DateRange


class _DummyState:
    def __init__(self, data=None):
        self._data = data or {}
        self.current_state = None
        self.clear = AsyncMock()

    async def get_data(self):
        return self._data

    async def set_state(self, new_state):
        self.current_state = new_state

    async def update_data(self, **kwargs):
        self._data.update(kwargs)


class _DummyCallback:
    def __init__(self, data: str):
        self.data = data
        self.message = SimpleNamespace(bot=object(), chat=SimpleNamespace(id=1))
        self.answer = AsyncMock()


class _DummyMessage:
    def __init__(self, text: str):
        self.text = text
        self.bot = object()
        self.chat = SimpleNamespace(id=1)
        self.from_user = SimpleNamespace(id=1)




def test_start_receipt_lookup_uses_back_only_keyboard(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    settings = Settings(
        ACCOUNTANT_BOT_TOKEN="token",
        ACCOUNTANT_ADMIN_IDS=[1],
        DATABASE_URL="postgresql://localhost/test",
        REVIEWS_CHANNEL_ID=777,
        TG_API_ID=123,
        TG_API_HASH="hash",
        ACCOUNTANT_TG_STRING_SESSION="session",
    )
    state = _DummyState()
    message = _DummyMessage("🔎 Найти чек")

    asyncio.run(start_receipt_lookup(message, state, settings))

    keyboard = safe_send_message.await_args.kwargs["reply_markup"]
    rows = [[button.text for button in row] for row in keyboard.keyboard]
    assert rows == [["Назад"]]


def test_products_delete_list_back_returns_to_products_menu(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    settings = Settings(
        ACCOUNTANT_BOT_TOKEN="token",
        ACCOUNTANT_ADMIN_IDS=[1],
        DATABASE_URL="postgresql://localhost/test",
        REVIEWS_CHANNEL_ID=777,
        TG_API_ID=123,
        TG_API_HASH="hash",
        ACCOUNTANT_TG_STRING_SESSION="session",
    )
    state = _DummyState()
    message = _DummyMessage("Назад")

    asyncio.run(products_delete_list(message, state, pool=AsyncMock(), settings=settings))

    assert state.current_state == ProductCatalogFSM.menu
    assert safe_send_message.await_args.args[2] == "Управление товарами:"
    assert safe_send_message.await_args.kwargs["reply_markup"] == PRODUCTS_MENU_KEYBOARD

def test_start_keyboard_has_expected_rows_and_labels():
    rows = [[button.text for button in row] for row in START_KEYBOARD.keyboard]
    assert rows == [
        ["📊 Отзывы", "🧾 Чек"],
        ["📤 Excel", "🔎 Найти чек"],
        ["🧾 Последние чеки"],
        ["📦 Управление товарами"],
    ]


def test_stats_keyboard_has_expected_labels_and_callbacks():
    row = STATS_KEYBOARD.inline_keyboard[0]
    assert [button.text for button in row] == ["📅 Сегодня", "📆 7 дней", "🗓 30 дней"]
    assert [button.callback_data for button in row] == ["stats:day", "stats:week", "stats:month"]




def test_game_keyboard_has_expected_buttons_and_callbacks():
    rows = GAME_KEYBOARD.inline_keyboard
    assert [button.text for button in rows[0]] == ["COS", "DA"]
    assert [button.callback_data for button in rows[0]] == ["add_check:game:COS", "add_check:game:DA"]


def test_game_callback_sets_game_and_moves_to_currency(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    callback = _DummyCallback("add_check:game:DA")
    state = _DummyState(data={"items": []})

    asyncio.run(add_check_game_callback(callback, state))

    assert state._data["game_code"] == "DA"
    assert state.current_state.state.endswith(":currency")
    assert safe_send_message.await_args_list[-1].args[2] == "💱 Выберите валюту:"

def test_currency_keyboard_has_expected_buttons_and_callbacks():
    rows = CURRENCY_KEYBOARD.inline_keyboard
    assert [button.text for button in rows[0]] == ["₽ RUB", "₴ UAH"]
    assert [button.callback_data for button in rows[0]] == ["add_check:currency:RUB", "add_check:currency:UAH"]
    assert [button.text for button in rows[1]] == ["$ USD", "€ EUR"]
    assert rows[2][0].text == "✍️ Другое"
    assert rows[3][0].text == "⬅️ Назад"
    assert rows[3][1].text == "❌ Отмена"


def test_pay_method_keyboard_has_expected_buttons_and_callbacks():
    rows = PAY_METHOD_KEYBOARD.inline_keyboard
    assert [button.text for button in rows[0]] == ["💳 Карта", "💵 Наличные"]
    assert rows[1][0].text == "🪙 Крипта"
    assert rows[2][0].text == "✍️ Другое"
    assert rows[3][0].text == "⏭ Пропустить"
    assert [button.text for button in rows[4]] == ["⬅️ Назад", "❌ Отмена"]


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

    reviews_service.get_stats_reviews.assert_awaited_once()
    date_range = reviews_service.get_stats_reviews.await_args.args[0]
    assert isinstance(date_range, DateRange)

    sent_text = safe_send_message.await_args.args[2]
    assert sent_text == (
        "📊 Статистика отзывов (7 дней)\n"
        "➕ Добавлено: 10\n"
        "➖ Удалено: 3\n"
        "✅ Активных: 7\n"
        "🌍 TZ: UTC"
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
        },
        {
            "category": "TOKENS",
            "item_name": "Item B",
            "qty": Decimal("3"),
            "unit_price": Decimal("10"),
            "line_total": Decimal("30"),
            "note": "",
        },
        {
            "category": "MUSHROOMS",
            "item_name": "Item C",
            "qty": Decimal("1"),
            "unit_price": Decimal("50"),
            "line_total": Decimal("50"),
            "note": "",
        },
        {
            "category": "OTHER",
            "item_name": "Item D",
            "qty": Decimal("4"),
            "unit_price": Decimal("5"),
            "line_total": Decimal("20"),
            "note": "",
        },
    ]

    text = _receipt_details_text(receipt, items)

    assert "🧾 Чек #123" in text
    assert "Статус: created" in text
    assert "Сумма: 300 RUB" in text
    assert "[🐉 Вид] Item A" in text
    assert "[🪙 Токены] Item B" in text
    assert "[🍄 Грибы] Item C" in text
    assert "[✍️ Другое] Item D" in text
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


def test_add_check_confirm_passes_game_code_to_storage(monkeypatch):
    safe_send_message = AsyncMock()
    add_receipt = AsyncMock(return_value={"receipt": {"id": 77}, "items": [{"line_total": "100"}]})

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

    state = _DummyState(data={"items": [{"line_total": "100"}], "game_code": "DA", "currency": "RUB"})
    message = _DummyMessage("Сохранить")

    asyncio.run(add_check_confirm(message, state, pool=object(), settings=settings))

    assert add_receipt.await_args.kwargs["game_code"] == "DA"


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


def test_currency_callback_sets_predefined_currency_and_moves_to_pay_method(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    callback = _DummyCallback("add_check:currency:USD")
    state = _DummyState(data={"items": []})

    asyncio.run(add_check_currency_callback(callback, state))

    assert state._data["currency"] == "USD"
    assert state.current_state.state.endswith(":pay_method")
    assert safe_send_message.await_args_list[-1].args[2] == "💳 Способ оплаты:"


def test_currency_custom_text_validation_and_upper(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    state = _DummyState(data={"items": []})
    invalid_message = _DummyMessage("x")
    valid_message = _DummyMessage("usd")

    asyncio.run(add_check_currency_custom(invalid_message, state))
    assert "2 до 6" in safe_send_message.await_args.args[2]

    asyncio.run(add_check_currency_custom(valid_message, state))
    assert state._data["currency"] == "USD"
    assert state.current_state.state.endswith(":pay_method")


def test_pay_method_callback_paths_for_skip_and_other(monkeypatch):
    safe_send_message = AsyncMock()
    monkeypatch.setattr("accountant_bot.admin_bot.safe_send_message", safe_send_message)

    skip_callback = _DummyCallback("add_check:pay_method:skip")
    other_callback = _DummyCallback("add_check:pay_method:other")
    state = _DummyState(data={"items": []})

    asyncio.run(add_check_pay_method_callback(skip_callback, state))
    assert state._data["pay_method"] is None
    assert state.current_state.state.endswith(":note")

    asyncio.run(add_check_pay_method_callback(other_callback, state))
    assert state.current_state.state.endswith(":pay_method_custom")