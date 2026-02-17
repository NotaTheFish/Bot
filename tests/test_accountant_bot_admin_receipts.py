from datetime import datetime, timezone
from decimal import Decimal

from accountant_bot.admin_bot import _receipt_details_text, _receipt_list_keyboard


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