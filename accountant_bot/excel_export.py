from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from io import BytesIO
from datetime import datetime
from typing import Any
from xml.sax.saxutils import escape


def build_transactions_report(transactions: list[dict[str, Any] | Any]) -> bytes:
    tx_headers = [
        "created_at",
        "admin_id",
        "item",
        "qty",
        "unit_price",
        "currency",
        "pay_method",
        "total",
        "note",
    ]

    tx_rows: list[str] = [_xml_row(tx_headers)]
    tx_count = 0
    total_by_currency: dict[str, Decimal] = defaultdict(Decimal)

    for row in transactions:
        record = dict(row)
        tx_count += 1
        currency = str(record.get("currency") or "")
        total_by_currency[currency] += _to_decimal(record.get("total"))

        tx_rows.append(
            _xml_row(
                [
                    _fmt_dt(record.get("created_at")),
                    str(record.get("admin_id", "")),
                    str(record.get("item") or ""),
                    str(record.get("qty") or ""),
                    str(record.get("unit_price") or ""),
                    currency,
                    str(record.get("pay_method") or ""),
                    str(record.get("total") or ""),
                    str(record.get("note") or ""),
                ]
            )
        )

    summary_rows = [_xml_row(["metric", "value"]), _xml_row(["transactions_count", str(tx_count)])]
    for currency, total in sorted(total_by_currency.items()):
        key = f"total_{currency}" if currency else "total"
        summary_rows.append(_xml_row([key, str(total)]))

    xml = (
        '<?xml version="1.0"?>\n'
        '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"\n'
        ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">\n'
        f'<Worksheet ss:Name="Transactions"><Table>{"".join(tx_rows)}</Table></Worksheet>\n'
        f'<Worksheet ss:Name="Summary"><Table>{"".join(summary_rows)}</Table></Worksheet>\n'
        "</Workbook>"
    )

    output = BytesIO()
    output.write(xml.encode("utf-8"))
    return output.getvalue()


def _xml_row(values: list[str]) -> str:
    cells = "".join(f"<Cell><Data ss:Type=\"String\">{escape(value)}</Data></Cell>" for value in values)
    return f"<Row>{cells}</Row>"


def _fmt_dt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")