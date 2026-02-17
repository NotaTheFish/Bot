from __future__ import annotations

from datetime import datetime
from typing import Any
from xml.sax.saxutils import escape


def build_transactions_report(rows: list[dict[str, Any] | Any], *, period: str) -> tuple[bytes, str]:
    headers = [
        "ID",
        "Дата",
        "Админ",
        "Item",
        "Qty",
        "Unit price",
        "Total",
        "Currency",
        "Pay method",
        "Note",
        "Receipt file_id",
        "Amount kopecks",
    ]

    xml_rows: list[str] = [_xml_row(headers)]
    for row in rows:
        record = dict(row)
        xml_rows.append(
            _xml_row(
                [
                    str(record.get("id", "")),
                    _fmt_dt(record.get("created_at")),
                    str(record.get("admin_id", "")),
                    str(record.get("item") or ""),
                    str(record.get("qty") or ""),
                    str(record.get("unit_price") or ""),
                    str(record.get("total") or ""),
                    str(record.get("currency") or ""),
                    str(record.get("pay_method") or ""),
                    str(record.get("note") or ""),
                    str(record.get("receipt_file_id") or ""),
                    str(record.get("amount_kopecks") or ""),
                ]
            )
        )

    xml = (
        '<?xml version="1.0"?>\n'
        '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"\n'
        ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">\n'
        f'<Worksheet ss:Name="Transactions"><Table>{"".join(xml_rows)}</Table></Worksheet>\n'
        "</Workbook>"
    )

    filename = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
    return xml.encode("utf-8"), filename


def _xml_row(values: list[str]) -> str:
    cells = "".join(f"<Cell><Data ss:Type=\"String\">{escape(value)}</Data></Cell>" for value in values)
    return f"<Row>{cells}</Row>"


def _fmt_dt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)