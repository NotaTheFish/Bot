from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from .category_labels import CATEGORY_LABELS

RECEIPTS_HEADERS = [
    "receipt_id",
    "created_at",
    "admin",
    "currency",
    "pay_method",
    "total_sum",
    "note",
    "has_receipt",
    "status",
]

ITEMS_HEADERS = [
    "receipt_id",
    "created_at",
    "admin",
    "status",
    "currency",
    "category",
    "item_name",
    "qty",
    "unit_price",
    "unit_basis",
    "line_total",
    "note",
]

UNIT_BASIS_LABELS = {
    "unit": "шт",
    "per_1000": "за 1000",
}

THIN_BORDER = Border(
    left=Side(style="thin", color="FFE5E7EB"),
    right=Side(style="thin", color="FFE5E7EB"),
    top=Side(style="thin", color="FFE5E7EB"),
    bottom=Side(style="thin", color="FFE5E7EB"),
)
HEADER_FILL = PatternFill("solid", fgColor="FFE9F2FF")
SECTION_FILL = PatternFill("solid", fgColor="FFF6FAFF")
SUMMARY_TITLE_FILL = PatternFill("solid", fgColor="FFF0F7EC")


def build_transactions_report(transactions: list[dict[str, Any] | Any]) -> bytes:
    receipts: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []

    for row in transactions:
        record = dict(row)
        receipt = {
            "receipt_id": record.get("receipt_id") or record.get("id"),
            "created_at": record.get("created_at"),
            "admin": str(record.get("admin") or record.get("admin_id") or ""),
            "currency": str(record.get("currency") or ""),
            "pay_method": str(record.get("pay_method") or ""),
            "total_sum": _to_decimal(record.get("total_sum") if record.get("total_sum") is not None else record.get("total")),
            "note": str(record.get("note") or ""),
            "has_receipt": bool(record.get("receipt_file_id")),
            "status": _normalize_status(record.get("status")),
        }
        receipts.append(receipt)

        for item in record.get("items") or []:
            item_record = dict(item)
            items.append(
                {
                    "receipt_id": receipt["receipt_id"],
                    "created_at": receipt["created_at"],
                    "admin": receipt["admin"],
                    "status": receipt["status"],
                    "currency": receipt["currency"],
                    "category": CATEGORY_LABELS.get(str(item_record.get("category") or "").upper(), str(item_record.get("category") or "")),
                    "item_name": str(item_record.get("item_name") or ""),
                    "qty": _to_decimal(item_record.get("qty")),
                    "unit_price": _to_decimal(item_record.get("unit_price")),
                    "unit_basis": UNIT_BASIS_LABELS.get(str(item_record.get("unit_basis") or ""), str(item_record.get("unit_basis") or "")),
                    "line_total": _to_decimal(item_record.get("line_total")),
                    "note": str(item_record.get("note") or ""),
                }
            )

    wb = Workbook()
    ws_receipts = wb.active
    ws_receipts.title = "Receipts"
    _build_receipts_sheet(ws_receipts, receipts)

    ws_items = wb.create_sheet("Items")
    _build_items_sheet(ws_items, items)

    ws_summary = wb.create_sheet("Summary")
    _build_summary_sheet(ws_summary, receipts, items)

    output = BytesIO()
    wb.save(output)
    return output.getvalue()


def _build_receipts_sheet(ws: Any, receipts: list[dict[str, Any]]) -> None:
    ws.append(RECEIPTS_HEADERS)
    for receipt in receipts:
        ws.append(
            [
                receipt["receipt_id"],
                _fmt_dt(receipt["created_at"]),
                receipt["admin"],
                receipt["currency"],
                receipt["pay_method"],
                receipt["total_sum"],
                receipt["note"],
                "yes" if receipt["has_receipt"] else "no",
                receipt["status"],
            ]
        )
    _apply_sheet_style(ws, money_columns={6})


def _build_items_sheet(ws: Any, items: list[dict[str, Any]]) -> None:
    ws.append(ITEMS_HEADERS)
    for item in items:
        ws.append(
            [
                item["receipt_id"],
                _fmt_dt(item["created_at"]),
                item["admin"],
                item["status"],
                item["currency"],
                item["category"],
                item["item_name"],
                item["qty"],
                item["unit_price"],
                item["unit_basis"],
                item["line_total"],
                item["note"],
            ]
        )
    _apply_sheet_style(ws, money_columns={9, 11}, qty_columns={8})


def _build_summary_sheet(ws: Any, receipts: list[dict[str, Any]], items: list[dict[str, Any]]) -> None:
    ws.append(["section", "metric", "value"])

    ok_receipts = [r for r in receipts if r["status"] == "OK"]
    canceled_receipts = [r for r in receipts if r["status"] == "CANCELED"]
    refund_receipts = [r for r in receipts if r["status"] == "REFUND"]

    ws.append(["counts", "receipts_total", len(receipts)])
    ws.append(["counts", "receipts_ok", len(ok_receipts)])
    ws.append(["counts", "receipts_canceled", len(canceled_receipts)])
    ws.append(["counts", "receipts_refund", len(refund_receipts)])
    ws.append(["counts", "items_total", len(items)])

    total_by_currency_ok: dict[str, Decimal] = defaultdict(Decimal)
    total_by_currency_canceled: dict[str, Decimal] = defaultdict(Decimal)
    total_by_currency_refund: dict[str, Decimal] = defaultdict(Decimal)

    for receipt in receipts:
        currency = receipt["currency"] or "-"
        if receipt["status"] == "CANCELED":
            total_by_currency_canceled[currency] += receipt["total_sum"]
        elif receipt["status"] == "REFUND":
            total_by_currency_refund[currency] += receipt["total_sum"]
        else:
            total_by_currency_ok[currency] += receipt["total_sum"]

    for currency, total in sorted(total_by_currency_ok.items()):
        ws.append(["currency_ok", currency, total])
    for currency, total in sorted(total_by_currency_canceled.items()):
        ws.append(["currency_canceled", currency, total])
    for currency, total in sorted(total_by_currency_refund.items()):
        ws.append(["currency_refund", currency, total])

    category_item_counts: dict[str, int] = defaultdict(int)
    for item in items:
        category_item_counts[item["category"] or "-"] += 1
    for category, count in sorted(category_item_counts.items()):
        ws.append(["category_items", category, count])

    _apply_sheet_style(ws, money_columns={3})
    _paint_summary_sections(ws)


def _apply_sheet_style(ws: Any, *, money_columns: set[int], qty_columns: set[int] | None = None) -> None:
    qty_columns = qty_columns or set()
    ws.freeze_panes = "A2"

    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN_BORDER

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.border = THIN_BORDER
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if cell.row % 2 == 0:
                cell.fill = SECTION_FILL

    for col_idx in money_columns:
        for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx, min_row=2, max_row=ws.max_row):
            for each in cell:
                each.number_format = '#,##0.00'

    for col_idx in qty_columns:
        for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx, min_row=2, max_row=ws.max_row):
            for each in cell:
                each.number_format = '#,##0.###'

    _autowidth(ws)


def _paint_summary_sections(ws: Any) -> None:
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=3):
        section_name = str(row[0].value or "")
        if section_name.startswith("counts"):
            fill = PatternFill("solid", fgColor="FFFFF7E8")
        elif section_name.startswith("currency"):
            fill = PatternFill("solid", fgColor="FFE9F7EF")
        elif section_name.startswith("category"):
            fill = PatternFill("solid", fgColor="FFF3ECFA")
        else:
            fill = SUMMARY_TITLE_FILL
        for cell in row:
            cell.fill = fill


def _autowidth(ws: Any) -> None:
    for column_cells in ws.columns:
        max_len = 0
        column_letter = column_cells[0].column_letter
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            if len(value) > max_len:
                max_len = len(value)
        ws.column_dimensions[column_letter].width = min(max(12, max_len + 2), 60)


def _normalize_status(value: Any) -> str:
    status_raw = str(value or "").strip().lower()
    if status_raw in {"refunded", "refund"}:
        return "REFUND"
    if status_raw in {"canceled", "cancelled", "cancel"}:
        return "CANCELED"
    return "OK"


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