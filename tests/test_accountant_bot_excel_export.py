import unittest
from io import BytesIO

from openpyxl import load_workbook

from accountant_bot.excel_export import build_transactions_report


class AccountantBotExcelExportTests(unittest.TestCase):
    def test_build_transactions_report_includes_receipts_items_and_summary_sheets(self):
        transactions = [
            {
                "receipt_id": 1,
                "created_at": "2026-01-01 10:00:00",
                "admin": "@admin (id: 100)",
                "currency": "RUB",
                "pay_method": "cash",
                "total_sum": "200",
                "note": "morning",
                "receipt_file_id": "abc",
                "status": "created",
                "items": [
                    {
                        "category": "VID",
                        "item_name": "Coffee",
                        "qty": "2",
                        "unit_price": "100",
                        "unit_basis": "unit",
                        "line_total": "200",
                        "note": "",
                    }
                ],
            },
            {
                "receipt_id": 2,
                "created_at": "2026-01-01 11:00:00",
                "admin": "User Name (id: 101)",
                "currency": "USD",
                "pay_method": "card",
                "total_sum": "5",
                "note": "",
                "receipt_file_id": None,
                "status": "refunded",
                "items": [
                    {
                        "category": "MUSHROOMS",
                        "item_name": "Tea",
                        "qty": "1000",
                        "unit_price": "5",
                        "unit_basis": "per_1000",
                        "line_total": "5",
                        "note": "promo",
                    }
                ],
            },
        ]

        report = build_transactions_report(transactions)

        wb = load_workbook(BytesIO(report))
        self.assertEqual(wb.sheetnames, ["Receipts", "Items", "Summary"])

        ws_receipts = wb["Receipts"]
        self.assertEqual(ws_receipts["A1"].value, "receipt_id")
        self.assertEqual(ws_receipts["C2"].value, "@admin (id: 100)")
        self.assertEqual(ws_receipts["H2"].value, "yes")
        self.assertEqual(ws_receipts["I3"].value, "REFUND")

        ws_items = wb["Items"]
        self.assertEqual(ws_items["F2"].value, "Видео")
        self.assertEqual(ws_items["J3"].value, "за 1000")

        ws_summary = wb["Summary"]
        summary_values = [tuple(row) for row in ws_summary.iter_rows(min_row=2, max_row=ws_summary.max_row, values_only=True)]

        self.assertIn(("counts", "receipts_total", 2), summary_values)
        self.assertIn(("counts", "receipts_refund", 1), summary_values)
        self.assertIn(("currency_ok", "RUB", 200), summary_values)
        self.assertIn(("currency_refund", "USD", 5), summary_values)


if __name__ == "__main__":
    unittest.main()