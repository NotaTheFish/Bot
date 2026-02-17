import unittest

from accountant_bot.excel_export import build_transactions_report


class AccountantBotExcelExportTests(unittest.TestCase):
    def test_build_transactions_report_includes_transactions_and_summary_sheets(self):
        transactions = [
            {
                "created_at": "2026-01-01 10:00:00",
                "admin_id": 100,
                "item": "Coffee",
                "qty": "2",
                "unit_price": "100",
                "currency": "RUB",
                "pay_method": "cash",
                "total": "200",
                "note": "morning",
            },
            {
                "created_at": "2026-01-01 11:00:00",
                "admin_id": 101,
                "item": "Tea",
                "qty": "1",
                "unit_price": "5",
                "currency": "USD",
                "pay_method": "card",
                "total": "5",
                "note": "",
            },
        ]

        report = build_transactions_report(transactions)
        xml = report.decode("utf-8")

        self.assertIn('Worksheet ss:Name="Transactions"', xml)
        self.assertIn('Worksheet ss:Name="Summary"', xml)
        self.assertIn("transactions_count", xml)
        self.assertIn("total_RUB", xml)
        self.assertIn("200", xml)
        self.assertIn("total_USD", xml)
        self.assertIn(">5<", xml)


if __name__ == "__main__":
    unittest.main()