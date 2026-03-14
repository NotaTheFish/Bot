import unittest
from datetime import datetime, timezone

from contest_webapp import contest_api


class ContestResultsTieBreakTests(unittest.TestCase):
    def test_apply_results_tie_break_adds_bonus_by_penalty_then_time_then_id(self):
        items = [
            {
                "id": 12,
                "net_votes": 5,
                "penalty_votes": 1,
                "submitted_at": datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
            },
            {
                "id": 13,
                "net_votes": 5,
                "penalty_votes": 0,
                "submitted_at": datetime(2026, 3, 10, 13, 0, tzinfo=timezone.utc),
            },
            {
                "id": 10,
                "net_votes": 5,
                "penalty_votes": 0,
                "submitted_at": datetime(2026, 3, 10, 13, 0, tzinfo=timezone.utc),
            },
            {
                "id": 14,
                "net_votes": 4,
                "penalty_votes": 0,
                "submitted_at": datetime(2026, 3, 10, 11, 0, tzinfo=timezone.utc),
            },
        ]

        contest_api._apply_results_tie_break(items)

        self.assertEqual(items[0]["id"], 10)
        self.assertEqual(items[0]["tie_break_bonus"], 1)
        self.assertEqual(items[0]["effective_net_votes"], 6)
        self.assertEqual(items[0]["place"], 1)
        self.assertEqual(items[1]["id"], 13)
        self.assertEqual(items[1]["place"], 2)
        self.assertEqual(items[2]["id"], 12)
        self.assertEqual(items[3]["id"], 14)

    def test_apply_results_tie_break_keeps_zero_bonus_without_tie(self):
        items = [{"id": 1, "net_votes": 3, "penalty_votes": 0, "submitted_at": None}]
        contest_api._apply_results_tie_break(items)
        self.assertEqual(items[0]["tie_break_bonus"], 0)
        self.assertEqual(items[0]["effective_net_votes"], 3)
        self.assertEqual(items[0]["place"], 1)


if __name__ == "__main__":
    unittest.main()