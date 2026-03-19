import unittest
from datetime import datetime, timezone

from contest_webapp import contest_api


class ContestResultsTieBreakTests(unittest.TestCase):
    def test_apply_results_tie_break_adds_bonus_by_penalty_then_time_then_id_within_prize_zone(self):
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

    def test_apply_results_tie_break_does_not_change_ties_outside_top_five(self):
        items = [
            {"id": 1, "net_votes": 10, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 0, tzinfo=timezone.utc)},
            {"id": 2, "net_votes": 9, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 1, tzinfo=timezone.utc)},
            {"id": 3, "net_votes": 8, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 2, tzinfo=timezone.utc)},
            {"id": 4, "net_votes": 7, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 3, tzinfo=timezone.utc)},
            {"id": 5, "net_votes": 6, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 4, tzinfo=timezone.utc)},
            {"id": 6, "net_votes": 5, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 5, tzinfo=timezone.utc)},
            {"id": 7, "net_votes": 5, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 6, tzinfo=timezone.utc)},
        ]

        contest_api._apply_results_tie_break(items)

        self.assertEqual(items[5]["id"], 6)
        self.assertEqual(items[6]["id"], 7)
        self.assertEqual(items[5]["tie_break_bonus"], 0)
        self.assertEqual(items[6]["tie_break_bonus"], 0)
        self.assertEqual(items[5]["effective_net_votes"], 5)
        self.assertEqual(items[6]["effective_net_votes"], 5)

    def test_apply_results_tie_break_breaks_tie_at_prize_boundary_inside_top_five(self):
        items = [
            {"id": 1, "net_votes": 9, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 0, tzinfo=timezone.utc)},
            {"id": 2, "net_votes": 8, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 1, tzinfo=timezone.utc)},
            {"id": 4, "net_votes": 7, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 3, tzinfo=timezone.utc)},
            {"id": 3, "net_votes": 7, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 2, tzinfo=timezone.utc)},
            {"id": 5, "net_votes": 6, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 4, tzinfo=timezone.utc)},
        ]

        contest_api._apply_results_tie_break(items)

        self.assertEqual([item["id"] for item in items[:4]], [1, 2, 3, 4])
        self.assertEqual(items[2]["tie_break_bonus"], 1)
        self.assertEqual(items[2]["effective_net_votes"], 8)
        self.assertEqual(items[3]["tie_break_bonus"], 0)
        self.assertEqual(items[3]["effective_net_votes"], 7)

    def test_apply_results_tie_break_keeps_zero_bonus_without_tie(self):
        items = [{"id": 1, "net_votes": 3, "penalty_votes": 0, "submitted_at": None}]
        contest_api._apply_results_tie_break(items)
        self.assertEqual(items[0]["tie_break_bonus"], 0)
        self.assertEqual(items[0]["effective_net_votes"], 3)
        self.assertEqual(items[0]["place"], 1)

    def test_apply_results_tie_break_regression_two_prize_zone_entries_with_same_net_pick_single_winner(self):
        items = [
            {"id": 21, "net_votes": 7, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 0, tzinfo=timezone.utc)},
            {"id": 22, "net_votes": 7, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 5, tzinfo=timezone.utc)},
            {"id": 23, "net_votes": 6, "penalty_votes": 0, "submitted_at": datetime(2026, 3, 10, 8, 10, tzinfo=timezone.utc)},
        ]

        contest_api._apply_results_tie_break(items)

        self.assertEqual([item["id"] for item in items], [21, 22, 23])
        self.assertEqual([item["effective_net_votes"] for item in items], [8, 7, 6])
        self.assertEqual([item["place"] for item in items], [1, 2, 3])
        self.assertEqual(items[0]["tie_break_bonus"], 1)
        self.assertEqual(items[1]["tie_break_bonus"], 0)


if __name__ == "__main__":
    unittest.main()