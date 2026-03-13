import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "contest_webapp"))
import contest_api


class ContestWebappEntryImageTests(unittest.IsolatedAsyncioTestCase):
    async def test_entry_image_approved_available_without_telegram_header(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 10,
                    "status": "approved",
                    "is_deleted": False,
                    "storage_chat_id": 123,
                    "storage_message_id": 456,
                    "storage_message_ids": [456],
                }
            )
        )

        with (
            patch.object(contest_api, "get_pool", return_value=pool),
            patch.object(contest_api, "_download_entry_image", AsyncMock(return_value=(b"img", "image/jpeg"))),
        ):
            response = await contest_api.entry_image(10)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body, b"img")

    async def test_entry_image_non_approved_or_deleted_returns_404(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 10,
                    "status": "pending",
                    "is_deleted": False,
                    "storage_chat_id": 123,
                    "storage_message_id": 456,
                    "storage_message_ids": [456],
                }
            )
        )

        with patch.object(contest_api, "get_pool", return_value=pool):
            response = await contest_api.entry_image(10)

        self.assertEqual(response.status_code, 404)
        payload = json.loads(response.body)
        self.assertEqual(payload["error_code"], "entry_not_available")

        pool_deleted = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 10,
                    "status": "approved",
                    "is_deleted": True,
                    "storage_chat_id": 123,
                    "storage_message_id": 456,
                    "storage_message_ids": [456],
                }
            )
        )

        with patch.object(contest_api, "get_pool", return_value=pool_deleted):
            deleted_response = await contest_api.entry_image(10)

        self.assertEqual(deleted_response.status_code, 404)
        deleted_payload = json.loads(deleted_response.body)
        self.assertEqual(deleted_payload["error_code"], "entry_not_available")

    async def test_entry_image_missing_storage_metadata_returns_404(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 10,
                    "status": "approved",
                    "is_deleted": False,
                    "storage_chat_id": None,
                    "storage_message_id": None,
                    "storage_message_ids": [],
                }
            )
        )

        with patch.object(contest_api, "get_pool", return_value=pool):
            response = await contest_api.entry_image(10)

        self.assertEqual(response.status_code, 404)
        payload = json.loads(response.body)
        self.assertEqual(payload["error_code"], "image_not_found")


if __name__ == "__main__":
    unittest.main()