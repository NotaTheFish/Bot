import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "contest_webapp"))
import contest_api


class ContestWebappEntryImageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        contest_api._entry_image_cache.clear()
        contest_api._telegram_file_path_cache.clear()

    async def test_entry_image_approved_available_without_telegram_header(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 10,
                    "status": "approved",
                    "is_deleted": False,
                    "telegram_file_id": "photo_file_1",
                    "storage_chat_id": 123,
                    "storage_message_id": 456,
                    "storage_message_ids": [456],
                }
            )
        )

        with (
            patch.object(contest_api, "get_pool", return_value=pool),
            patch.object(contest_api, "_download_image_by_file_id", AsyncMock(return_value=(b"img", "image/jpeg"))),
        ):
            response = await contest_api.entry_image(10)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body, b"img")
        self.assertEqual(response.media_type, "image/jpeg")

    async def test_entry_image_non_approved_or_deleted_returns_404(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 10,
                    "status": "pending",
                    "is_deleted": False,
                    "telegram_file_id": "photo_file_1",
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
                    "telegram_file_id": "photo_file_1",
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
                    "telegram_file_id": None,
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

    async def test_entry_image_telegram_temporary_error_returns_502(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 11,
                    "status": "approved",
                    "is_deleted": False,
                    "telegram_file_id": "photo_file_2",
                    "storage_chat_id": 123,
                    "storage_message_id": 456,
                    "storage_message_ids": [456],
                }
            )
        )

        with (
            patch.object(contest_api, "get_pool", return_value=pool),
            patch.object(contest_api, "_download_image_by_file_id", AsyncMock(side_effect=RuntimeError("telegram timeout"))),
        ):
            response = await contest_api.entry_image(11)

        self.assertEqual(response.status_code, 502)
        payload = json.loads(response.body)
        self.assertEqual(payload["error_code"], "image_unavailable")

    async def test_entry_image_repeated_request_uses_cache(self):
        pool = SimpleNamespace(
            fetchrow=AsyncMock(
                return_value={
                    "id": 12,
                    "status": "approved",
                    "is_deleted": False,
                    "telegram_file_id": "photo_file_cache",
                    "storage_chat_id": 123,
                    "storage_message_id": 456,
                    "storage_message_ids": [456],
                }
            )
        )
        download = AsyncMock(return_value=(b"cached-bytes", "image/jpeg"))

        with (
            patch.object(contest_api, "get_pool", return_value=pool),
            patch.object(contest_api, "_download_image_by_file_id", download),
        ):
            first = await contest_api.entry_image(12)
            second = await contest_api.entry_image(12)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(download.await_count, 1)

    async def test_content_type_for_photo_and_image_document(self):
        self.assertEqual(contest_api._content_type_for_file("photos/file_1.jpg", "application/octet-stream"), "image/jpeg")
        self.assertEqual(contest_api._content_type_for_file("docs/file_1.png", None), "image/png")


if __name__ == "__main__":
    unittest.main()