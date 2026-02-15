import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")

from bot_controller import main_core


class FakePool:
    def __init__(self) -> None:
        self._next_id = 1
        self._active_by_dedupe: dict[str, int] = {}
        self.insert_attempts = 0

    async def fetchrow(self, query, *args):
        self.insert_attempts += 1
        dedupe_key = args[-1]
        if dedupe_key in self._active_by_dedupe:
            return None
        task_id = self._next_id
        self._next_id += 1
        self._active_by_dedupe[dedupe_key] = task_id
        return {"id": task_id}

    async def fetchval(self, query, dedupe_key):
        return self._active_by_dedupe.get(dedupe_key)


class CreateUserbotTaskTests(unittest.IsolatedAsyncioTestCase):
    async def test_double_enqueue_is_idempotent_for_same_logical_bucket(self):
        pool = FakePool()
        run_at = datetime(2026, 1, 1, 10, 0, 0, 123456, tzinfo=timezone.utc)

        with (
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core, "ensure_userbot_tasks_schema", AsyncMock(return_value=None)),
            patch.object(
                main_core,
                "get_userbot_tasks_columns",
                AsyncMock(return_value={"dedupe_key", "status", "storage_message_id", "attempts", "last_error"}),
            ),
        ):
            first_id = await main_core.create_userbot_task(777, [30, 10], None, run_at)
            second_id = await main_core.create_userbot_task(
                777,
                [10, 30],
                None,
                run_at + timedelta(microseconds=1),
            )

        self.assertEqual(first_id, second_id)
        self.assertEqual(pool.insert_attempts, 2)
        self.assertEqual(len(pool._active_by_dedupe), 1)

    async def test_enqueue_in_different_time_window_creates_new_active_task(self):
        pool = FakePool()
        run_at = datetime(2026, 1, 1, 10, 0, 59, 999999, tzinfo=timezone.utc)

        with (
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core, "ensure_userbot_tasks_schema", AsyncMock(return_value=None)),
            patch.object(
                main_core,
                "get_userbot_tasks_columns",
                AsyncMock(return_value={"dedupe_key", "status", "storage_message_id", "attempts", "last_error"}),
            ),
        ):
            first_id = await main_core.create_userbot_task(777, [10, 30], None, run_at)
            second_id = await main_core.create_userbot_task(777, [10, 30], None, run_at + timedelta(seconds=1))

        self.assertNotEqual(first_id, second_id)
        self.assertEqual(len(pool._active_by_dedupe), 2)


if __name__ == "__main__":
    unittest.main()