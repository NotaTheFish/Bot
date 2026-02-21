import unittest
from unittest.mock import AsyncMock, patch

from userbot_worker.db import try_mark_autoreply


class TryMarkAutoreplySqlTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_explicit_should_reply_case(self):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={"should_reply": True})

        with (
            patch("userbot_worker.db._acquire_conn", AsyncMock(return_value=conn)),
            patch("userbot_worker.db._release_conn", AsyncMock()),
        ):
            result = await try_mark_autoreply(object(), user_id=42, cooldown_seconds=60)

        self.assertTrue(result)
        sql = conn.fetchrow.await_args.args[0]
        self.assertIn("CASE", sql)
        self.assertIn("AS should_reply", sql)
        self.assertNotIn("last_replied_at = NOW() AS should_reply", sql)


if __name__ == "__main__":
    unittest.main()