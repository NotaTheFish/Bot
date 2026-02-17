import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from accountant_bot.config import Settings
from accountant_bot.taboo import is_taboo, load_taboo, safe_send_document, safe_send_message


class AccountantBotTabooTests(unittest.IsolatedAsyncioTestCase):
    def _settings(self, taboo_chat_ids):
        return Settings(
            ACCOUNTANT_BOT_TOKEN="token",
            ACCOUNTANT_ADMIN_IDS=[1],
            DATABASE_URL="postgresql://localhost/test",
            REVIEWS_CHANNEL_ID=777,
            TG_API_ID=10001,
            TG_API_HASH="hash",
            ACCOUNTANT_TG_STRING_SESSION="session",
            TABOO_CHAT_IDS=taboo_chat_ids,
        )

    def test_load_taboo_and_is_taboo(self):
        loaded = load_taboo(self._settings([10, 20]))

        self.assertEqual(loaded, {10, 20})
        self.assertTrue(is_taboo(10))
        self.assertFalse(is_taboo(99))

    async def test_safe_send_message_blocks_taboo_chat(self):
        load_taboo(self._settings([42]))
        bot = SimpleNamespace(send_message=AsyncMock())

        with self.assertLogs("accountant_bot.taboo", level="WARNING") as logs:
            result = await safe_send_message(bot, 42, "hello")

        self.assertIsNone(result)
        bot.send_message.assert_not_awaited()
        self.assertTrue(any("Blocked send_message to taboo chat_id=42" in line for line in logs.output))

    async def test_safe_send_message_sends_for_allowed_chat(self):
        load_taboo(self._settings([42]))
        bot = SimpleNamespace(send_message=AsyncMock(return_value="sent"))

        result = await safe_send_message(bot, 7, "hello", parse_mode="HTML")

        self.assertEqual(result, "sent")
        bot.send_message.assert_awaited_once_with(chat_id=7, text="hello", parse_mode="HTML")

    async def test_safe_send_document_blocks_taboo_chat(self):
        load_taboo(self._settings([42]))
        bot = SimpleNamespace(send_document=AsyncMock())

        with self.assertLogs("accountant_bot.taboo", level="WARNING") as logs:
            result = await safe_send_document(bot, 42, "file")

        self.assertIsNone(result)
        bot.send_document.assert_not_awaited()
        self.assertTrue(any("Blocked send_document to taboo chat_id=42" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()