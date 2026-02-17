import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from accountant_bot.config import Settings
from accountant_bot.reviews import ReviewsService


class AccountantBotReviewsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = Settings(
            ACCOUNTANT_BOT_TOKEN="token",
            ACCOUNTANT_ADMIN_IDS=[1],
            DATABASE_URL="postgresql://localhost/test",
            REVIEWS_CHANNEL_ID=777,
            TG_API_ID=123,
            TG_API_HASH="hash",
            ACCOUNTANT_TG_STRING_SESSION="session",
            ABOUT_TEMPLATE="Отзывов: {count}. Обновлено: {date}",
            ABOUT_DATE_FORMAT="%d.%m.%Y",
        )
        self.pool = MagicMock()
        self.bot = AsyncMock()
        self.service = ReviewsService(self.pool, self.bot, self.settings)

    def test_build_review_key(self):
        self.assertEqual(ReviewsService.build_review_key(10, None), "10:single")
        self.assertEqual(ReviewsService.build_review_key(10, "grp1"), "10:grp1")

    def test_replace_or_append_about_replaces_existing_line(self):
        description = "Описание\nОтзывов: 2. Обновлено: 01.01.2025\nЕще"
        replaced = ReviewsService.replace_or_append_about(description, "Отзывов: 3. Обновлено: 02.01.2025")
        self.assertEqual(replaced, "Описание\nОтзывов: 3. Обновлено: 02.01.2025\nЕще")

    def test_replace_or_append_about_appends_when_missing(self):
        description = "Описание"
        replaced = ReviewsService.replace_or_append_about(description, "Отзывов: 1. Обновлено: 02.01.2025")
        self.assertEqual(replaced, "Описание\nОтзывов: 1. Обновлено: 02.01.2025")

    async def test_debounced_about_update_updates_description(self):
        fast_settings = self.settings.__class__(**{**self.settings.__dict__, "ABOUT_UPDATE_DEBOUNCE_SECONDS": 0.01})
        service = ReviewsService(self.pool, self.bot, fast_settings)

        service.count_active = AsyncMock(return_value=5)
        self.bot.get_chat = AsyncMock(return_value=SimpleNamespace(description="Описание"))
        self.bot.set_chat_description = AsyncMock()

        service.schedule_about_update()
        await asyncio.sleep(0.05)

        self.bot.get_chat.assert_awaited_once_with(777)
        self.bot.set_chat_description.assert_awaited_once()

    async def test_schedule_about_update_cancels_previous_task(self):
        fast_settings = self.settings.__class__(**{**self.settings.__dict__, "ABOUT_UPDATE_DEBOUNCE_SECONDS": 0.2})
        service = ReviewsService(self.pool, self.bot, fast_settings)

        service.schedule_about_update()
        first_task = service._about_update_task
        self.assertIsNotNone(first_task)

        service.schedule_about_update()
        await asyncio.sleep(0)
        self.assertTrue(first_task.cancelled() or first_task.done())

        # cleanup
        task = service._about_update_task
        if task is not None:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    unittest.main()