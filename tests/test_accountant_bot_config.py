import os
import unittest
from contextlib import contextmanager

from accountant_bot.config import load_settings


@contextmanager
def _temp_env(updates: dict[str, str | None]):
    old = {k: os.environ.get(k) for k in updates}
    try:
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class AccountantBotConfigTests(unittest.TestCase):
    def test_load_settings_parses_required_values(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123,456",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "TABOO_CHAT_IDS": "888,999",
                # Ensure old names do not interfere
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.ACCOUNTANT_ADMIN_IDS, [123, 456])
        self.assertEqual(settings.REVIEWS_CHANNEL_ID, 777)
        self.assertEqual(settings.TABOO_CHAT_IDS, [888, 999])
        self.assertEqual(settings.TG_API_ID, 10001)
        self.assertEqual(settings.TG_API_HASH, "hash")

    def test_taboo_defaults_to_reviews_channel_id(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "TABOO_CHAT_IDS": None,
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.TABOO_CHAT_IDS, [777])

    def test_invalid_csv_reports_variable_name(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123,bad",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            with self.assertRaisesRegex(ValueError, "ACCOUNTANT_ADMIN_IDS"):
                load_settings()

    def test_missing_required_variable_reports_name(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": None,
                "ACCOUNTANT_ADMIN_IDS": "123",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            with self.assertRaisesRegex(RuntimeError, "ACCOUNTANT_BOT_TOKEN"):
                load_settings()

    def test_load_settings_accepts_legacy_telegram_api_env_names(self):
        # New names are absent, old names provided -> should still load.
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": None,
                "TG_API_HASH": None,
                "TELEGRAM_API_ID": "20002",
                "TELEGRAM_API_HASH": "legacy_hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.TG_API_ID, 20002)
        self.assertEqual(settings.TG_API_HASH, "legacy_hash")

    def test_tg_api_env_names_take_priority_over_legacy(self):
        # If both provided, TG_* should win (by design of _get_required_env_any order)
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "new_hash",
                "TELEGRAM_API_ID": "20002",
                "TELEGRAM_API_HASH": "legacy_hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.TG_API_ID, 10001)
        self.assertEqual(settings.TG_API_HASH, "new_hash")

    def test_admin_timezones_parsing_and_fallback(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123,456",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "DEFAULT_TIMEZONE": "Europe/Berlin",
                "ADMIN_TIMEZONES": "123=Asia/Tokyo,456=Europe/Paris",
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.ADMIN_TIMEZONES, {123: "Asia/Tokyo", 456: "Europe/Paris"})
        self.assertEqual(settings.get_admin_timezone(123), "Asia/Tokyo")
        self.assertEqual(settings.get_admin_timezone(999), "Europe/Berlin")

    def test_admin_timezones_raises_on_invalid_timezone(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "ADMIN_TIMEZONES": "123=Bad/Timezone",
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            with self.assertRaisesRegex(ValueError, "ADMIN_TIMEZONES"):
                load_settings()

    def test_empty_admin_timezones_uses_default_timezone_then_utc(self):
        with _temp_env(
            {
                "ACCOUNTANT_BOT_TOKEN": "token",
                "ACCOUNTANT_ADMIN_IDS": "123",
                "DATABASE_URL": "postgresql://localhost/test",
                "REVIEWS_CHANNEL_ID": "777",
                "TG_API_ID": "10001",
                "TG_API_HASH": "hash",
                "ACCOUNTANT_TG_STRING_SESSION": "session",
                "DEFAULT_TIMEZONE": "UTC",
                "ADMIN_TIMEZONES": "",
                "TELEGRAM_API_ID": None,
                "TELEGRAM_API_HASH": None,
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.ADMIN_TIMEZONES, {})
        self.assertEqual(settings.get_admin_timezone(123), "UTC")

        settings_with_empty_default = settings.__class__(
            ACCOUNTANT_BOT_TOKEN=settings.ACCOUNTANT_BOT_TOKEN,
            ACCOUNTANT_ADMIN_IDS=settings.ACCOUNTANT_ADMIN_IDS,
            DATABASE_URL=settings.DATABASE_URL,
            REVIEWS_CHANNEL_ID=settings.REVIEWS_CHANNEL_ID,
            TG_API_ID=settings.TG_API_ID,
            TG_API_HASH=settings.TG_API_HASH,
            ACCOUNTANT_TG_STRING_SESSION=settings.ACCOUNTANT_TG_STRING_SESSION,
            DEFAULT_TIMEZONE="",
        )
        self.assertEqual(settings_with_empty_default.get_admin_timezone(999), "UTC")


if __name__ == "__main__":
    unittest.main()
