import os
import unittest
from contextlib import contextmanager

from userbot_worker.config import load_settings


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


class UserbotWorkerConfigTests(unittest.TestCase):
    def test_new_guard_env_defaults_are_applied(self):
        with _temp_env(
            {
                "DATABASE_URL": "postgresql://localhost/test",
                "TELEGRAM_API_ID": "123",
                "TELEGRAM_API_HASH": "hash",
                "COOLDOWN_MINUTES": None,
                "ACTIVITY_GATE_MIN_MESSAGES": None,
                "ANTI_DUP_MINUTES": None,
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.cooldown_minutes, 10)
        self.assertEqual(settings.activity_gate_min_messages, 5)
        self.assertEqual(settings.anti_dup_minutes, 10)

    def test_new_guard_env_values_are_clamped_to_non_negative(self):
        with _temp_env(
            {
                "DATABASE_URL": "postgresql://localhost/test",
                "TELEGRAM_API_ID": "123",
                "TELEGRAM_API_HASH": "hash",
                "COOLDOWN_MINUTES": "-7",
                "ACTIVITY_GATE_MIN_MESSAGES": "-2",
                "ANTI_DUP_MINUTES": "-10",
            }
        ):
            settings = load_settings()

        self.assertEqual(settings.cooldown_minutes, 0)
        self.assertEqual(settings.activity_gate_min_messages, 0)
        self.assertEqual(settings.anti_dup_minutes, 0)


if __name__ == "__main__":
    unittest.main()