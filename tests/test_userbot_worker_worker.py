import unittest
from datetime import datetime, timedelta, timezone

from userbot_worker.config import Settings
from userbot_worker.db import UserbotTargetState
from userbot_worker.worker import _skip_reason_by_history


def _build_settings(*, cooldown_minutes: int, anti_dup_minutes: int) -> Settings:
    return Settings(
        database_url="postgresql://localhost/test",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telethon_session="userbot.session",
        min_seconds_between_chats=1,
        max_seconds_between_chats=1,
        worker_poll_seconds=1,
        admin_notify_bot_token="",
        admin_id=0,
        storage_chat_id=1,
        blacklist_chat_ids=set(),
        max_task_attempts=1,
        target_chat_ids=[],
        targets_sync_seconds=30,
        targets_stale_grace_days=7,
        targets_disable_on_entity_error=True,
        auto_targets_mode="groups_only",
        cooldown_minutes=cooldown_minutes,
        activity_gate_min_messages=0,
        anti_dup_minutes=anti_dup_minutes,
        controller_bot_username="controller_bot",
        authkey_duplicated_cooldown_seconds=120,
    )


class UserbotWorkerHistorySkipTests(unittest.TestCase):
    def test_returns_none_without_state(self):
        reason = _skip_reason_by_history(
            settings=_build_settings(cooldown_minutes=10, anti_dup_minutes=10),
            state=None,
            fingerprint="abc",
            now_utc=datetime.now(timezone.utc),
        )
        self.assertIsNone(reason)

    def test_skips_by_cooldown_when_enabled(self):
        now_utc = datetime.now(timezone.utc)
        state = UserbotTargetState(
            chat_id=1,
            enabled=True,
            last_success_post_at=now_utc - timedelta(minutes=5),
            last_self_message_id=None,
            last_post_fingerprint="zzz",
            last_post_at=now_utc - timedelta(minutes=30),
        )

        reason = _skip_reason_by_history(
            settings=_build_settings(cooldown_minutes=10, anti_dup_minutes=10),
            state=state,
            fingerprint="abc",
            now_utc=now_utc,
        )

        self.assertEqual(reason, "cooldown")

    def test_skips_by_anti_dup_when_enabled(self):
        now_utc = datetime.now(timezone.utc)
        state = UserbotTargetState(
            chat_id=1,
            enabled=True,
            last_success_post_at=now_utc - timedelta(minutes=30),
            last_self_message_id=None,
            last_post_fingerprint="abc",
            last_post_at=now_utc - timedelta(minutes=5),
        )

        reason = _skip_reason_by_history(
            settings=_build_settings(cooldown_minutes=10, anti_dup_minutes=10),
            state=state,
            fingerprint="abc",
            now_utc=now_utc,
        )

        self.assertEqual(reason, "anti_dup")

    def test_zero_values_disable_history_checks(self):
        now_utc = datetime.now(timezone.utc)
        state = UserbotTargetState(
            chat_id=1,
            enabled=True,
            last_success_post_at=now_utc - timedelta(minutes=1),
            last_self_message_id=None,
            last_post_fingerprint="abc",
            last_post_at=now_utc - timedelta(minutes=1),
        )

        reason = _skip_reason_by_history(
            settings=_build_settings(cooldown_minutes=0, anti_dup_minutes=0),
            state=state,
            fingerprint="abc",
            now_utc=now_utc,
        )

        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()