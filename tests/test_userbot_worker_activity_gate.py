import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from userbot_worker.worker import _apply_activity_gate


class FakeClient:
    def __init__(self, *, own_messages=None, recent_messages=None):
        self._own_messages = list(own_messages or [])
        self._recent_messages = list(recent_messages or [])

    async def iter_messages(self, chat_id, **kwargs):
        if kwargs.get("from_user") == "me":
            for message in self._own_messages:
                yield message
            return

        for message in self._recent_messages:
            yield message


class ActivityGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_counts_only_messages_not_from_me(self):
        my_id = 101
        client = FakeClient(
            recent_messages=[
                SimpleNamespace(id=10, sender_id=my_id, out=False),
                SimpleNamespace(id=11, sender_id=202, out=True),
                SimpleNamespace(id=12, sender_id=303, out=False),
            ]
        )

        passed, last_self_message_id, count = await _apply_activity_gate(
            pool=object(),
            client=client,
            chat_id=777,
            known_last_self_message_id=9,
            my_id=my_id,
            threshold=2,
        )

        self.assertTrue(passed)
        self.assertEqual(last_self_message_id, 9)
        self.assertEqual(count, 2)

    async def test_returns_threshold_count_when_last_self_unknown(self):
        client = FakeClient(own_messages=[])

        passed, last_self_message_id, count = await _apply_activity_gate(
            pool=object(),
            client=client,
            chat_id=777,
            known_last_self_message_id=None,
            my_id=101,
            threshold=4,
        )

        self.assertTrue(passed)
        self.assertIsNone(last_self_message_id)
        self.assertEqual(count, 4)

    async def test_resolves_last_self_message_and_persists_it(self):
        client = FakeClient(own_messages=[SimpleNamespace(id=55, sender_id=101, out=True)], recent_messages=[])
        pool = object()

        with patch("userbot_worker.worker.set_userbot_target_last_self_message_id", AsyncMock()) as persist_mock:
            passed, last_self_message_id, count = await _apply_activity_gate(
                pool=pool,
                client=client,
                chat_id=777,
                known_last_self_message_id=None,
                my_id=101,
                threshold=1,
            )

        self.assertFalse(passed)
        self.assertEqual(last_self_message_id, 55)
        self.assertEqual(count, 0)
        persist_mock.assert_awaited_once_with(pool, chat_id=777, message_id=55)


if __name__ == "__main__":
    unittest.main()