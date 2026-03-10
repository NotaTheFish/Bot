import hashlib
import hmac
import json
import time
import unittest
from urllib.parse import urlencode
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot_controller import contest_api
from bot_controller.contest_api import ContestAPI, VoteError, parse_and_verify_init_data


def build_init_data(bot_token: str, user_id: int = 42, auth_date: int | None = None) -> str:
    auth_date = auth_date or int(time.time())
    payload = {
        "auth_date": str(auth_date),
        "query_id": "AAHdF6IQAAAAAN0XohDhrOrc",
        "user": json.dumps({"id": user_id, "first_name": "Test"}, separators=(",", ":")),
    }
    data_check = "\n".join(f"{k}={v}" for k, v in sorted(payload.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    payload["hash"] = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    return urlencode(payload)


class ContestApiAuthTests(unittest.TestCase):
    def test_parse_and_verify_init_data_success(self):
        raw = build_init_data("123:abc")
        result = parse_and_verify_init_data(raw, "123:abc")
        self.assertEqual(result["user_id"], 42)

    def test_parse_and_verify_init_data_rejects_invalid_hash(self):
        raw = build_init_data("123:abc") + "tamper"
        with self.assertRaises(ValueError):
            parse_and_verify_init_data(raw, "123:abc")

    def test_parse_and_verify_init_data_rejects_old_auth_date(self):
        raw = build_init_data("123:abc", auth_date=int(time.time()) - 5000)
        with self.assertRaises(ValueError):
            parse_and_verify_init_data(raw, "123:abc", max_age_seconds=60)


class _DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class ContestApiVotingTests(unittest.IsolatedAsyncioTestCase):
    def _build_api(self) -> ContestAPI:
        with (
            patch.object(contest_api, "BOT_TOKEN", "123:abc"),
            patch.object(contest_api, "DATABASE_URL", "postgres://localhost/db"),
            patch.object(contest_api, "CONTEST_CHANNEL_ID", "-1001"),
        ):
            return ContestAPI()

    async def test_cast_vote_first_vote_creates_record(self):
        api = self._build_api()
        conn = SimpleNamespace(
            fetchrow=AsyncMock(return_value={"id": 10, "user_id": 77, "status": "approved"}),
            fetchval=AsyncMock(side_effect=[0, None]),
            execute=AsyncMock(),
            transaction=lambda: _DummyTx(),
        )
        api._pool = SimpleNamespace(acquire=lambda: _AcquireCtx(conn))
        request = SimpleNamespace(
            json=AsyncMock(return_value={"entry_id": 10}),
            remote="127.0.0.1",
            headers={"User-Agent": "UA"},
        )

        with (
            patch.object(api, "_extract_auth", return_value={"user_id": 42}),
            patch.object(api, "_ensure_channel_subscription", AsyncMock()),
            patch.object(api, "_is_suspicious", AsyncMock(return_value=False)),
        ):
            response = await api.cast_vote(request)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 200)
        self.assertTrue(payload["ok"])
        conn.execute.assert_awaited_once()

    async def test_cast_vote_enforces_max_votes_limit(self):
        api = self._build_api()
        conn = SimpleNamespace(
            fetchrow=AsyncMock(return_value={"id": 10, "user_id": 77, "status": "approved"}),
            fetchval=AsyncMock(return_value=3),
            execute=AsyncMock(),
            transaction=lambda: _DummyTx(),
        )
        api._pool = SimpleNamespace(acquire=lambda: _AcquireCtx(conn))
        request = SimpleNamespace(
            json=AsyncMock(return_value={"entry_id": 10}),
            remote="127.0.0.1",
            headers={"User-Agent": "UA"},
        )

        with (
            patch.object(api, "_extract_auth", return_value={"user_id": 42}),
            patch.object(api, "_ensure_channel_subscription", AsyncMock()),
        ):
            response = await api.cast_vote(request)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 400)
        self.assertEqual(payload["error_code"], "vote_limit_reached")

    async def test_cast_vote_rejects_duplicate_vote_for_entry(self):
        api = self._build_api()
        conn = SimpleNamespace(
            fetchrow=AsyncMock(return_value={"id": 10, "user_id": 77, "status": "approved"}),
            fetchval=AsyncMock(side_effect=[1, 1]),
            execute=AsyncMock(),
            transaction=lambda: _DummyTx(),
        )
        api._pool = SimpleNamespace(acquire=lambda: _AcquireCtx(conn))
        request = SimpleNamespace(
            json=AsyncMock(return_value={"entry_id": 10}),
            remote="127.0.0.1",
            headers={"User-Agent": "UA"},
        )

        with (
            patch.object(api, "_extract_auth", return_value={"user_id": 42}),
            patch.object(api, "_ensure_channel_subscription", AsyncMock()),
        ):
            response = await api.cast_vote(request)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 409)
        self.assertEqual(payload["error_code"], "duplicate_vote")

    async def test_cast_vote_forbids_self_vote(self):
        api = self._build_api()
        conn = SimpleNamespace(
            fetchrow=AsyncMock(return_value={"id": 10, "user_id": 42, "status": "approved"}),
            fetchval=AsyncMock(),
            execute=AsyncMock(),
            transaction=lambda: _DummyTx(),
        )
        api._pool = SimpleNamespace(acquire=lambda: _AcquireCtx(conn))
        request = SimpleNamespace(
            json=AsyncMock(return_value={"entry_id": 10}),
            remote="127.0.0.1",
            headers={"User-Agent": "UA"},
        )

        with (
            patch.object(api, "_extract_auth", return_value={"user_id": 42}),
            patch.object(api, "_ensure_channel_subscription", AsyncMock()),
        ):
            response = await api.cast_vote(request)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 400)
        self.assertEqual(payload["error_code"], "self_vote_forbidden")

    async def test_cast_vote_blocks_when_subscription_missing(self):
        api = self._build_api()
        conn = SimpleNamespace(
            fetchrow=AsyncMock(),
            fetchval=AsyncMock(),
            execute=AsyncMock(),
            transaction=lambda: _DummyTx(),
        )
        api._pool = SimpleNamespace(acquire=lambda: _AcquireCtx(conn))
        request = SimpleNamespace(
            json=AsyncMock(return_value={"entry_id": 10}),
            remote="127.0.0.1",
            headers={"User-Agent": "UA"},
        )

        with (
            patch.object(api, "_extract_auth", return_value={"user_id": 42}),
            patch.object(
                api,
                "_ensure_channel_subscription",
                AsyncMock(side_effect=VoteError("need sub", "subscription_required", 403)),
            ),
        ):
            response = await api.cast_vote(request)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 403)
        self.assertEqual(payload["error_code"], "subscription_required")


if __name__ == "__main__":
    unittest.main()