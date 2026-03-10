import hashlib
import hmac
import json
import time
import unittest
from urllib.parse import urlencode

from bot_controller.contest_api import parse_and_verify_init_data


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


if __name__ == "__main__":
    unittest.main()