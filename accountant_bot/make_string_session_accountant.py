import os

from telethon.sessions import StringSession
from telethon.sync import TelegramClient


def _read_api_id() -> int:
    raw_api_id = (os.getenv("TG_API_ID") or "").strip()
    if not raw_api_id:
        raw_api_id = input("TG_API_ID (или API_ID): ").strip()
    return int(raw_api_id)


def _read_api_hash() -> str:
    api_hash = (os.getenv("TG_API_HASH") or "").strip()
    if not api_hash:
        api_hash = input("TG_API_HASH (или API_HASH): ").strip()
    return api_hash


def main() -> None:
    api_id = _read_api_id()
    api_hash = _read_api_hash()

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\n✅ ACCOUNTANT_TG_STRING_SESSION (сохраните и никому не показывайте):\n")
        print(client.session.save())


if __name__ == "__main__":
    main()