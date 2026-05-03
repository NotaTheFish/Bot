import asyncio
import getpass

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

print("### RUNNING CODE LOGIN SESSION SCRIPT ###")

api_id = int(input("API_ID: ").strip())
api_hash = input("API_HASH: ").strip()
phone = input("PHONE (+380...): ").strip()

async def main():
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    try:
        if not await client.is_user_authorized():
            await client.send_code_request(phone)

            code = input("Код из Telegram: ").strip()
            try:
                await client.sign_in(phone=phone, code=code)
            except SessionPasswordNeededError:
                pwd = getpass.getpass("2FA password: ")
                await client.sign_in(password=pwd)

        print("\n✅ LOGIN SUCCESS")
        print("\nSTRING_SESSION:\n")
        print(client.session.save())

    finally:
        await client.disconnect()

asyncio.run(main())