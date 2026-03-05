from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio

api_id = int(input("API_ID: ").strip())
api_hash = input("API_HASH: ").strip()

async def main():
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        qr = await client.qr_login()
        print("\nОткрой Telegram на телефоне → Settings → Devices → Link Desktop Device")
        print("И отсканируй этот QR (ссылка ниже):\n")
        print(qr.url)
        print("\nЖду подтверждения на телефоне...\n")

        await qr.wait()  # ждёт пока ты подтвердишь вход
        print("✅ Вход выполнен!")
        print("\nSTRING_SESSION:\n" + client.session.save() + "\n")

asyncio.run(main())