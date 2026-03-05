import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("API_ID: "))
api_hash = input("API_HASH: ")

async def main():
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    qr_login = await client.qr_login()

    print("\nОткрой Telegram на телефоне:")
    print("Settings → Devices → Link Desktop Device")
    print("\nИ отсканируй этот QR:\n")

    print(qr_login.url)

    await qr_login.wait()

    print("\nУСПЕШНО!")
    print("\nSTRING SESSION:\n")
    print(client.session.save())

asyncio.run(main())