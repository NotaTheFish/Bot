import asyncio
import qrcode
import getpass

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

print("### RUNNING QR SCRIPT make_qr_session_ascii.py ###")

api_id = int(input("API_ID: ").strip())
api_hash = input("API_HASH: ").strip()

async def main():
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    try:
        qr_login = await client.qr_login()

        print("\nОткрой Telegram на телефоне:")
        print("Settings -> Devices -> Link Desktop Device")
        print("\nСканируй QR с экрана компьютера:\n")

        qr = qrcode.QRCode(border=1)
        qr.add_data(qr_login.url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)

        try:
            await qr_login.wait()
        except SessionPasswordNeededError:
            # 2FA включена — нужен пароль
            pwd = getpass.getpass("\n2FA password (не будет отображаться): ")
            await client.sign_in(password=pwd)

        print("\n✅ LOGIN SUCCESS")
        print("\nSTRING_SESSION:\n")
        print(client.session.save())

    finally:
        await client.disconnect()

asyncio.run(main())