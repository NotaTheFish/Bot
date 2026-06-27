import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
ADMIN_IDS: list[int] = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
CHANNEL_ID: int = int(os.getenv("CHANNEL_ID", "0"))
LOG_CHAT_ID: int = int(os.getenv("LOG_CHAT_ID", "0"))
DATABASE_URL: str = os.getenv("DATABASE_URL", "")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

FLOOD_LIMIT_SECONDS: int = 300  # 5 минут между предложками
TABLE_PREFIX: str = "sgb_"
