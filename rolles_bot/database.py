"""
database.py — слой работы с PostgreSQL через asyncpg.
"""
import asyncpg
from config import settings


class Database:
    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def init(self):
        self.pool = await asyncpg.create_pool(settings.DATABASE_URL)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id BIGINT PRIMARY KEY,
                    title   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS clans (
                    id      SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    name    TEXT NOT NULL,
                    UNIQUE(chat_id, name)
                );
                CREATE TABLE IF NOT EXISTS triggers (
                    id      SERIAL PRIMARY KEY,
                    clan_id INTEGER NOT NULL REFERENCES clans(id) ON DELETE CASCADE,
                    trigger TEXT NOT NULL,
                    UNIQUE(clan_id, trigger)
                );
                CREATE TABLE IF NOT EXISTS members (
                    id       SERIAL PRIMARY KEY,
                    clan_id  INTEGER NOT NULL REFERENCES clans(id) ON DELETE CASCADE,
                    user_id  BIGINT NOT NULL,
                    username TEXT,
                    UNIQUE(clan_id, user_id)
                );
            """)

    # ── Чаты ──────────────────────────────────────────────────
    async def register_chat(self, chat_id: int, title: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chats(chat_id, title) VALUES($1,$2) "
                "ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title",
                chat_id, title
            )

    async def get_chats(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id, title FROM chats")
        return [dict(r) for r in rows]

    async def get_chat(self, chat_id: int) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT chat_id, title FROM chats WHERE chat_id=$1", chat_id)
        return dict(row) if row else None

    # ── Кланы ─────────────────────────────────────────────────
    async def create_clan(self, chat_id: int, name: str, triggers: list[str]) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO clans(chat_id, name) VALUES($1,$2) "
                "ON CONFLICT(chat_id, name) DO UPDATE SET name=EXCLUDED.name "
                "RETURNING id",
                chat_id, name
            )
            clan_id = row["id"]
            for t in triggers:
                await conn.execute(
                    "INSERT INTO triggers(clan_id, trigger) VALUES($1,$2) ON CONFLICT DO NOTHING",
                    clan_id, t.lower()
                )
        return {"id": clan_id, "name": name}

    async def get_clan(self, chat_id: int, name: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, name FROM clans WHERE chat_id=$1 AND LOWER(name)=LOWER($2)",
                chat_id, name
            )
            if not row:
                return None
            triggers = await self._get_triggers(conn, row["id"])
        return {"id": row["id"], "name": row["name"], "triggers": triggers}

    async def get_clans(self, chat_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, name FROM clans WHERE chat_id=$1", chat_id)
            result = []
            for r in rows:
                triggers = await self._get_triggers(conn, r["id"])
                result.append({"id": r["id"], "name": r["name"], "triggers": triggers})
        return result

    async def delete_clan(self, chat_id: int, name: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM clans WHERE chat_id=$1 AND LOWER(name)=LOWER($2)",
                chat_id, name
            )
        return result.split()[-1] != "0"

    async def rename_clan(self, chat_id: int, old_name: str, new_name: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE clans SET name=$1 WHERE chat_id=$2 AND LOWER(name)=LOWER($3)",
                new_name, chat_id, old_name
            )
        return result.split()[-1] != "0"

    async def get_clan_member_count(self, chat_id: int, name: str) -> int:
        async with self.pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT COUNT(*) FROM members m JOIN clans c ON m.clan_id=c.id "
                "WHERE c.chat_id=$1 AND LOWER(c.name)=LOWER($2)",
                chat_id, name
            )
        return val or 0

    # ── Триггеры ──────────────────────────────────────────────
    async def _get_triggers(self, conn, clan_id: int) -> list[str]:
        rows = await conn.fetch("SELECT trigger FROM triggers WHERE clan_id=$1", clan_id)
        return [r["trigger"] for r in rows]

    async def add_trigger(self, chat_id: int, clan_name: str, trigger: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO triggers(clan_id, trigger) VALUES($1,$2) ON CONFLICT DO NOTHING",
                clan["id"], trigger.lower()
            )
        return True

    async def remove_trigger(self, chat_id: int, clan_name: str, trigger: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM triggers WHERE clan_id=$1 AND LOWER(trigger)=LOWER($2)",
                clan["id"], trigger
            )
        return result.split()[-1] != "0"

    async def rename_trigger(self, chat_id: int, clan_name: str, old_t: str, new_t: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE triggers SET trigger=$1 WHERE clan_id=$2 AND LOWER(trigger)=LOWER($3)",
                new_t.lower(), clan["id"], old_t
            )
        return result.split()[-1] != "0"

    async def get_clan_by_trigger(self, chat_id: int, trigger: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT c.id, c.name
                FROM triggers t
                JOIN clans c ON t.clan_id = c.id
                WHERE c.chat_id=$1 AND LOWER(t.trigger)=LOWER($2)
                """,
                chat_id, trigger
            )
        return {"id": row["id"], "name": row["name"]} if row else None

    # ── Участники ─────────────────────────────────────────────
    async def add_member(self, chat_id: int, clan_name: str, user_id: int, username: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "INSERT INTO members(clan_id, user_id, username) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                clan["id"], user_id, username
            )
        return result.split()[-1] != "0"

    async def remove_member(self, chat_id: int, clan_name: str, user_id: int) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM members WHERE clan_id=$1 AND user_id=$2",
                clan["id"], user_id
            )
        return result.split()[-1] != "0"

    async def get_clan_members(self, chat_id: int, clan_name: str) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT m.user_id, m.username
                FROM members m
                JOIN clans c ON m.clan_id = c.id
                WHERE c.chat_id=$1 AND LOWER(c.name)=LOWER($2)
                """,
                chat_id, clan_name
            )
        return [dict(r) for r in rows]


db = Database()