"""
database.py — слой работы с SQLite через aiosqlite.
Структура:
  chats(chat_id, title)
  clans(id, chat_id, name)
  triggers(id, clan_id, trigger)
  members(id, clan_id, user_id, username)
"""
import aiosqlite
import os

DB_PATH = os.getenv("DB_PATH", "shimmbot.db")


class Database:
    def __init__(self):
        self.db: aiosqlite.Connection | None = None

    async def init(self):
        self.db = await aiosqlite.connect(DB_PATH)
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript("""
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                title   TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS clans (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                name    TEXT NOT NULL,
                UNIQUE(chat_id, name)
            );
            CREATE TABLE IF NOT EXISTS triggers (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                clan_id INTEGER NOT NULL REFERENCES clans(id) ON DELETE CASCADE,
                trigger TEXT NOT NULL,
                UNIQUE(clan_id, trigger)
            );
            CREATE TABLE IF NOT EXISTS members (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                clan_id  INTEGER NOT NULL REFERENCES clans(id) ON DELETE CASCADE,
                user_id  INTEGER NOT NULL,
                username TEXT,
                UNIQUE(clan_id, user_id)
            );
        """)
        await self.db.commit()

    # ── Чаты ──────────────────────────────────────────────────
    async def register_chat(self, chat_id: int, title: str):
        await self.db.execute(
            "INSERT INTO chats(chat_id, title) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title",
            (chat_id, title)
        )
        await self.db.commit()

    async def get_chats(self) -> list[dict]:
        async with self.db.execute("SELECT chat_id, title FROM chats") as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_chat(self, chat_id: int) -> dict | None:
        async with self.db.execute("SELECT chat_id, title FROM chats WHERE chat_id=?", (chat_id,)) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    # ── Кланы ─────────────────────────────────────────────────
    async def create_clan(self, chat_id: int, name: str, triggers: list[str]) -> dict:
        async with self.db.execute(
            "INSERT INTO clans(chat_id, name) VALUES(?,?) ON CONFLICT(chat_id, name) DO NOTHING RETURNING id",
            (chat_id, name)
        ) as cur:
            row = await cur.fetchone()

        if row is None:
            async with self.db.execute("SELECT id FROM clans WHERE chat_id=? AND name=?", (chat_id, name)) as cur:
                row = await cur.fetchone()

        clan_id = row["id"]
        for t in triggers:
            await self.db.execute(
                "INSERT INTO triggers(clan_id, trigger) VALUES(?,?) ON CONFLICT DO NOTHING",
                (clan_id, t.lower())
            )
        await self.db.commit()
        return {"id": clan_id, "name": name}

    async def get_clan(self, chat_id: int, name: str) -> dict | None:
        async with self.db.execute(
            "SELECT id, name FROM clans WHERE chat_id=? AND LOWER(name)=LOWER(?)",
            (chat_id, name)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        clan_id = row["id"]
        triggers = await self._get_triggers(clan_id)
        return {"id": clan_id, "name": row["name"], "triggers": triggers}

    async def get_clans(self, chat_id: int) -> list[dict]:
        async with self.db.execute("SELECT id, name FROM clans WHERE chat_id=?", (chat_id,)) as cur:
            rows = await cur.fetchall()
        result = []
        for r in rows:
            triggers = await self._get_triggers(r["id"])
            result.append({"id": r["id"], "name": r["name"], "triggers": triggers})
        return result

    async def delete_clan(self, chat_id: int, name: str) -> bool:
        async with self.db.execute(
            "DELETE FROM clans WHERE chat_id=? AND LOWER(name)=LOWER(?)",
            (chat_id, name)
        ) as cur:
            deleted = cur.rowcount > 0
        await self.db.commit()
        return deleted

    async def rename_clan(self, chat_id: int, old_name: str, new_name: str) -> bool:
        async with self.db.execute(
            "UPDATE clans SET name=? WHERE chat_id=? AND LOWER(name)=LOWER(?)",
            (new_name, chat_id, old_name)
        ) as cur:
            ok = cur.rowcount > 0
        await self.db.commit()
        return ok

    async def get_clan_member_count(self, chat_id: int, name: str) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) FROM members m JOIN clans c ON m.clan_id=c.id "
            "WHERE c.chat_id=? AND LOWER(c.name)=LOWER(?)",
            (chat_id, name)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    # ── Триггеры ──────────────────────────────────────────────
    async def _get_triggers(self, clan_id: int) -> list[str]:
        async with self.db.execute("SELECT trigger FROM triggers WHERE clan_id=?", (clan_id,)) as cur:
            rows = await cur.fetchall()
        return [r["trigger"] for r in rows]

    async def add_trigger(self, chat_id: int, clan_name: str, trigger: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        await self.db.execute(
            "INSERT INTO triggers(clan_id, trigger) VALUES(?,?) ON CONFLICT DO NOTHING",
            (clan["id"], trigger.lower())
        )
        await self.db.commit()
        return True

    async def remove_trigger(self, chat_id: int, clan_name: str, trigger: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.db.execute(
            "DELETE FROM triggers WHERE clan_id=? AND LOWER(trigger)=LOWER(?)",
            (clan["id"], trigger)
        ) as cur:
            ok = cur.rowcount > 0
        await self.db.commit()
        return ok

    async def rename_trigger(self, chat_id: int, clan_name: str, old_t: str, new_t: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.db.execute(
            "UPDATE triggers SET trigger=? WHERE clan_id=? AND LOWER(trigger)=LOWER(?)",
            (new_t.lower(), clan["id"], old_t)
        ) as cur:
            ok = cur.rowcount > 0
        await self.db.commit()
        return ok

    async def get_clan_by_trigger(self, chat_id: int, trigger: str) -> dict | None:
        async with self.db.execute(
            """
            SELECT c.id, c.name
            FROM triggers t
            JOIN clans c ON t.clan_id = c.id
            WHERE c.chat_id=? AND LOWER(t.trigger)=LOWER(?)
            """,
            (chat_id, trigger)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return {"id": row["id"], "name": row["name"]}

    # ── Участники ─────────────────────────────────────────────
    async def add_member(self, chat_id: int, clan_name: str, user_id: int, username: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.db.execute(
            "INSERT INTO members(clan_id, user_id, username) VALUES(?,?,?) ON CONFLICT DO NOTHING",
            (clan["id"], user_id, username)
        ) as cur:
            added = cur.rowcount > 0
        await self.db.commit()
        return added

    async def remove_member(self, chat_id: int, clan_name: str, user_id: int) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.db.execute(
            "DELETE FROM members WHERE clan_id=? AND user_id=?",
            (clan["id"], user_id)
        ) as cur:
            ok = cur.rowcount > 0
        await self.db.commit()
        return ok

    async def get_clan_members(self, chat_id: int, clan_name: str) -> list[dict]:
        async with self.db.execute(
            """
            SELECT m.user_id, m.username
            FROM members m
            JOIN clans c ON m.clan_id = c.id
            WHERE c.chat_id=? AND LOWER(c.name)=LOWER(?)
            """,
            (chat_id, clan_name)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]


db = Database()
