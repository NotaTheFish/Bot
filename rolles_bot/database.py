"""
database.py — слой работы с PostgreSQL через asyncpg.
"""
import asyncpg
from config import settings

COOLDOWN_SECONDS = 600  # 10 минут
COOLDOWN_MAX = 3        # максимум созывов за период


class Database:
    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def init(self):
        self.pool = await asyncpg.create_pool(settings.DATABASE_URL)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS shimm_chats (
                    chat_id BIGINT PRIMARY KEY,
                    title   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS shimm_clans (
                    id      SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    name    TEXT NOT NULL,
                    UNIQUE(chat_id, name)
                );
                CREATE TABLE IF NOT EXISTS shimm_triggers (
                    id      SERIAL PRIMARY KEY,
                    clan_id INTEGER NOT NULL REFERENCES shimm_clans(id) ON DELETE CASCADE,
                    trigger TEXT NOT NULL,
                    UNIQUE(clan_id, trigger)
                );
                CREATE TABLE IF NOT EXISTS shimm_members (
                    id       SERIAL PRIMARY KEY,
                    clan_id  INTEGER NOT NULL REFERENCES shimm_clans(id) ON DELETE CASCADE,
                    user_id  BIGINT NOT NULL,
                    username TEXT,
                    UNIQUE(clan_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS shimm_chat_admins (
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    PRIMARY KEY(chat_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS shimm_keys (
                    key     TEXT PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    used    BOOLEAN NOT NULL DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS shimm_cooldowns (
                    chat_id  BIGINT NOT NULL,
                    user_id  BIGINT NOT NULL,
                    count    INTEGER NOT NULL DEFAULT 1,
                    reset_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY(chat_id, user_id)
                );
            """)

    # ── Чаты ──────────────────────────────────────────────────
    async def register_chat(self, chat_id: int, title: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shimm_chats(chat_id, title) VALUES($1,$2) "
                "ON CONFLICT(chat_id) DO UPDATE SET title=EXCLUDED.title",
                chat_id, title
            )

    async def get_chats(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id, title FROM shimm_chats")
        return [dict(r) for r in rows]

    async def get_chat(self, chat_id: int) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT chat_id, title FROM shimm_chats WHERE chat_id=$1", chat_id)
        return dict(row) if row else None

    # ── Кланы ─────────────────────────────────────────────────
    async def create_clan(self, chat_id: int, name: str, triggers: list[str]) -> dict | None:
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT id FROM shimm_clans WHERE chat_id=$1 AND LOWER(name)=LOWER($2)",
                chat_id, name
            )
            if exists:
                return None
            row = await conn.fetchrow(
                "INSERT INTO shimm_clans(chat_id, name) VALUES($1,$2) RETURNING id",
                chat_id, name
            )
            clan_id = row["id"]
            for t in triggers:
                await conn.execute(
                    "INSERT INTO shimm_triggers(clan_id, trigger) VALUES($1,$2) ON CONFLICT DO NOTHING",
                    clan_id, t.lower()
                )
        return {"id": clan_id, "name": name}

    async def get_clan(self, chat_id: int, name: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, name FROM shimm_clans WHERE chat_id=$1 AND LOWER(name)=LOWER($2)",
                chat_id, name
            )
            if not row:
                return None
            triggers = await self._get_triggers(conn, row["id"])
        return {"id": row["id"], "name": row["name"], "triggers": triggers}

    async def get_clans(self, chat_id: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, name FROM shimm_clans WHERE chat_id=$1", chat_id)
            result = []
            for r in rows:
                triggers = await self._get_triggers(conn, r["id"])
                result.append({"id": r["id"], "name": r["name"], "triggers": triggers})
        return result

    async def delete_clan(self, chat_id: int, name: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM shimm_clans WHERE chat_id=$1 AND LOWER(name)=LOWER($2)",
                chat_id, name
            )
        return result.split()[-1] != "0"

    async def rename_clan(self, chat_id: int, old_name: str, new_name: str) -> bool:
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT id FROM shimm_clans WHERE chat_id=$1 AND LOWER(name)=LOWER($2) AND LOWER(name)!=LOWER($3)",
                chat_id, new_name, old_name
            )
            if exists:
                return False
            result = await conn.execute(
                "UPDATE shimm_clans SET name=$1 WHERE chat_id=$2 AND LOWER(name)=LOWER($3)",
                new_name, chat_id, old_name
            )
        return result.split()[-1] != "0"

    async def get_clan_member_count(self, chat_id: int, name: str) -> int:
        async with self.pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT COUNT(*) FROM shimm_members m JOIN shimm_clans c ON m.clan_id=c.id "
                "WHERE c.chat_id=$1 AND LOWER(c.name)=LOWER($2)",
                chat_id, name
            )
        return val or 0

    # ── Триггеры ──────────────────────────────────────────────
    async def _get_triggers(self, conn, clan_id: int) -> list[str]:
        rows = await conn.fetch("SELECT trigger FROM shimm_triggers WHERE clan_id=$1", clan_id)
        return [r["trigger"] for r in rows]

    async def add_trigger(self, chat_id: int, clan_name: str, trigger: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shimm_triggers(clan_id, trigger) VALUES($1,$2) ON CONFLICT DO NOTHING",
                clan["id"], trigger.lower()
            )
        return True

    async def remove_trigger(self, chat_id: int, clan_name: str, trigger: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM shimm_triggers WHERE clan_id=$1 AND LOWER(trigger)=LOWER($2)",
                clan["id"], trigger
            )
        return result.split()[-1] != "0"

    async def rename_trigger(self, chat_id: int, clan_name: str, old_t: str, new_t: str) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE shimm_triggers SET trigger=$1 WHERE clan_id=$2 AND LOWER(trigger)=LOWER($3)",
                new_t.lower(), clan["id"], old_t
            )
        return result.split()[-1] != "0"

    async def get_clans_by_trigger(self, chat_id: int, trigger: str) -> list[dict]:
        """Все кланы с данным триггером."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT c.id, c.name
                FROM shimm_triggers t
                JOIN shimm_clans c ON t.clan_id = c.id
                WHERE c.chat_id=$1 AND LOWER(t.trigger)=LOWER($2)
                """,
                chat_id, trigger
            )
        return [{"id": r["id"], "name": r["name"]} for r in rows]

    # ── Участники ─────────────────────────────────────────────
    async def add_member(self, chat_id: int, clan_name: str, user_id: int, username: str | None) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """INSERT INTO shimm_members(clan_id, user_id, username) VALUES($1,$2,$3)
                ON CONFLICT(clan_id, user_id) DO UPDATE SET username=EXCLUDED.username
                WHERE shimm_members.username IS DISTINCT FROM EXCLUDED.username""",
                clan["id"], user_id, username
            )
        return result.split()[-1] != "0"

    async def remove_member(self, chat_id: int, clan_name: str, user_id: int) -> bool:
        clan = await self.get_clan(chat_id, clan_name)
        if not clan:
            return False
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM shimm_members WHERE clan_id=$1 AND user_id=$2",
                clan["id"], user_id
            )
        return result.split()[-1] != "0"

    async def get_clan_members(self, chat_id: int, clan_name: str) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT m.user_id, m.username
                FROM shimm_members m
                JOIN shimm_clans c ON m.clan_id = c.id
                WHERE c.chat_id=$1 AND LOWER(c.name)=LOWER($2)
                """,
                chat_id, clan_name
            )
        return [dict(r) for r in rows]

    async def update_member_username(self, chat_id: int, user_id: int, username: str | None):
        """Обновляет username участника во всех его кланах этого чата."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE shimm_members SET username=$1
                WHERE user_id=$2
                AND clan_id IN (SELECT id FROM shimm_clans WHERE chat_id=$3)""",
                username, user_id, chat_id
            )

    async def get_all_chat_members_tg(self, chat_id: int) -> list[dict]:
        """Все уникальные участники кланов этого чата."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT m.user_id, m.username
                FROM shimm_members m
                JOIN shimm_clans c ON m.clan_id = c.id
                WHERE c.chat_id=$1
                """,
                chat_id
            )
        return [dict(r) for r in rows]

    async def remove_member_from_all_clans(self, chat_id: int, user_id: int) -> int:
        """Удаляет участника из всех кланов чата. Возвращает количество удалённых записей."""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """DELETE FROM shimm_members
                WHERE user_id=$1
                AND clan_id IN (SELECT id FROM shimm_clans WHERE chat_id=$2)""",
                user_id, chat_id
            )
        return int(result.split()[-1])

    # ── Кулдаун ───────────────────────────────────────────────
    async def check_cooldown(self, chat_id: int, user_id: int) -> tuple[bool, int]:
        """
        Проверяет и увеличивает счётчик созывов.
        Возвращает (allowed, remaining_seconds).
        allowed=True если лимит не превышен.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT count, reset_at FROM shimm_cooldowns WHERE chat_id=$1 AND user_id=$2",
                chat_id, user_id
            )
            now_ts = "NOW()"

            if row is None or f"reset_at < NOW()" and True:
                # Проверяем истёк ли период
                if row is not None:
                    expired = await conn.fetchval(
                        "SELECT reset_at < NOW() FROM shimm_cooldowns WHERE chat_id=$1 AND user_id=$2",
                        chat_id, user_id
                    )
                else:
                    expired = True

                if expired:
                    # Сброс счётчика
                    await conn.execute(
                        """INSERT INTO shimm_cooldowns(chat_id, user_id, count, reset_at)
                        VALUES($1,$2,1, NOW() + INTERVAL '10 minutes')
                        ON CONFLICT(chat_id, user_id) DO UPDATE
                        SET count=1, reset_at=NOW() + INTERVAL '10 minutes'""",
                        chat_id, user_id
                    )
                    return True, 0

            # Период активен
            count = row["count"]
            if count >= COOLDOWN_MAX:
                remaining = await conn.fetchval(
                    "SELECT EXTRACT(EPOCH FROM (reset_at - NOW()))::int FROM shimm_cooldowns "
                    "WHERE chat_id=$1 AND user_id=$2",
                    chat_id, user_id
                )
                return False, max(0, remaining or 0)

            # Увеличиваем счётчик
            await conn.execute(
                "UPDATE shimm_cooldowns SET count=count+1 WHERE chat_id=$1 AND user_id=$2",
                chat_id, user_id
            )
            return True, 0

    # ── Бот-админы чата ───────────────────────────────────────
    async def is_bot_admin(self, chat_id: int, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM shimm_chat_admins WHERE chat_id=$1 AND user_id=$2",
                chat_id, user_id
            )
        return row is not None

    async def add_bot_admin(self, chat_id: int, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shimm_chat_admins(chat_id, user_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
                chat_id, user_id
            )

    async def remove_bot_admin(self, chat_id: int, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM shimm_chat_admins WHERE chat_id=$1 AND user_id=$2",
                chat_id, user_id
            )

    # ── Ключи доступа ─────────────────────────────────────────
    async def create_key(self, chat_id: int) -> str:
        import secrets
        key = secrets.token_hex(4).upper()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shimm_keys(key, chat_id) VALUES($1,$2) "
                "ON CONFLICT(key) DO UPDATE SET chat_id=EXCLUDED.chat_id, used=FALSE",
                key, chat_id
            )
        return key

    async def use_key(self, key: str, chat_id: int) -> bool:
        """True если ключ валиден, привязан к этому чату и не использован."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT chat_id, used FROM shimm_keys WHERE key=$1",
                key.upper()
            )
            if not row or row["used"] or row["chat_id"] != chat_id:
                return False
            await conn.execute(
                "UPDATE shimm_keys SET used=TRUE WHERE key=$1",
                key.upper()
            )
        return True


db = Database()
