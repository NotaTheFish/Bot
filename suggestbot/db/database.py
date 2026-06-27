import asyncpg
from config import DATABASE_URL, TABLE_PREFIX

pool: asyncpg.Pool | None = None


async def create_pool() -> None:
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await init_tables()


async def get_pool() -> asyncpg.Pool:
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    return pool


async def init_tables() -> None:
    p = TABLE_PREFIX
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {p}users (
                user_id     BIGINT PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                is_banned   BOOLEAN NOT NULL DEFAULT FALSE,
                ban_reason  TEXT,
                banned_by   BIGINT,
                banned_at   TIMESTAMPTZ,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS {p}submissions (
                id                  SERIAL PRIMARY KEY,
                user_id             BIGINT NOT NULL,
                status              TEXT NOT NULL DEFAULT 'pending',
                forward_mode        TEXT,
                media_group_id      TEXT,
                admin_msg_id        BIGINT,
                admin_chat_id       BIGINT,
                channel_msg_id      BIGINT,
                source_chat_id      BIGINT,
                source_message_ids  BIGINT[],
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                handled_by          BIGINT,
                FOREIGN KEY (user_id) REFERENCES {p}users(user_id)
            );

            CREATE TABLE IF NOT EXISTS {p}submission_content (
                id              SERIAL PRIMARY KEY,
                submission_id   INT NOT NULL REFERENCES {p}submissions(id) ON DELETE CASCADE,
                content_type    TEXT NOT NULL,
                file_id         TEXT,
                caption         TEXT,
                sort_order      INT NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS {p}settings (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            );

            INSERT INTO {p}settings (key, value) VALUES
                ('forward_mode', 'repost'),
                ('show_author_btn', 'true'),
                ('channel_setup_done', 'false')
            ON CONFLICT (key) DO NOTHING;
        """)
