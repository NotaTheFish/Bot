import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)


CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS rvb_sellers (
    id BIGINT PRIMARY KEY,
    username TEXT,
    shop_name TEXT NOT NULL DEFAULT '',
    template_id TEXT NOT NULL DEFAULT 'classic_gold',
    stars_mode TEXT NOT NULL DEFAULT 'buyer_choice',
    stars_value INT NOT NULL DEFAULT 5,
    item_mode TEXT NOT NULL DEFAULT 'free',
    item_value TEXT NOT NULL DEFAULT '',
    allow_template_choice BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rvb_client_templates (
    id BIGINT PRIMARY KEY,
    username TEXT,
    template_id TEXT NOT NULL DEFAULT 'classic_gold',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rvb_custom_templates (
    id SERIAL PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    creator_id BIGINT NOT NULL,
    creator_username TEXT,
    name TEXT NOT NULL DEFAULT 'Мой шаблон',
    layout TEXT NOT NULL DEFAULT 'classic',
    font TEXT NOT NULL DEFAULT 'montserrat',
    text_color TEXT NOT NULL DEFAULT '#e0e0e0',
    accent_color TEXT NOT NULL DEFAULT '#c9a84c',
    bg_color TEXT NOT NULL DEFAULT '#1a1a2e',
    bg_image TEXT,
    is_edited BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rvb_template_keys (
    key TEXT PRIMARY KEY,
    template_id INT NOT NULL REFERENCES rvb_custom_templates(id) ON DELETE CASCADE,
    used BOOLEAN NOT NULL DEFAULT FALSE,
    used_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rvb_custom_owner_idx ON rvb_custom_templates(owner_id);

CREATE TABLE IF NOT EXISTS rvb_reviews (
    id SERIAL PRIMARY KEY,
    seller_id BIGINT NOT NULL REFERENCES rvb_sellers(id) ON DELETE CASCADE,
    buyer_id BIGINT NOT NULL,
    buyer_name TEXT NOT NULL DEFAULT '',
    buyer_username TEXT,
    review_text TEXT NOT NULL DEFAULT '',
    item_bought TEXT NOT NULL DEFAULT '',
    stars INT NOT NULL DEFAULT 5,
    template_used TEXT NOT NULL DEFAULT 'classic_gold',
    card_file_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rvb_reviews_seller_idx ON rvb_reviews(seller_id);
"""


class Database:
    def __init__(self, url: str):
        self.url = url
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.url, min_size=2, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_TABLES)
        logger.info("Database initialized")

    async def close(self):
        if self.pool:
            await self.pool.close()

    # ── Custom Templates (конструктор) ────────────────────────────────────

    async def create_custom_template(self, owner_id: int, creator_id: int,
                                       creator_username, **fields) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO rvb_custom_templates
                    (owner_id, creator_id, creator_username, name, layout, font,
                     text_color, accent_color, bg_color, bg_image, is_edited)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                RETURNING *
            """, owner_id, creator_id, creator_username,
                fields.get("name", "Мой шаблон"),
                fields.get("layout", "classic"),
                fields.get("font", "montserrat"),
                fields.get("text_color", "#e0e0e0"),
                fields.get("accent_color", "#c9a84c"),
                fields.get("bg_color", "#1a1a2e"),
                fields.get("bg_image"),
                fields.get("is_edited", False))
            return dict(row)

    async def get_custom_template(self, template_id: int):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_custom_templates WHERE id = $1", template_id)
            return dict(row) if row else None

    async def list_custom_templates(self, owner_id: int) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM rvb_custom_templates WHERE owner_id = $1 ORDER BY created_at DESC",
                owner_id)
            return [dict(r) for r in rows]

    async def count_custom_templates(self, owner_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM rvb_custom_templates WHERE owner_id = $1", owner_id)

    async def update_custom_template(self, template_id: int, **fields):
        if not fields:
            return
        sets, vals = [], []
        for i, (k, v) in enumerate(fields.items(), start=1):
            sets.append(f"{k} = ${i}")
            vals.append(v)
        vals.append(template_id)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE rvb_custom_templates SET {', '.join(sets)}, updated_at=NOW() "
                f"WHERE id = ${len(vals)} RETURNING *", *vals)
            return dict(row) if row else None

    async def delete_custom_template(self, template_id: int, owner_id: int) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM rvb_custom_templates WHERE id = $1 AND owner_id = $2",
                template_id, owner_id)
            return result.endswith("1")

    # ── Template Keys (шаринг) ────────────────────────────────────────────

    async def create_template_key(self, key: str, template_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO rvb_template_keys (key, template_id) VALUES ($1, $2)",
                key, template_id)

    async def get_template_key(self, key: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_template_keys WHERE key = $1", key)
            return dict(row) if row else None

    async def use_template_key(self, key: str, used_by: int) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE rvb_template_keys SET used = TRUE, used_by = $1 "
                "WHERE key = $2 AND used = FALSE", used_by, key)
            return result.endswith("1")

    # ── Client Templates ───────────────────────────────────────────────────

    async def get_client_template(self, user_id: int):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_client_templates WHERE id = $1", user_id
            )
            return dict(row) if row else None

    async def save_client_template(self, user_id: int, username, template_id: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO rvb_client_templates (id, username, template_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    template_id = $3,
                    updated_at = NOW()
                RETURNING *
            """, user_id, username, template_id)
            return dict(row)

    # ── Sellers ────────────────────────────────────────────────────────────

    async def get_seller(self, user_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_sellers WHERE id = $1", user_id
            )
            return dict(row) if row else None

    async def upsert_seller(self, user_id: int, username: Optional[str], shop_name: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO rvb_sellers (id, username, shop_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    updated_at = NOW()
                RETURNING *
            """, user_id, username, shop_name)
            return dict(row)

    async def update_seller(self, user_id: int, **fields) -> Optional[dict]:
        if not fields:
            return await self.get_seller(user_id)
        set_parts = []
        values = []
        for i, (k, v) in enumerate(fields.items(), start=1):
            set_parts.append(f"{k} = ${i}")
            values.append(v)
        values.append(user_id)
        query = f"UPDATE rvb_sellers SET {', '.join(set_parts)}, updated_at = NOW() WHERE id = ${len(values)} RETURNING *"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *values)
            return dict(row) if row else None

    async def save_review(
        self, seller_id: int, buyer_id: int, buyer_name: str,
        buyer_username: Optional[str], review_text: str,
        item_bought: str, stars: int, template_used: str,
        card_file_id: Optional[str] = None
    ) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO rvb_reviews
                    (seller_id, buyer_id, buyer_name, buyer_username,
                     review_text, item_bought, stars, template_used, card_file_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                RETURNING *
            """, seller_id, buyer_id, buyer_name, buyer_username,
                review_text, item_bought, stars, template_used, card_file_id)
            return dict(row)

    async def update_review_file_id(self, review_id: int, file_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_reviews SET card_file_id = $1 WHERE id = $2",
                file_id, review_id
            )

    async def get_seller_stats(self, seller_id: int) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as total,
                       ROUND(AVG(stars), 1) as avg_stars
                FROM rvb_reviews WHERE seller_id = $1
            """, seller_id)
            return dict(row) if row else {"total": 0, "avg_stars": 0}
