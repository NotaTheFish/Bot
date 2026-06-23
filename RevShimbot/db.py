import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)


CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS rvb_sellers (
    id BIGINT PRIMARY KEY,
    pub_id TEXT UNIQUE,
    pub_id_changed_at TIMESTAMPTZ,
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
    title_font TEXT NOT NULL DEFAULT 'caveat',
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
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS rvb_reviews_seller_idx ON rvb_reviews(seller_id);

CREATE TABLE IF NOT EXISTS rvb_seller_channels (
    seller_id BIGINT PRIMARY KEY REFERENCES rvb_sellers(id) ON DELETE CASCADE,
    channel_id BIGINT NOT NULL UNIQUE,
    channel_title TEXT NOT NULL DEFAULT '',
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    verify_key TEXT,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class Database:
    def __init__(self, url: str):
        self.url = url
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.url, min_size=2, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_TABLES)
            # Авто-миграция: добавляем title_font если таблица уже существовала без неё
            await conn.execute("""
                ALTER TABLE rvb_custom_templates
                ADD COLUMN IF NOT EXISTS title_font TEXT NOT NULL DEFAULT 'caveat'
            """)
            # Авто-миграция: pub_id для продавцов
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS pub_id TEXT UNIQUE
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS pub_id_changed_at TIMESTAMPTZ
            """)
            # Авто-миграция: verified_at для каналов
            await conn.execute("""
                ALTER TABLE rvb_seller_channels
                ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ
            """)
            # Авто-миграция: статус отзыва
            await conn.execute("""
                ALTER TABLE rvb_reviews
                ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending'
            """)
            # Авто-миграция: таблица каналов продавцов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rvb_seller_channels (
                    seller_id BIGINT PRIMARY KEY REFERENCES rvb_sellers(id) ON DELETE CASCADE,
                    channel_id BIGINT NOT NULL UNIQUE,
                    channel_title TEXT NOT NULL DEFAULT \'\',
                    verified BOOLEAN NOT NULL DEFAULT FALSE,
                    verify_key TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
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
                     title_font, text_color, accent_color, bg_color, bg_image, is_edited)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                RETURNING *
            """, owner_id, creator_id, creator_username,
                fields.get("name", "Мой шаблон"),
                fields.get("layout", "classic"),
                fields.get("font", "montserrat"),
                fields.get("title_font", "caveat"),
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

    async def count_own_custom_templates(self, owner_id: int) -> int:
        """Шаблоны созданные самим продавцом (не полученные от других)."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM rvb_custom_templates WHERE owner_id = $1 AND creator_id = $1",
                owner_id
            ) or 0

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

    async def get_seller_by_pubid(self, pub_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_sellers WHERE pub_id = $1", pub_id
            )
            return dict(row) if row else None

    async def ensure_pub_id(self, user_id: int) -> str:
        """Возвращает pub_id продавца, генерируя уникальный если ещё нет."""
        import secrets
        import string
        async with self.pool.acquire() as conn:
            existing = await conn.fetchval(
                "SELECT pub_id FROM rvb_sellers WHERE id = $1", user_id)
            if existing:
                return existing
            # Генерируем уникальный 4-значный код
            alphabet = string.ascii_uppercase + string.digits
            for _ in range(50):
                code = "".join(secrets.choice(alphabet) for _ in range(4))
                taken = await conn.fetchval(
                    "SELECT 1 FROM rvb_sellers WHERE pub_id = $1", code)
                if not taken:
                    await conn.execute(
                        "UPDATE rvb_sellers SET pub_id = $1 WHERE id = $2",
                        code, user_id)
                    return code
            # Крайне маловероятно — фолбэк на 5 символов
            code = "".join(secrets.choice(alphabet) for _ in range(5))
            await conn.execute(
                "UPDATE rvb_sellers SET pub_id = $1 WHERE id = $2", code, user_id)
            return code

    async def change_pub_id(self, user_id: int, new_id: str) -> tuple[bool, str]:
        """
        Меняет pub_id продавца с проверками:
        - кулдаун 24 часа
        - уникальность
        Возвращает (успех, сообщение/код_ошибки).
        Коды ошибок: 'cooldown:<секунды>', 'taken', 'ok'
        """
        import re
        from datetime import datetime, timezone, timedelta

        new_id = new_id.strip().upper()
        # Валидация формата — ровно 1-4 латинских буквы/цифры
        if not re.fullmatch(r"[A-Z0-9]{1,4}", new_id):
            return False, "format"

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT pub_id, pub_id_changed_at FROM rvb_sellers WHERE id = $1", user_id)
            if not row:
                return False, "no_seller"

            # Если новый ID совпадает с текущим — ничего не делаем
            if row["pub_id"] == new_id:
                return False, "same"

            # Проверка кулдауна (24 часа)
            changed_at = row["pub_id_changed_at"]
            if changed_at:
                now = datetime.now(timezone.utc)
                elapsed = now - changed_at
                cooldown = timedelta(hours=24)
                if elapsed < cooldown:
                    remaining = int((cooldown - elapsed).total_seconds())
                    return False, f"cooldown:{remaining}"

            # Проверка уникальности
            taken = await conn.fetchval(
                "SELECT 1 FROM rvb_sellers WHERE pub_id = $1 AND id != $2", new_id, user_id)
            if taken:
                return False, "taken"

            # Меняем — старый ID освобождается автоматически (он был в этой же строке)
            await conn.execute(
                "UPDATE rvb_sellers SET pub_id = $1, pub_id_changed_at = NOW() WHERE id = $2",
                new_id, user_id)
            return True, "ok"

    async def upsert_seller(self, user_id: int, username: Optional[str], shop_name: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO rvb_sellers (id, username, shop_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET
                    username = EXCLUDED.username,
                    shop_name = EXCLUDED.shop_name,
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

    async def get_pending_reviews(self, seller_id: int) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT r.id, r.buyer_name, r.buyer_username, r.review_text, r.stars, r.created_at, r.card_file_id
                FROM rvb_reviews r
                JOIN rvb_seller_channels c ON c.seller_id = r.seller_id
                WHERE r.seller_id = $1
                  AND r.status = 'pending'
                  AND c.verified = TRUE
                  AND r.created_at > c.verified_at
                ORDER BY r.created_at DESC
            """, seller_id)
            return [dict(r) for r in rows]

    async def set_review_status(self, review_id: int, status: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_reviews SET status = $1 WHERE id = $2",
                status, review_id
            )

    async def get_total_cards(self, seller_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM rvb_reviews WHERE seller_id = $1", seller_id
            ) or 0

    async def update_review_file_id(self, review_id: int, file_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_reviews SET card_file_id = $1 WHERE id = $2",
                file_id, review_id
            )

    # ── Seller Channels ──────────────────────────────────────────────────

    async def set_seller_channel(self, seller_id: int, channel_id: int, channel_title: str, verify_key: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_seller_channels (seller_id, channel_id, channel_title, verified, verify_key)
                VALUES ($1, $2, $3, FALSE, $4)
                ON CONFLICT (seller_id) DO UPDATE
                    SET channel_id = EXCLUDED.channel_id,
                        channel_title = EXCLUDED.channel_title,
                        verified = FALSE,
                        verify_key = EXCLUDED.verify_key,
                        created_at = NOW()
            """, seller_id, channel_id, channel_title, verify_key)

    async def get_seller_channel(self, seller_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_seller_channels WHERE seller_id = $1", seller_id
            )
            return dict(row) if row else None

    async def get_seller_by_channel(self, channel_id: int) -> Optional[dict]:
        """Найти продавца по channel_id (для my_chat_member апдейта)."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_seller_channels WHERE channel_id = $1", channel_id
            )
            return dict(row) if row else None

    async def verify_seller_channel(self, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_seller_channels SET verified = TRUE, verify_key = NULL, verified_at = NOW() WHERE channel_id = $1",
                channel_id
            )

    async def delete_seller_channel(self, seller_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM rvb_seller_channels WHERE seller_id = $1", seller_id
            )

    async def get_pending_channel_by_key(self, key: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_seller_channels WHERE verify_key = $1 AND verified = FALSE", key
            )
            return dict(row) if row else None

    async def get_global_stats(self) -> dict:
        async with self.pool.acquire() as conn:
            sellers = await conn.fetchval("SELECT COUNT(*) FROM rvb_sellers")
            reviews = await conn.fetchval("SELECT COUNT(*) FROM rvb_reviews")
            reviews_7d = await conn.fetchval(
                "SELECT COUNT(*) FROM rvb_reviews WHERE created_at > NOW() - INTERVAL '7 days'"
            )
            buyers = await conn.fetchval("SELECT COUNT(DISTINCT buyer_id) FROM rvb_reviews")
            return {
                "sellers": sellers or 0,
                "reviews": reviews or 0,
                "reviews_7d": reviews_7d or 0,
                "buyers": buyers or 0,
            }

    async def get_seller_stats(self, seller_id: int) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as total,
                       ROUND(AVG(stars), 1) as avg_stars
                FROM rvb_reviews WHERE seller_id = $1
            """, seller_id)
            return dict(row) if row else {"total": 0, "avg_stars": 0}
