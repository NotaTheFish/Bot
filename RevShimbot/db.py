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
    card_source_mode TEXT NOT NULL DEFAULT 'standard',
    allowed_custom_ids JSONB NOT NULL DEFAULT '[]',
    inline_button_mode TEXT NOT NULL DEFAULT 'shown',
    inline_template_id TEXT,
    inline_button_show BOOLEAN NOT NULL DEFAULT TRUE,
    inline_notify_seller BOOLEAN NOT NULL DEFAULT FALSE,
    anon_mode TEXT NOT NULL DEFAULT 'off',
    anon_nickname TEXT NOT NULL DEFAULT 'Анонимный покупатель',
    anon_avatar TEXT,
    inline_anon BOOLEAN NOT NULL DEFAULT FALSE,
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
    lineage_id INT,
    name TEXT NOT NULL DEFAULT 'Мой шаблон',
    layout TEXT NOT NULL DEFAULT 'classic',
    font TEXT NOT NULL DEFAULT 'montserrat',
    title_font TEXT NOT NULL DEFAULT 'caveat',
    text_color TEXT NOT NULL DEFAULT '#e0e0e0',
    accent_color TEXT NOT NULL DEFAULT '#c9a84c',
    bg_color TEXT NOT NULL DEFAULT '#1a1a2e',
    bg_image TEXT,
    is_edited BOOLEAN NOT NULL DEFAULT FALSE,
    extra_cfg JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rvb_template_keys (
    key TEXT PRIMARY KEY,
    template_id INT NOT NULL REFERENCES rvb_custom_templates(id) ON DELETE CASCADE,
    key_type TEXT NOT NULL DEFAULT 'copy',
    max_uses INT,
    uses_count INT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ,
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
    show_buyer_button BOOLEAN NOT NULL DEFAULT TRUE,
    proof_count INT NOT NULL DEFAULT 0,
    is_anonymous BOOLEAN NOT NULL DEFAULT FALSE,
    verify_code TEXT UNIQUE,
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
    numbering_mode TEXT NOT NULL DEFAULT 'off',
    numbering_start INT NOT NULL DEFAULT 1,
    numbering_template TEXT NOT NULL DEFAULT 'Отзыв №{n}',
    numbering_entities TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS rvb_review_numbers (
    id SERIAL PRIMARY KEY,
    seller_id BIGINT NOT NULL,
    review_id INT,
    number INT NOT NULL,
    channel_msg_id BIGINT,
    number_msg_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS rvb_bot_users (
    id BIGINT PRIMARY KEY,
    username TEXT,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_blocked BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS rvb_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS rvb_ad_pins (
    user_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    pinned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, message_id)
);
CREATE TABLE IF NOT EXISTS rvb_banned_users (
    id BIGINT PRIMARY KEY,
    username TEXT,
    banned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notified BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ,
    reason TEXT
);
CREATE TABLE IF NOT EXISTS rvb_admin_log (
    id SERIAL PRIMARY KEY,
    action TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


# Crockford Base32: без I, L, O, U — не путается при чтении с карточки
_VERIFY_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _gen_verify_code() -> str:
    import secrets
    return "RSB-" + "".join(secrets.choice(_VERIFY_ALPHABET) for _ in range(8))


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
            # Авто-миграция: extra_cfg (JSON со всеми расширенными настройками)
            await conn.execute("""
                ALTER TABLE rvb_custom_templates
                ADD COLUMN IF NOT EXISTS extra_cfg JSONB NOT NULL DEFAULT '{}'
            """)
            # Авто-миграция: lineage_id (связь копий с оригиналом для передачи авторства)
            await conn.execute("""
                ALTER TABLE rvb_custom_templates
                ADD COLUMN IF NOT EXISTS lineage_id INT
            """)
            # Бэкфилл: у кого lineage_id пустой — ставим равным собственному id
            await conn.execute("""
                UPDATE rvb_custom_templates SET lineage_id = id WHERE lineage_id IS NULL
            """)
            # Авто-миграция: расширенные поля ключей (лимиты, срок, тип)
            await conn.execute("""
                ALTER TABLE rvb_template_keys
                ADD COLUMN IF NOT EXISTS key_type TEXT NOT NULL DEFAULT 'copy'
            """)
            await conn.execute("""
                ALTER TABLE rvb_template_keys
                ADD COLUMN IF NOT EXISTS max_uses INT
            """)
            await conn.execute("""
                ALTER TABLE rvb_template_keys
                ADD COLUMN IF NOT EXISTS uses_count INT NOT NULL DEFAULT 0
            """)
            await conn.execute("""
                ALTER TABLE rvb_template_keys
                ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ
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
            # Авто-миграция: режим источника карточек и список разрешённых своих
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS card_source_mode TEXT NOT NULL DEFAULT 'standard'
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS allowed_custom_ids JSONB NOT NULL DEFAULT '[]'
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS inline_button_mode TEXT NOT NULL DEFAULT 'shown'
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS inline_template_id TEXT
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS inline_button_show BOOLEAN NOT NULL DEFAULT TRUE
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS inline_notify_seller BOOLEAN NOT NULL DEFAULT FALSE
            """)
            # Авто-миграция: анонимные отзывы
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS anon_mode TEXT NOT NULL DEFAULT 'off'
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS anon_nickname TEXT NOT NULL DEFAULT 'Анонимный покупатель'
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS anon_avatar TEXT
            """)
            await conn.execute("""
                ALTER TABLE rvb_sellers
                ADD COLUMN IF NOT EXISTS inline_anon BOOLEAN NOT NULL DEFAULT FALSE
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
            await conn.execute("""
                ALTER TABLE rvb_reviews
                ADD COLUMN IF NOT EXISTS show_buyer_button BOOLEAN NOT NULL DEFAULT TRUE
            """)
            await conn.execute("""
                ALTER TABLE rvb_reviews
                ADD COLUMN IF NOT EXISTS proof_count INT NOT NULL DEFAULT 0
            """)
            await conn.execute("""
                ALTER TABLE rvb_reviews
                ADD COLUMN IF NOT EXISTS is_anonymous BOOLEAN NOT NULL DEFAULT FALSE
            """)
            # Авто-миграция: уникальный код проверки подлинности отзыва
            await conn.execute("""
                ALTER TABLE rvb_reviews
                ADD COLUMN IF NOT EXISTS verify_code TEXT UNIQUE
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS rvb_reviews_verify_idx
                ON rvb_reviews(verify_code)
            """)
            # Бэкфилл кодов для старых отзывов
            old_rows = await conn.fetch(
                "SELECT id FROM rvb_reviews WHERE verify_code IS NULL")
            for _r in old_rows:
                for _attempt in range(5):
                    try:
                        await conn.execute(
                            "UPDATE rvb_reviews SET verify_code = $1 WHERE id = $2",
                            _gen_verify_code(), _r["id"])
                        break
                    except Exception:
                        continue
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
            # Бэкфилл: добавляем существующих продавцов и покупателей в список пользователей,
            # чтобы рассылка охватила тех, кто пользовался ботом до появления учёта.
            await conn.execute("""
                INSERT INTO rvb_bot_users (id, username, first_seen, last_seen)
                SELECT id, username, created_at, created_at FROM rvb_sellers
                ON CONFLICT (id) DO NOTHING
            """)
            await conn.execute("""
                INSERT INTO rvb_bot_users (id, username, first_seen, last_seen)
                SELECT DISTINCT buyer_id, buyer_username, MIN(created_at), MAX(created_at)
                FROM rvb_reviews
                GROUP BY buyer_id, buyer_username
                ON CONFLICT (id) DO NOTHING
            """)
            # Авто-миграция: таблицы настроек и закрепов рекламы
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rvb_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rvb_ad_pins (
                    user_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    pinned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, message_id)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rvb_banned_users (
                    id BIGINT PRIMARY KEY,
                    username TEXT,
                    banned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    notified BOOLEAN NOT NULL DEFAULT FALSE
                )
            """)
            await conn.execute("""
                ALTER TABLE rvb_banned_users
                ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ
            """)
            await conn.execute("""
                ALTER TABLE rvb_banned_users
                ADD COLUMN IF NOT EXISTS reason TEXT
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rvb_admin_log (
                    id SERIAL PRIMARY KEY,
                    action TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # Авто-миграция: нумерация отзывов в канале
            await conn.execute("""
                ALTER TABLE rvb_seller_channels
                ADD COLUMN IF NOT EXISTS numbering_mode TEXT NOT NULL DEFAULT 'off'
            """)
            await conn.execute("""
                ALTER TABLE rvb_seller_channels
                ADD COLUMN IF NOT EXISTS numbering_start INT NOT NULL DEFAULT 1
            """)
            await conn.execute("""
                ALTER TABLE rvb_seller_channels
                ADD COLUMN IF NOT EXISTS numbering_template TEXT NOT NULL DEFAULT 'Отзыв №{n}'
            """)
            await conn.execute("""
                ALTER TABLE rvb_seller_channels
                ADD COLUMN IF NOT EXISTS numbering_entities TEXT
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rvb_review_numbers (
                    id SERIAL PRIMARY KEY,
                    seller_id BIGINT NOT NULL,
                    review_id INT,
                    number INT NOT NULL,
                    channel_msg_id BIGINT,
                    number_msg_id BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS rvb_review_numbers_seller_idx
                ON rvb_review_numbers(seller_id, number)
            """)
        logger.info("Database initialized")

    async def close(self):
        if self.pool:
            await self.pool.close()

    # ── Custom Templates (конструктор) ────────────────────────────────────

    async def create_custom_template(self, owner_id: int, creator_id: int,
                                       creator_username, **fields) -> dict:
        import json as _json
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO rvb_custom_templates
                    (owner_id, creator_id, creator_username, name, layout, font,
                     title_font, text_color, accent_color, bg_color, bg_image, is_edited, extra_cfg)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
                fields.get("is_edited", False),
                _json.dumps(fields.get("extra_cfg", {})))
            d = dict(row)
            # lineage_id: либо наследуем (для копий), либо ставим равным своему id (для оригиналов)
            lineage = fields.get("lineage_id") or d["id"]
            await conn.execute(
                "UPDATE rvb_custom_templates SET lineage_id = $1 WHERE id = $2",
                lineage, d["id"])
            d["lineage_id"] = lineage
            return d

    async def get_custom_template(self, template_id: int):
        import json as _json
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_custom_templates WHERE id = $1", template_id)
            if not row:
                return None
            d = dict(row)
            if isinstance(d.get("extra_cfg"), str):
                try:
                    d["extra_cfg"] = _json.loads(d["extra_cfg"])
                except Exception:
                    d["extra_cfg"] = {}
            elif d.get("extra_cfg") is None:
                d["extra_cfg"] = {}
            return d

    async def list_custom_templates(self, owner_id: int) -> list:
        import json as _json
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM rvb_custom_templates WHERE owner_id = $1 ORDER BY created_at DESC",
                owner_id)
            result = []
            for r in rows:
                d = dict(r)
                if isinstance(d.get("extra_cfg"), str):
                    try:
                        d["extra_cfg"] = _json.loads(d["extra_cfg"])
                    except Exception:
                        d["extra_cfg"] = {}
                elif d.get("extra_cfg") is None:
                    d["extra_cfg"] = {}
                result.append(d)
            return result

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
        import json as _json
        if not fields:
            return
        sets, vals = [], []
        for i, (k, v) in enumerate(fields.items(), start=1):
            if k == "extra_cfg" and not isinstance(v, str):
                v = _json.dumps(v)
            sets.append(f"{k} = ${i}")
            vals.append(v)
        vals.append(template_id)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE rvb_custom_templates SET {', '.join(sets)}, updated_at=NOW() "
                f"WHERE id = ${len(vals)} RETURNING *", *vals)
            if not row:
                return None
            d = dict(row)
            if isinstance(d.get("extra_cfg"), str):
                try:
                    d["extra_cfg"] = _json.loads(d["extra_cfg"])
                except Exception:
                    d["extra_cfg"] = {}
            return d

    async def delete_custom_template(self, template_id: int, owner_id: int) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM rvb_custom_templates WHERE id = $1 AND owner_id = $2",
                template_id, owner_id)
            return result.endswith("1")

    # ── Template Keys (шаринг) ────────────────────────────────────────────

    async def create_template_key(self, key: str, template_id: int,
                                   key_type: str = "copy",
                                   max_uses: Optional[int] = None,
                                   expires_days: Optional[int] = None):
        """Создаёт ключ. max_uses=None — без лимита; expires_days=None — бессрочно."""
        expires_at = None
        if expires_days:
            # Срок считаем от текущего момента: дни × 24 часа
            import datetime as _dt
            expires_at = _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=expires_days)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_template_keys
                    (key, template_id, key_type, max_uses, uses_count, expires_at)
                VALUES ($1, $2, $3, $4, 0, $5)
            """, key, template_id, key_type, max_uses, expires_at)

    async def get_template_key(self, key: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_template_keys WHERE key = $1", key)
            return dict(row) if row else None

    def _key_status(self, key_row: dict) -> tuple:
        """Проверяет валидность ключа. Возвращает (ok: bool, reason: str)."""
        import datetime as _dt
        # Срок
        exp = key_row.get("expires_at")
        if exp is not None:
            now = _dt.datetime.now(_dt.timezone.utc)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=_dt.timezone.utc)
            if now > exp:
                return False, "expired"
        # Лимит активаций
        max_uses = key_row.get("max_uses")
        if max_uses is not None and key_row.get("uses_count", 0) >= max_uses:
            return False, "exhausted"
        return True, "ok"

    async def consume_template_key(self, key: str, used_by: int) -> tuple:
        """Атомарно использует одну активацию ключа с проверкой лимита и срока.
        Возвращает (ok, reason)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT * FROM rvb_template_keys WHERE key = $1 FOR UPDATE", key)
                if not row:
                    return False, "not_found"
                kr = dict(row)
                ok, reason = self._key_status(kr)
                if not ok:
                    return False, reason
                await conn.execute("""
                    UPDATE rvb_template_keys
                    SET uses_count = uses_count + 1,
                        used = TRUE,
                        used_by = $1
                    WHERE key = $2
                """, used_by, key)
                return True, "ok"

    async def list_template_keys(self, template_id: int) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM rvb_template_keys WHERE template_id = $1 ORDER BY created_at DESC",
                template_id)
            return [dict(r) for r in rows]

    async def delete_template_key(self, key: str, owner_id: int) -> bool:
        """Удаляет ключ, только если карточка принадлежит owner_id."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM rvb_template_keys k
                USING rvb_custom_templates t
                WHERE k.key = $1 AND k.template_id = t.id AND t.owner_id = $2
            """, key, owner_id)
            return result.endswith("1")

    async def get_user_template_by_lineage(self, user_id: int, lineage_id: int):
        """Находит копию шаблона данной линии у конкретного пользователя."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_custom_templates WHERE owner_id = $1 AND lineage_id = $2 LIMIT 1",
                user_id, lineage_id)
            return dict(row) if row else None

    async def has_transfer_key(self, template_id: int) -> bool:
        """Есть ли уже активный (неиспользованный) ключ передачи авторства."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchval("""
                SELECT 1 FROM rvb_template_keys
                WHERE template_id = $1 AND key_type = 'transfer' AND used = FALSE
                LIMIT 1
            """, template_id)
            return bool(row)

    async def transfer_authorship(self, lineage_id: int, old_owner_id: int,
                                   new_owner_id: int, new_username) -> bool:
        """Передаёт авторство: новый владелец становится создателем своей копии,
        у старого карточка удаляется, у всех остальных копий обновляется водяной знак."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Обновляем creator во ВСЕХ копиях этой линии (динамический водяной знак)
                await conn.execute("""
                    UPDATE rvb_custom_templates
                    SET creator_id = $1, creator_username = $2, is_edited = FALSE
                    WHERE lineage_id = $3
                """, new_owner_id, new_username, lineage_id)
                # У нового владельца копия становится «созданной» (owner=creator)
                await conn.execute("""
                    UPDATE rvb_custom_templates
                    SET owner_id = $1
                    WHERE lineage_id = $2 AND owner_id = $1
                """, new_owner_id, lineage_id)
                # Удаляем карточку у старого владельца
                await conn.execute("""
                    DELETE FROM rvb_custom_templates
                    WHERE lineage_id = $1 AND owner_id = $2
                """, lineage_id, old_owner_id)
                return True

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

    @staticmethod
    def _parse_seller(row) -> Optional[dict]:
        if not row:
            return None
        import json as _json
        d = dict(row)
        v = d.get("allowed_custom_ids")
        if isinstance(v, str):
            try:
                d["allowed_custom_ids"] = _json.loads(v)
            except Exception:
                d["allowed_custom_ids"] = []
        elif v is None:
            d["allowed_custom_ids"] = []
        return d

    async def get_seller(self, user_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_sellers WHERE id = $1", user_id
            )
            return self._parse_seller(row)

    async def get_seller_by_pubid(self, pub_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_sellers WHERE pub_id = $1", pub_id
            )
            return self._parse_seller(row)

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

    # Разрешённые поля для update_seller — защита от случайного/вредного апдейта
    _ALLOWED_SELLER_FIELDS = {
        "shop_name", "username", "template_id", "stars_mode", "stars_value",
        "item_mode", "item_value", "pub_id", "card_source_mode", "allowed_custom_ids",
        "inline_button_mode", "inline_template_id", "inline_button_show",
        "inline_notify_seller", "show_buyer_button", "allow_template_choice",
        "anon_mode", "anon_nickname", "anon_avatar", "inline_anon",
    }

    async def update_seller(self, user_id: int, **fields) -> Optional[dict]:
        if not fields:
            return await self.get_seller(user_id)
        # Отсекаем любые поля вне whitelist
        bad = set(fields) - self._ALLOWED_SELLER_FIELDS
        if bad:
            raise ValueError(f"Недопустимые поля update_seller: {bad}")
        import json as _json
        set_parts = []
        values = []
        for i, (k, v) in enumerate(fields.items(), start=1):
            if k == "allowed_custom_ids" and not isinstance(v, str):
                v = _json.dumps(v)
            set_parts.append(f"{k} = ${i}")
            values.append(v)
        values.append(user_id)
        query = f"UPDATE rvb_sellers SET {', '.join(set_parts)}, updated_at = NOW() WHERE id = ${len(values)} RETURNING *"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *values)
            return self._parse_seller(row)

    async def save_review(
        self, seller_id: int, buyer_id: int, buyer_name: str,
        buyer_username: Optional[str], review_text: str,
        item_bought: str, stars: int, template_used: str,
        card_file_id: Optional[str] = None, show_buyer_button: bool = True,
        proof_count: int = 0, verify_code: Optional[str] = None,
        is_anonymous: bool = False
    ) -> dict:
        async with self.pool.acquire() as conn:
            last_err = None
            for _ in range(3):
                code = verify_code or _gen_verify_code()
                try:
                    row = await conn.fetchrow("""
                        INSERT INTO rvb_reviews
                            (seller_id, buyer_id, buyer_name, buyer_username,
                             review_text, item_bought, stars, template_used, card_file_id,
                             show_buyer_button, proof_count, verify_code, is_anonymous)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                        RETURNING *
                    """, seller_id, buyer_id, buyer_name, buyer_username,
                        review_text, item_bought, stars, template_used, card_file_id,
                        show_buyer_button, proof_count, code, is_anonymous)
                    return dict(row)
                except Exception as e:
                    # Коллизия кода (вероятность ~0) — регенерируем и пробуем ещё
                    last_err = e
                    verify_code = None
            raise last_err

    async def reserve_verify_code(self) -> str:
        """Генерирует код, свободный на момент проверки (для печати на карточке ДО сохранения)."""
        async with self.pool.acquire() as conn:
            for _ in range(5):
                code = _gen_verify_code()
                exists = await conn.fetchval(
                    "SELECT 1 FROM rvb_reviews WHERE verify_code = $1", code)
                if not exists:
                    return code
            return _gen_verify_code()

    async def get_review_by_code(self, code: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_reviews WHERE verify_code = $1", code.upper())
            return dict(row) if row else None

    async def get_shop_stats(self, seller_id: int) -> dict:
        """Публичная статистика продавца — источник правды против накрутки."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(DISTINCT buyer_id) AS unique_buyers,
                       ROUND(AVG(stars)::numeric, 1) AS avg_stars,
                       MIN(created_at) AS first_at,
                       MAX(created_at) AS last_at
                FROM rvb_reviews WHERE seller_id = $1
            """, seller_id)
            return dict(row) if row else {"total": 0, "unique_buyers": 0,
                                          "avg_stars": None, "first_at": None, "last_at": None}

    async def has_recent_review(self, buyer_id: int, seller_id: int, hours: int = 6) -> bool:
        """Дедуп: покупатель уже оставлял отзыв этому продавцу за последние N часов."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchval("""
                SELECT 1 FROM rvb_reviews
                WHERE buyer_id = $1 AND seller_id = $2
                  AND created_at > NOW() - ($3 || ' hours')::interval
                LIMIT 1
            """, buyer_id, seller_id, str(hours))
            return bool(row)

    async def count_reviews_24h(self, buyer_id: int) -> int:
        """Сколько отзывов оставил аккаунт за сутки (по всем продавцам)."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM rvb_reviews
                WHERE buyer_id = $1 AND created_at > NOW() - INTERVAL '24 hours'
            """, buyer_id) or 0

    async def seller_review_burst(self, seller_id: int) -> tuple:
        """Возвращает (за_час, за_сутки) число отзывов продавцу — для детектора накрутки."""
        async with self.pool.acquire() as conn:
            hour = await conn.fetchval("""
                SELECT COUNT(*) FROM rvb_reviews
                WHERE seller_id = $1 AND created_at > NOW() - INTERVAL '1 hour'
            """, seller_id) or 0
            day = await conn.fetchval("""
                SELECT COUNT(*) FROM rvb_reviews
                WHERE seller_id = $1 AND created_at > NOW() - INTERVAL '24 hours'
            """, seller_id) or 0
            return hour, day

    async def get_setting(self, key: str) -> Optional[str]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT value FROM rvb_settings WHERE key = $1", key)

    async def set_setting(self, key: str, value: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_settings (key, value) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, key, value)

    # ── Журнал действий админа (append-only) ───────────────────────────────

    async def log_admin_action(self, action: str, details: str = ""):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO rvb_admin_log (action, details) VALUES ($1, $2)",
                action, details)

    async def get_admin_log(self, limit: int = 20) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT action, details, created_at FROM rvb_admin_log "
                "ORDER BY created_at DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

    async def get_review(self, review_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_reviews WHERE id = $1", review_id)
            return dict(row) if row else None

    async def update_review_card(self, review_id: int, card_file_id: str):
        """Обновляет file_id карточки (после редактирования пруфа)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_reviews SET card_file_id = $1 WHERE id = $2",
                card_file_id, review_id)

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

    # ── Нумерация отзывов в канале ────────────────────────────────────────

    async def set_numbering(self, seller_id: int, **fields):
        """Обновляет настройки нумерации (mode/start/template)."""
        allowed = {"numbering_mode", "numbering_start", "numbering_template", "numbering_entities"}
        bad = set(fields) - allowed
        if bad:
            raise ValueError(f"Недопустимые поля нумерации: {bad}")
        if not fields:
            return
        sets, vals = [], []
        for i, (k, v) in enumerate(fields.items(), start=1):
            sets.append(f"{k} = ${i}")
            vals.append(v)
        vals.append(seller_id)
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE rvb_seller_channels SET {', '.join(sets)} WHERE seller_id = ${len(vals)}",
                *vals)

    async def next_review_number(self, seller_id: int) -> int:
        """Вычисляет номер для нового отзыва по «умной» модели:
        обычно max(number)+1, но если последний номер был удалён (запись помечена
        deleted или физически удалена) — переиспользует освободившийся хвост.
        Здесь просто: следующий = max(number)+1, старт = numbering_start при первом.
        Переиспользование последнего реализуется через release_last_number при удалении."""
        async with self.pool.acquire() as conn:
            ch = await conn.fetchrow(
                "SELECT numbering_start FROM rvb_seller_channels WHERE seller_id = $1",
                seller_id)
            start = (ch["numbering_start"] if ch else 1) or 1
            max_num = await conn.fetchval(
                "SELECT MAX(number) FROM rvb_review_numbers WHERE seller_id = $1",
                seller_id)
            if max_num is None:
                return start
            return max(max_num + 1, start)

    async def record_review_number(self, seller_id: int, review_id: int, number: int,
                                   channel_msg_id: int, number_msg_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_review_numbers
                    (seller_id, review_id, number, channel_msg_id, number_msg_id)
                VALUES ($1, $2, $3, $4, $5)
            """, seller_id, review_id, number, channel_msg_id, number_msg_id)

    async def get_review_number_by_review(self, review_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rvb_review_numbers WHERE review_id = $1", review_id)
            return dict(row) if row else None

    async def get_last_number_entry(self, seller_id: int) -> Optional[dict]:
        """Запись с самым большим номером — для проверки «умного последнего»."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM rvb_review_numbers
                WHERE seller_id = $1
                ORDER BY number DESC LIMIT 1
            """, seller_id)
            return dict(row) if row else None

    async def delete_number_entry(self, entry_id: int):
        """Удаляет запись о номере (когда последний номер освобождается)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM rvb_review_numbers WHERE id = $1", entry_id)

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
            total_users = await conn.fetchval("SELECT COUNT(*) FROM rvb_bot_users")
            users_7d = await conn.fetchval(
                "SELECT COUNT(*) FROM rvb_bot_users WHERE first_seen > NOW() - INTERVAL '7 days'"
            )
            custom_tpls = await conn.fetchval("SELECT COUNT(*) FROM rvb_custom_templates")
            channels = await conn.fetchval("SELECT COUNT(*) FROM rvb_seller_channels WHERE verified = TRUE")
            return {
                "sellers": sellers or 0,
                "reviews": reviews or 0,
                "reviews_7d": reviews_7d or 0,
                "buyers": buyers or 0,
                "total_users": total_users or 0,
                "users_7d": users_7d or 0,
                "custom_tpls": custom_tpls or 0,
                "channels": channels or 0,
            }

    # ── Учёт пользователей бота (для рассылки рекламы) ─────────────────────

    async def track_user(self, user_id: int, username: Optional[str] = None):
        """Регистрирует/обновляет пользователя бота (вызывается на /start)."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_bot_users (id, username, first_seen, last_seen, is_blocked)
                VALUES ($1, $2, NOW(), NOW(), FALSE)
                ON CONFLICT (id) DO UPDATE
                    SET last_seen = NOW(),
                        username = COALESCE(EXCLUDED.username, rvb_bot_users.username),
                        is_blocked = FALSE
            """, user_id, username)

    async def get_all_user_ids(self) -> list:
        """Все ID пользователей бота, кто не заблокировал бота."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM rvb_bot_users WHERE is_blocked = FALSE"
            )
            return [r["id"] for r in rows]

    async def mark_user_blocked(self, user_id: int):
        """Помечает пользователя как заблокировавшего бота (чтобы не слать ему)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_bot_users SET is_blocked = TRUE WHERE id = $1", user_id
            )

    async def count_bot_users(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM rvb_bot_users WHERE is_blocked = FALSE"
            ) or 0

    # ── Закрепы рекламы (для точного открепления и состояния кнопки) ───────

    async def record_ad_pin(self, user_id: int, message_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_ad_pins (user_id, message_id, pinned_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (user_id, message_id) DO NOTHING
            """, user_id, message_id)

    async def get_ad_pins(self) -> list:
        """Возвращает список (user_id, message_id) всех активных закрепов рекламы."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, message_id FROM rvb_ad_pins")
            return [(r["user_id"], r["message_id"]) for r in rows]

    async def clear_ad_pins(self):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM rvb_ad_pins")

    async def has_ad_pins(self) -> bool:
        async with self.pool.acquire() as conn:
            n = await conn.fetchval("SELECT COUNT(*) FROM rvb_ad_pins")
            return (n or 0) > 0

    # ── Топ продавцов ──────────────────────────────────────────────────────

    async def get_top_sellers(self, limit: int = 10) -> list:
        """Топ продавцов по количеству отзывов."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT s.id, s.username, s.shop_name, COUNT(r.id) AS review_count
                FROM rvb_sellers s
                JOIN rvb_reviews r ON r.seller_id = s.id
                GROUP BY s.id, s.username, s.shop_name
                ORDER BY review_count DESC
                LIMIT $1
            """, limit)
            return [dict(r) for r in rows]

    # ── Баны ───────────────────────────────────────────────────────────────

    async def ban_user(self, user_id: int, username: Optional[str] = None,
                       hours: Optional[int] = None, reason: Optional[str] = None):
        """Банит юзера. hours=None — навсегда (админ-бан); иначе временный авто-бан."""
        import datetime as _dt
        expires = None
        if hours:
            expires = _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(hours=hours)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO rvb_banned_users (id, username, banned_at, notified, expires_at, reason)
                VALUES ($1, $2, NOW(), FALSE, $3, $4)
                ON CONFLICT (id) DO UPDATE
                    SET username = COALESCE(EXCLUDED.username, rvb_banned_users.username),
                        banned_at = NOW(),
                        expires_at = EXCLUDED.expires_at,
                        reason = EXCLUDED.reason,
                        notified = FALSE
            """, user_id, username, expires, reason)

    async def unban_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM rvb_banned_users WHERE id = $1", user_id)

    async def is_banned(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT expires_at FROM rvb_banned_users WHERE id = $1", user_id)
            if not row:
                return False
            exp = row["expires_at"]
            if exp is not None:
                import datetime as _dt
                now = _dt.datetime.now(_dt.timezone.utc)
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=_dt.timezone.utc)
                if now > exp:
                    # Срок истёк — авто-разбан
                    await conn.execute("DELETE FROM rvb_banned_users WHERE id = $1", user_id)
                    return False
            return True

    async def was_ban_notified(self, user_id: int) -> bool:
        """Был ли уже отправлен забаненному ответ «вы забанены»."""
        async with self.pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT notified FROM rvb_banned_users WHERE id = $1", user_id
            ))

    async def mark_ban_notified(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rvb_banned_users SET notified = TRUE WHERE id = $1", user_id
            )

    async def get_banned_users(self) -> list:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, username, banned_at, expires_at, reason "
                "FROM rvb_banned_users ORDER BY banned_at DESC"
            )
            return [dict(r) for r in rows]

    async def find_user_id_by_username(self, username: str) -> Optional[int]:
        """Ищет ID пользователя по юзернейму среди известных боту (для бана по @username)."""
        uname = username.lstrip("@").lower()
        async with self.pool.acquire() as conn:
            # Ищем в пользователях бота, продавцах и покупателях отзывов
            row = await conn.fetchval(
                "SELECT id FROM rvb_bot_users WHERE LOWER(username) = $1 LIMIT 1", uname
            )
            if row:
                return row
            row = await conn.fetchval(
                "SELECT id FROM rvb_sellers WHERE LOWER(username) = $1 LIMIT 1", uname
            )
            if row:
                return row
            row = await conn.fetchval(
                "SELECT buyer_id FROM rvb_reviews WHERE LOWER(buyer_username) = $1 LIMIT 1", uname
            )
            return row

    async def get_seller_stats(self, seller_id: int) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as total,
                       ROUND(AVG(stars), 1) as avg_stars
                FROM rvb_reviews WHERE seller_id = $1
            """, seller_id)
            return dict(row) if row else {"total": 0, "avg_stars": 0}
