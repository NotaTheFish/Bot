-- Payment Terminal Bot — DB migrations
-- Prefix: pt_
-- Run once on fresh Railway PostgreSQL instance

CREATE TABLE IF NOT EXISTS pt_users (
    id          BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    role        TEXT CHECK (role IN ('main_admin', 'sub_admin', 'client')) NOT NULL DEFAULT 'client',
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pt_admins (
    id               BIGSERIAL PRIMARY KEY,
    user_id          BIGINT UNIQUE REFERENCES pt_users(id) ON DELETE CASCADE,
    is_main          BOOLEAN DEFAULT FALSE,
    aggregator       TEXT CHECK (aggregator IN ('lava', 'payok', 'freekassa')),
    aggregator_key   TEXT,   -- encrypted AES-256
    aggregator_data  JSONB,  -- shop_id, secret, etc.
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pt_deals (
    id                BIGSERIAL PRIMARY KEY,
    admin_id          BIGINT NOT NULL REFERENCES pt_admins(id),
    client_id         BIGINT REFERENCES pt_users(id),
    invite_token      TEXT UNIQUE NOT NULL,
    base_price_usd    NUMERIC(10,2) NOT NULL,
    selected_currency TEXT,
    status            TEXT CHECK (status IN (
        'CREATED', 'WAITING_PAYMENT', 'PAID', 'CLOSED', 'EXPIRED'
    )) NOT NULL DEFAULT 'CREATED',
    expires_at        TIMESTAMP NOT NULL,
    closed_at         TIMESTAMP,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pt_deals_status ON pt_deals(status);
CREATE INDEX IF NOT EXISTS idx_pt_deals_invite_token ON pt_deals(invite_token);
CREATE INDEX IF NOT EXISTS idx_pt_deals_admin_id ON pt_deals(admin_id);
CREATE INDEX IF NOT EXISTS idx_pt_deals_expires_at ON pt_deals(expires_at);

CREATE TABLE IF NOT EXISTS pt_payments (
    id                    BIGSERIAL PRIMARY KEY,
    deal_id               BIGINT NOT NULL REFERENCES pt_deals(id),
    provider              TEXT CHECK (provider IN ('lava', 'payok', 'freekassa')) NOT NULL,
    amount_original       NUMERIC(12,2),
    currency              TEXT,
    amount_usd_equivalent NUMERIC(12,2),
    external_tx_id        TEXT UNIQUE,
    idempotency_key       TEXT UNIQUE NOT NULL,
    status                TEXT CHECK (status IN ('pending', 'confirmed', 'failed')) NOT NULL DEFAULT 'pending',
    created_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pt_payments_deal_id ON pt_payments(deal_id);
CREATE INDEX IF NOT EXISTS idx_pt_payments_external_tx_id ON pt_payments(external_tx_id);

CREATE TABLE IF NOT EXISTS pt_currency_rates (
    currency   TEXT PRIMARY KEY,
    usd_rate   NUMERIC(12,6) NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO pt_currency_rates (currency, usd_rate) VALUES
    ('RUB', 90.0),
    ('UAH', 41.0),
    ('EUR', 0.92),
    ('USD', 1.0),
    ('KZT', 450.0),
    ('BYN', 3.25),
    ('MDL', 17.5)
ON CONFLICT (currency) DO NOTHING;

CREATE TABLE IF NOT EXISTS pt_admin_keys (
    id          BIGSERIAL PRIMARY KEY,
    key_token   TEXT UNIQUE NOT NULL,
    created_by  BIGINT NOT NULL REFERENCES pt_admins(id),
    used_by     BIGINT REFERENCES pt_users(id),
    used_at     TIMESTAMP,
    expires_at  TIMESTAMP NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pt_logs (
    id          BIGSERIAL PRIMARY KEY,
    deal_id     BIGINT REFERENCES pt_deals(id),
    admin_id    BIGINT REFERENCES pt_admins(id),
    event_type  TEXT NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pt_logs_deal_id ON pt_logs(deal_id);
CREATE INDEX IF NOT EXISTS idx_pt_logs_event_type ON pt_logs(event_type);
