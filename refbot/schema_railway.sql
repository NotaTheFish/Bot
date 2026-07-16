-- ============================================================
-- refbot — схема для вставки в Railway → Postgres → Data → Query
-- Идемпотентна: можно вставлять повторно, ничего не сломается и не сотрётся.
-- Если панель ругается на длину — вставляй по блокам (они помечены).
-- ============================================================

-- ---------- БЛОК 1: типы ----------
DO $$ BEGIN
    CREATE TYPE rb_currency AS ENUM ('mushrooms', 'coins');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE rb_ref_status AS ENUM ('hold', 'paid', 'void');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE rb_wd_status AS ENUM ('pending', 'confirmed', 'cancelled', 'rejected');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


-- ---------- БЛОК 2: таблицы ----------
CREATE TABLE IF NOT EXISTS rb_users (
    tg_id      BIGINT PRIMARY KEY,
    username   TEXT,
    first_name TEXT,
    currency   rb_currency NOT NULL DEFAULT 'mushrooms',
    banned     BOOLEAN     NOT NULL DEFAULT FALSE,
    ban_reason TEXT,
    banned_by  BIGINT,
    banned_at  TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rb_chats (
    chat_id              BIGINT PRIMARY KEY,
    title                TEXT,
    owner_id             BIGINT  NOT NULL,
    active               BOOLEAN NOT NULL DEFAULT TRUE,
    reward_mushrooms     BIGINT  NOT NULL DEFAULT 5000,
    reward_coins         BIGINT  NOT NULL DEFAULT 100000,
    hold_hours           INT     NOT NULL DEFAULT 72,
    daily_budget_mush    BIGINT  NOT NULL DEFAULT 500000,
    budget_date          DATE    NOT NULL DEFAULT CURRENT_DATE,
    budget_spent_mush    BIGINT  NOT NULL DEFAULT 0,
    max_refs_per_day     INT     NOT NULL DEFAULT 15,
    min_account_age_days INT     NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rb_admins (
    chat_id  BIGINT NOT NULL REFERENCES rb_chats(chat_id) ON DELETE CASCADE,
    tg_id    BIGINT NOT NULL,
    role     TEXT   NOT NULL DEFAULT 'admin',
    added_by BIGINT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, tg_id)
);

CREATE TABLE IF NOT EXISTS rb_balances (
    tg_id    BIGINT NOT NULL REFERENCES rb_users(tg_id) ON DELETE CASCADE,
    currency rb_currency NOT NULL,
    amount   BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
    PRIMARY KEY (tg_id, currency)
);

CREATE TABLE IF NOT EXISTS rb_ledger (
    id              BIGSERIAL PRIMARY KEY,
    tg_id           BIGINT NOT NULL,
    currency        rb_currency NOT NULL,
    delta           BIGINT NOT NULL,
    balance_after   BIGINT NOT NULL,
    reason          TEXT   NOT NULL,
    ref_id          BIGINT,
    idempotency_key TEXT   NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rb_ref_links (
    code       TEXT PRIMARY KEY,
    chat_id    BIGINT NOT NULL REFERENCES rb_chats(chat_id) ON DELETE CASCADE,
    inviter_id BIGINT NOT NULL REFERENCES rb_users(tg_id)   ON DELETE CASCADE,
    clicks     INT    NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chat_id, inviter_id)
);

CREATE TABLE IF NOT EXISTS rb_invites (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    inviter_id  BIGINT NOT NULL,
    visitor_id  BIGINT NOT NULL,
    invite_name TEXT   NOT NULL UNIQUE,
    invite_url  TEXT   NOT NULL,
    used_by     BIGINT,
    used_at     TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chat_id, visitor_id)
);

CREATE TABLE IF NOT EXISTS rb_targets (
    chat_id          BIGINT NOT NULL,
    user_id          BIGINT NOT NULL,
    first_inviter_id BIGINT NOT NULL,
    burned           BOOLEAN NOT NULL DEFAULT FALSE,
    burned_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS rb_referrals (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    inviter_id  BIGINT NOT NULL,
    invitee_id  BIGINT NOT NULL,
    currency    rb_currency   NOT NULL,
    amount      BIGINT        NOT NULL,
    status      rb_ref_status NOT NULL DEFAULT 'hold',
    joined_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    hold_until  TIMESTAMPTZ   NOT NULL,
    credited_at TIMESTAMPTZ,
    voided_at   TIMESTAMPTZ,
    flagged     BOOLEAN NOT NULL DEFAULT FALSE,
    flag_reason TEXT
);

CREATE TABLE IF NOT EXISTS rb_withdrawals (
    id            BIGSERIAL PRIMARY KEY,
    tg_id         BIGINT NOT NULL,
    chat_id       BIGINT NOT NULL,
    currency      rb_currency  NOT NULL,
    amount        BIGINT       NOT NULL CHECK (amount > 0),
    status        rb_wd_status NOT NULL DEFAULT 'pending',
    version       INT NOT NULL DEFAULT 1,
    admin_chat_id BIGINT,
    admin_msg_id  BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at    TIMESTAMPTZ,
    decided_by    BIGINT,
    comment       TEXT
);

CREATE TABLE IF NOT EXISTS rb_spins (
    id         BIGSERIAL PRIMARY KEY,
    tg_id      BIGINT NOT NULL,
    chat_id    BIGINT NOT NULL,
    currency   rb_currency NOT NULL,
    amount     BIGINT NOT NULL,
    spin_day   DATE   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rb_audit (
    id         BIGSERIAL PRIMARY KEY,
    actor_id   BIGINT,
    action     TEXT NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------- БЛОК 3: индексы (без них защита от накрутки не работает) ----------
CREATE INDEX        IF NOT EXISTS rb_ledger_user_idx      ON rb_ledger (tg_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS rb_referrals_alive_idx  ON rb_referrals (chat_id, invitee_id) WHERE status IN ('hold','paid');
CREATE INDEX        IF NOT EXISTS rb_referrals_due_idx    ON rb_referrals (hold_until) WHERE status = 'hold';
CREATE INDEX        IF NOT EXISTS rb_referrals_inviter_idx ON rb_referrals (inviter_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS rb_withdrawals_one_pending ON rb_withdrawals (tg_id) WHERE status = 'pending';
CREATE INDEX        IF NOT EXISTS rb_withdrawals_user_idx ON rb_withdrawals (tg_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS rb_spins_daily          ON rb_spins (tg_id, spin_day);
CREATE INDEX        IF NOT EXISTS rb_audit_idx            ON rb_audit (action, created_at DESC);


-- ---------- БЛОК 4: проверка ----------
SELECT
  (SELECT count(*) FROM pg_tables WHERE tablename ~ '^rb_')                  AS tables_expect_12,
  (SELECT count(*) FROM pg_type   WHERE typname ~ '^rb_' AND typtype = 'e')  AS enums_expect_3,
  (SELECT count(*) FROM pg_indexes WHERE indexname IN
     ('rb_referrals_alive_idx','rb_withdrawals_one_pending','rb_spins_daily')) AS guards_expect_3,
  current_database() AS db;
