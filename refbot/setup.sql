-- ============================================================
-- refbot — ЕДИНЫЙ файл установки. Больше ничего накатывать не надо.
--
-- Railway → Postgres → Console → Upload этого файла → затем:
--     psql -U postgres -d railway -f /setup.sql
--
-- Идемпотентен: гоняй сколько угодно раз. Ничего не удаляет, данные не трогает.
-- Подходит и для чистой базы, и поверх уже накатанной схемы.
-- ============================================================
-- ---------- 1. Типы ----------
DO $$ BEGIN
    CREATE TYPE rb_currency AS ENUM ('mushrooms', 'coins');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE rb_ref_status AS ENUM ('hold', 'paid', 'void');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE rb_wd_status AS ENUM ('pending', 'confirmed', 'cancelled', 'rejected');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;


-- ---------- 2. Таблицы ----------
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


-- ---------- 3. Индексы (без них защита от накрутки не работает) ----------
CREATE INDEX        IF NOT EXISTS rb_ledger_user_idx      ON rb_ledger (tg_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS rb_referrals_alive_idx  ON rb_referrals (chat_id, invitee_id) WHERE status IN ('hold','paid');
CREATE INDEX        IF NOT EXISTS rb_referrals_due_idx    ON rb_referrals (hold_until) WHERE status = 'hold';
CREATE INDEX        IF NOT EXISTS rb_referrals_inviter_idx ON rb_referrals (inviter_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS rb_withdrawals_one_pending ON rb_withdrawals (tg_id) WHERE status = 'pending';
CREATE INDEX        IF NOT EXISTS rb_withdrawals_user_idx ON rb_withdrawals (tg_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS rb_spins_daily          ON rb_spins (tg_id, spin_day);
CREATE INDEX        IF NOT EXISTS rb_audit_idx            ON rb_audit (action, created_at DESC);



-- ---------- 4. Кастомизация и отключение чатов ----------
-- настройки: эмодзи, названия, шаблон профиля
CREATE TABLE IF NOT EXISTS rb_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_by BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- отключение чата: прогресс сохраняется целиком
-- active уже был. Добавляем, кто и когда отключил — чтобы потом не гадать.
ALTER TABLE rb_withdrawals ADD COLUMN IF NOT EXISTS admin_cards JSONB;
ALTER TABLE rb_chats ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMPTZ;
ALTER TABLE rb_chats ADD COLUMN IF NOT EXISTS deactivated_by BIGINT;


-- ---------- 4b. Свободная рулетка (чаты без /bind) ----------
-- Отдельно от rb_chats СПЕЦИАЛЬНО: иначе случайные чаты полезут в «Мою ссылку»,
-- в маршрутизацию выводов и в список чатов админки.

-- глобальный суточный потолок выплат по всем непривязанным чатам
CREATE TABLE IF NOT EXISTS rb_free_budget (
    day        DATE PRIMARY KEY,
    spent_mush BIGINT NOT NULL DEFAULT 0
);

-- реестр чатов, где крутили без привязки: для видимости и блокировки
CREATE TABLE IF NOT EXISTS rb_free_chats (
    chat_id    BIGINT PRIMARY KEY,
    title      TEXT,
    blocked    BOOLEAN NOT NULL DEFAULT FALSE,
    spins      INT NOT NULL DEFAULT 0,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- 4c. Еженедельный конкурс по активности ----------
-- Отдельно от rb_chats: /шимшайнуть не должен привязывать чат к рефералке.

CREATE TABLE IF NOT EXISTS rb_contest_chats (
    chat_id        BIGINT PRIMARY KEY,
    title          TEXT,
    owner_id       BIGINT NOT NULL,
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    pinned_msg_id  BIGINT,          -- СВОЙ последний закреп, чужие не трогаем
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at TIMESTAMPTZ,
    deactivated_by BIGINT
);

-- Счётчики сообщений. Ключ — начало периода, поэтому НИЧЕГО не обнуляется:
-- новая неделя = новая строка. Старый период лежит нетронутым, пока админ
-- не проведёт розыгрыш (хоть через месяц).
CREATE TABLE IF NOT EXISTS rb_week_msgs (
    chat_id      BIGINT      NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    user_id      BIGINT      NOT NULL,
    msgs         INT         NOT NULL DEFAULT 0,
    last_msg_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, period_start, user_id)
);
CREATE INDEX IF NOT EXISTS rb_week_msgs_period_idx ON rb_week_msgs (chat_id, period_start);

CREATE TABLE IF NOT EXISTS rb_week_draws (
    id            BIGSERIAL PRIMARY KEY,
    chat_id       BIGINT      NOT NULL,
    period_start  TIMESTAMPTZ NOT NULL,
    period_end    TIMESTAMPTZ NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending',  -- pending | drawn | empty
    announce_msg_id BIGINT,
    winner_id     BIGINT,
    winner_tickets INT,
    tickets_total INT NOT NULL DEFAULT 0,
    players       INT NOT NULL DEFAULT 0,
    currency      rb_currency,
    amount        BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    drawn_at      TIMESTAMPTZ,
    drawn_by      BIGINT
);
-- один розыгрыш на (чат, период) — гарантия от двойной выплаты на уровне БД
CREATE UNIQUE INDEX IF NOT EXISTS rb_week_draws_uniq ON rb_week_draws (chat_id, period_start);

-- ---------- 4d. Промокод (тест системы вывода) ----------
-- Промокод задаётся переменной PROMO_CODE. БЕЗЛИМИТНЫЙ — тестовый инструмент,
-- активировать можно сколько угодно раз. Sequence даёт уникальный ключ каждому
-- начислению, чтобы идемпотентность db.apply не блокировала повторы.
CREATE SEQUENCE IF NOT EXISTS rb_promo_seq;

-- ---------- 5. Проверка ----------
SELECT
  (SELECT count(*) FROM pg_tables WHERE tablename ~ '^rb_')                   AS tables_expect_18,
  (SELECT count(*) FROM pg_type   WHERE typname ~ '^rb_' AND typtype = 'e')   AS enums_expect_3,
  (SELECT count(*) FROM pg_indexes WHERE indexname IN
     ('rb_referrals_alive_idx','rb_withdrawals_one_pending','rb_spins_daily',
      'rb_week_draws_uniq')) AS guards_expect_4,
  (SELECT count(*) FROM information_schema.columns
     WHERE table_name='rb_chats' AND column_name IN
       ('deactivated_at','deactivated_by'))                                    AS newcols_expect_2,
  current_database() AS db;
