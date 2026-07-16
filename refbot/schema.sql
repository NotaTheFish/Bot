-- ============================================================
-- refbot — схема. Префикс rb_
-- Принципы:
--   1. Баланс НИКОГДА не меняется напрямую — только через rb_ledger.
--   2. Каждая проводка имеет idempotency_key -> двойное начисление невозможно.
--   3. CHECK (amount >= 0) на балансе -> уйти в минус нельзя даже при баге.
--   4. Привязка реферала к пригласившему вечная (rb_targets).
-- ============================================================

CREATE TYPE rb_currency   AS ENUM ('mushrooms', 'coins');
CREATE TYPE rb_ref_status AS ENUM ('hold', 'paid', 'void');
CREATE TYPE rb_wd_status  AS ENUM ('pending', 'confirmed', 'cancelled', 'rejected');

-- ---------- пользователи ----------
CREATE TABLE rb_users (
    tg_id        BIGINT PRIMARY KEY,
    username     TEXT,
    first_name   TEXT,
    currency     rb_currency NOT NULL DEFAULT 'mushrooms',
    banned       BOOLEAN     NOT NULL DEFAULT FALSE,
    ban_reason   TEXT,
    banned_by    BIGINT,
    banned_at    TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- чаты, которые пиарим ----------
CREATE TABLE rb_chats (
    chat_id            BIGINT PRIMARY KEY,
    title              TEXT,
    owner_id           BIGINT NOT NULL,              -- главный админ, отвечает за выплаты
    active             BOOLEAN NOT NULL DEFAULT TRUE,
    reward_mushrooms   BIGINT NOT NULL DEFAULT 5000,
    reward_coins       BIGINT NOT NULL DEFAULT 100000,
    hold_hours         INT    NOT NULL DEFAULT 72,   -- 3 дня
    -- предохранители от слива бюджета
    daily_budget_mush  BIGINT NOT NULL DEFAULT 500000,  -- потолок начислений в сутки (в грибах, коины считаются /20)
    budget_date        DATE   NOT NULL DEFAULT CURRENT_DATE,
    budget_spent_mush  BIGINT NOT NULL DEFAULT 0,
    max_refs_per_day   INT    NOT NULL DEFAULT 15,      -- потолок рефералов на одного юзера в сутки
    min_account_age_days INT  NOT NULL DEFAULT 0,       -- 0 = выкл (эвристика по tg_id)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE rb_admins (
    chat_id  BIGINT NOT NULL REFERENCES rb_chats(chat_id) ON DELETE CASCADE,
    tg_id    BIGINT NOT NULL,
    role     TEXT   NOT NULL DEFAULT 'admin',   -- 'owner' | 'admin'
    added_by BIGINT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, tg_id)
);

-- ---------- деньги ----------
CREATE TABLE rb_balances (
    tg_id    BIGINT NOT NULL REFERENCES rb_users(tg_id) ON DELETE CASCADE,
    currency rb_currency NOT NULL,
    amount   BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
    PRIMARY KEY (tg_id, currency)
);

CREATE TABLE rb_ledger (
    id              BIGSERIAL PRIMARY KEY,
    tg_id           BIGINT NOT NULL,
    currency        rb_currency NOT NULL,
    delta           BIGINT NOT NULL,
    balance_after   BIGINT NOT NULL,
    reason          TEXT   NOT NULL,   -- referral | roulette | withdraw | admin_adjust | void
    ref_id          BIGINT,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX rb_ledger_user_idx ON rb_ledger (tg_id, created_at DESC);

-- ---------- реферальные ссылки ----------
-- публичная ссылка юзера: t.me/bot?start=<code>
CREATE TABLE rb_ref_links (
    code       TEXT PRIMARY KEY,
    chat_id    BIGINT NOT NULL REFERENCES rb_chats(chat_id) ON DELETE CASCADE,
    inviter_id BIGINT NOT NULL REFERENCES rb_users(tg_id)   ON DELETE CASCADE,
    clicks     INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chat_id, inviter_id)
);

-- одноразовый invite-link, который бот выдаёт конкретному гостю
CREATE TABLE rb_invites (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    inviter_id  BIGINT NOT NULL,
    visitor_id  BIGINT NOT NULL,          -- кому выдали
    invite_name TEXT   NOT NULL UNIQUE,   -- ключ атрибуции, прилетает в chat_member.invite_link.name
    invite_url  TEXT   NOT NULL,
    used_by     BIGINT,
    used_at     TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chat_id, visitor_id)
);

-- ---------- вечная привязка реферала ----------
CREATE TABLE rb_targets (
    chat_id          BIGINT NOT NULL,
    user_id          BIGINT NOT NULL,
    first_inviter_id BIGINT NOT NULL,     -- кто привёл ПЕРВЫМ. Навсегда. Перепривязки нет.
    burned           BOOLEAN NOT NULL DEFAULT FALSE,  -- выплатили и он ушёл -> больше никому и никогда
    burned_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, user_id)
);

-- ---------- рефералы (холды и выплаты) ----------
CREATE TABLE rb_referrals (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    inviter_id  BIGINT NOT NULL,
    invitee_id  BIGINT NOT NULL,
    currency    rb_currency NOT NULL,   -- фиксируется в момент входа реферала
    amount      BIGINT NOT NULL,
    status      rb_ref_status NOT NULL DEFAULT 'hold',
    joined_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    hold_until  TIMESTAMPTZ NOT NULL,
    credited_at TIMESTAMPTZ,
    voided_at   TIMESTAMPTZ,
    flagged     BOOLEAN NOT NULL DEFAULT FALSE,   -- подозрение на накрутку -> ручная проверка
    flag_reason TEXT
);
-- ровно один живой холд/выплата на пару (чат, реферал)
CREATE UNIQUE INDEX rb_referrals_alive_idx
    ON rb_referrals (chat_id, invitee_id)
    WHERE status IN ('hold', 'paid');
CREATE INDEX rb_referrals_due_idx ON rb_referrals (hold_until) WHERE status = 'hold';
CREATE INDEX rb_referrals_inviter_idx ON rb_referrals (inviter_id, status);

-- ---------- выводы ----------
CREATE TABLE rb_withdrawals (
    id          BIGSERIAL PRIMARY KEY,
    tg_id       BIGINT NOT NULL,
    chat_id     BIGINT NOT NULL,
    currency    rb_currency NOT NULL,
    amount      BIGINT NOT NULL CHECK (amount > 0),
    status      rb_wd_status NOT NULL DEFAULT 'pending',
    version     INT NOT NULL DEFAULT 1,      -- растёт при каждом изменении суммы (защита от старых кнопок)
    admin_chat_id BIGINT,
    admin_msg_id  BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at  TIMESTAMPTZ,
    decided_by  BIGINT,
    comment     TEXT
);
-- один активный вывод на юзера
CREATE UNIQUE INDEX rb_withdrawals_one_pending ON rb_withdrawals (tg_id) WHERE status = 'pending';
CREATE INDEX rb_withdrawals_user_idx ON rb_withdrawals (tg_id, created_at DESC);

-- ---------- рулетка ----------
CREATE TABLE rb_spins (
    id         BIGSERIAL PRIMARY KEY,
    tg_id      BIGINT NOT NULL,
    chat_id    BIGINT NOT NULL,
    currency   rb_currency NOT NULL,
    amount     BIGINT NOT NULL,
    spin_day   DATE   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- одна прокрутка в сутки на юзера — гарантия на уровне БД, а не логики
CREATE UNIQUE INDEX rb_spins_daily ON rb_spins (tg_id, spin_day);

-- ---------- аудит ----------
CREATE TABLE rb_audit (
    id         BIGSERIAL PRIMARY KEY,
    actor_id   BIGINT,
    action     TEXT NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX rb_audit_idx ON rb_audit (action, created_at DESC);
