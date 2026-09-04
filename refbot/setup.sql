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
    CREATE TYPE rb_currency AS ENUM ('mushrooms', 'coins', 'shimcoins');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Для существующей базы (enum создан без shimcoins) — добавляем значение.
-- ADD VALUE автокоммитится отдельным стейтментом (psql без глобальной транзакции),
-- поэтому доступно для использования ниже.
ALTER TYPE rb_currency ADD VALUE IF NOT EXISTS 'shimcoins';
-- Токены (Revive/Max/Partials): целые, только купить и вывести.
ALTER TYPE rb_currency ADD VALUE IF NOT EXISTS 'revive';
ALTER TYPE rb_currency ADD VALUE IF NOT EXISTS 'max';
ALTER TYPE rb_currency ADD VALUE IF NOT EXISTS 'partials';

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
    wheel_anim TEXT        NOT NULL DEFAULT 'runner',
    free_spins INT         NOT NULL DEFAULT 0,
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
    amount   BIGINT NOT NULL DEFAULT 0,  -- может быть <0 после админ-штрафа (изъятие в минус)
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
    currency      rb_currency,                        -- legacy (старые заявки); в корзине NULL
    amount        BIGINT CHECK (amount IS NULL OR amount > 0),
    status        rb_wd_status NOT NULL DEFAULT 'pending',
    version       INT NOT NULL DEFAULT 1,
    admin_chat_id BIGINT,
    admin_msg_id  BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at    TIMESTAMPTZ,
    decided_by    BIGINT,
    comment       TEXT
);

-- Позиции корзины вывода. Одна заявка (rb_withdrawals) -> много позиций.
-- Вариант А: у каждой позиции свой статус — админ подтверждает/отклоняет по отдельности.
-- Отклонённая позиция -> заморозка возвращается игроку; подтверждённая -> выдана в игре.
-- Заявка закрывается, когда все её позиции обработаны.
CREATE TABLE IF NOT EXISTS rb_wd_items (
    id          BIGSERIAL PRIMARY KEY,
    wid         BIGINT NOT NULL REFERENCES rb_withdrawals(id) ON DELETE CASCADE,
    currency    rb_currency  NOT NULL,
    amount      BIGINT       NOT NULL CHECK (amount > 0),
    status      rb_wd_status NOT NULL DEFAULT 'pending',
    decided_at  TIMESTAMPTZ,
    decided_by  BIGINT,
    comment     TEXT
);
CREATE INDEX IF NOT EXISTS rb_wd_items_wid_idx ON rb_wd_items (wid);

-- Миграция существующей rb_withdrawals: для корзины currency/amount становятся NULL-able.
DO $$
BEGIN
    BEGIN
        ALTER TABLE rb_withdrawals ALTER COLUMN currency DROP NOT NULL;
    EXCEPTION WHEN others THEN NULL; END;
    BEGIN
        ALTER TABLE rb_withdrawals ALTER COLUMN amount DROP NOT NULL;
    EXCEPTION WHEN others THEN NULL; END;
END $$;

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


-- Лог открытий кейсов (казино). Для истории и антифрод-аналитики.
CREATE TABLE IF NOT EXISTS rb_case_opens (
    id         BIGSERIAL PRIMARY KEY,
    tg_id      BIGINT NOT NULL,
    case_key   TEXT NOT NULL,          -- small|medium|big
    currency   TEXT NOT NULL,          -- mushrooms|coins
    cost       BIGINT NOT NULL,        -- сколько заплатил (в валюте)
    won        BIGINT NOT NULL,        -- сколько выиграл (в валюте)
    multiplier NUMERIC NOT NULL,       -- множитель приза (0.25..10)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- Промокоды: код + награда в грибах (коины по курсу), лимит активаций, срок.
CREATE TABLE IF NOT EXISTS rb_promo (
    id           BIGSERIAL PRIMARY KEY,
    code         TEXT NOT NULL,             -- кодовое слово (храним как есть; сверяем без регистра)
    reward_mush  BIGINT NOT NULL,           -- сумма награды (трактуется по reward_kind)
    reward_kind  TEXT NOT NULL DEFAULT 'rate',  -- rate|mushrooms|coins (см. ниже)
    max_acts     INT,                       -- лимит активаций; NULL = безлимит
    used         INT NOT NULL DEFAULT 0,    -- сколько раз активирован
    expires_at   TIMESTAMPTZ,               -- срок; NULL = бессрочно
    created_by   BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- reward_kind:
--   rate      — reward_mush в грибах, игрок получает в СВОЕЙ валюте (коины = ×COIN_RATE)
--   mushrooms — все получают reward_mush ГРИБОВ независимо от своей валюты
--   coins     — все получают reward_mush КОИНОВ независимо от своей валюты

-- Активации промокодов: кто какой код активировал (одна активация на человека).
-- секретные промокоды: активация даёт особый счётчик для достижений
ALTER TABLE rb_promo ADD COLUMN IF NOT EXISTS is_secret BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS rb_promo_acts (
    promo_id   BIGINT NOT NULL REFERENCES rb_promo(id) ON DELETE CASCADE,
    tg_id      BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (promo_id, tg_id)
);

-- Особые предложения (акции обмена Шимкоинов на грибы/коины). Адресные —
-- создаются для конкретного игрока. Цена динамическая, задаётся админом.
CREATE TABLE IF NOT EXISTS rb_offers (
    id              BIGSERIAL PRIMARY KEY,
    tg_id           BIGINT NOT NULL,          -- кому адресована акция
    -- цена в Шимкоинах: за 1млн грибов и/или за 100млн коинов. NULL = валюта недоступна.
    price_mush      BIGINT,                    -- Шимкоинов за 1_000_000 грибов
    price_coin      BIGINT,                    -- Шимкоинов за 100_000_000 коинов
    -- лимит на ВСЮ акцию (сколько всего грибов/коинов можно купить). NULL = без лимита.
    limit_mush      BIGINT,
    limit_coin      BIGINT,
    sold_mush       BIGINT NOT NULL DEFAULT 0, -- уже куплено грибов
    sold_coin       BIGINT NOT NULL DEFAULT 0, -- уже куплено коинов
    expires_at      TIMESTAMPTZ,               -- срок; NULL = бессрочно
    active          BOOLEAN NOT NULL DEFAULT TRUE,  -- админ может выключить/удалить
    created_by      BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rb_offers_user_idx ON rb_offers (tg_id) WHERE active = TRUE;

-- Карта username -> tg_id, чтобы резолвить @username в шёпотах (!секрет).
-- Заполняется при каждом сообщении в чате (кого бот видел). last_seen — для свежести.
CREATE TABLE IF NOT EXISTS rb_usernames (
    username   TEXT PRIMARY KEY,   -- в нижнем регистре, без @
    tg_id      BIGINT NOT NULL,
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rb_usernames_tgid_idx ON rb_usernames (tg_id);

-- Счётчик дневных обменов грибы<->коины в банке (лимит 2/день, сброс в полночь МСК).
-- exch_day — дата по МСК (YYYY-MM-DD строкой).
CREATE TABLE IF NOT EXISTS rb_bank_exch (
    tg_id     BIGINT NOT NULL,
    exch_day  TEXT   NOT NULL,
    cnt       INT    NOT NULL DEFAULT 0,
    PRIMARY KEY (tg_id, exch_day)
);


-- МИГРАЦИЯ: шимкоины переходят на хранение В ЦЕНТАХ (1 ШК = 100 центов).
-- Выполняется ОДИН раз. Повторный запуск setup.sql не задваивает — флаг в rb_settings.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM rb_settings WHERE key = 'shk.cents_migrated') THEN
        UPDATE rb_balances SET amount = amount * 100 WHERE currency = 'shimcoins';
        INSERT INTO rb_settings (key, value) VALUES ('shk.cents_migrated', '1')
            ON CONFLICT (key) DO UPDATE SET value = '1';
    END IF;
END $$;


-- ---------- 3. Индексы (без них защита от накрутки не работает) ----------
CREATE INDEX        IF NOT EXISTS rb_ledger_user_idx      ON rb_ledger (tg_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS rb_referrals_alive_idx  ON rb_referrals (chat_id, invitee_id) WHERE status IN ('hold','paid');
CREATE INDEX        IF NOT EXISTS rb_referrals_due_idx    ON rb_referrals (hold_until) WHERE status = 'hold';
CREATE INDEX        IF NOT EXISTS rb_referrals_inviter_idx ON rb_referrals (inviter_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS rb_withdrawals_one_pending ON rb_withdrawals (tg_id) WHERE status = 'pending';
CREATE INDEX        IF NOT EXISTS rb_withdrawals_user_idx ON rb_withdrawals (tg_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS rb_spins_daily          ON rb_spins (tg_id, spin_day);
CREATE INDEX        IF NOT EXISTS rb_audit_idx            ON rb_audit (action, created_at DESC);
CREATE INDEX        IF NOT EXISTS rb_case_opens_idx     ON rb_case_opens (tg_id, created_at DESC);
CREATE INDEX        IF NOT EXISTS rb_promo_code_idx     ON rb_promo (lower(code));
CREATE INDEX        IF NOT EXISTS rb_promo_acts_idx     ON rb_promo_acts (tg_id);



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


-- ---------- PvP-матчи (крестики-нолики, морской бой, будущие игры) ----------
-- Универсальная таблица для любых игр на двоих со ставкой.
--   game     — 'ttt' (крестики-нолики), 'sea' (морской бой), ...
--   status   — searching (ждём случайного) | invited (ждём ответа приглашённого)
--              | active (идёт игра) | finished | cancelled | expired
--   stake/currency — равная ставка каждого; банк = 2*stake (заморожен у обоих)
--   p1 — создатель, p2 — оппонент (NULL пока ищем/не принял)
--   turn — чей сейчас ход (tg_id); state — JSONB состояние доски
--   chat_id/message_id — где показано поле (для общего поля в чате);
--   p1_msg/p2_msg — сообщения в личках (для ЛС-игры, синхронизируем оба)
--   move_deadline — дедлайн текущего хода (тайм-аут 5 мин); search_deadline — поиск 10 мин
CREATE TABLE IF NOT EXISTS rb_matches (
    id            BIGSERIAL PRIMARY KEY,
    game          TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'searching',
    currency      rb_currency NOT NULL,
    stake         BIGINT NOT NULL CHECK (stake > 0),
    p1            BIGINT NOT NULL,
    p2            BIGINT,
    p1_symbol     TEXT,                    -- 'X'|'O' для крестиков
    turn          BIGINT,                  -- чей ход (tg_id)
    winner        BIGINT,                  -- NULL пока не решено; 0 = ничья
    state         JSONB NOT NULL DEFAULT '{}'::jsonb,
    origin_chat   BIGINT,                  -- где создан (чат или личка-создателя)
    board_chat_id BIGINT,                  -- чат общего поля
    board_msg_id  BIGINT,                  -- сообщение общего поля в чате
    p1_msg_id     BIGINT,                  -- поле в личке p1 (ЛС-режим)
    p2_msg_id     BIGINT,                  -- поле в личке p2 (ЛС-режим)
    move_deadline TIMESTAMPTZ,             -- дедлайн текущего хода
    search_deadline TIMESTAMPTZ,           -- дедлайн поиска оппонента
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS rb_matches_status_idx ON rb_matches (status);
CREATE INDEX IF NOT EXISTS rb_matches_p1_idx ON rb_matches (p1);
CREATE INDEX IF NOT EXISTS rb_matches_p2_idx ON rb_matches (p2);
-- один активный матч на игрока: частичный уникальный индекс по участникам в игре/поиске
-- (реализуем проверкой в коде, т.к. игрок может быть p1 или p2 — сложно одним индексом)

-- ---------- Участники мультиплеерных матчей (камень-ножницы-бумага и будущие) ----------
-- Для игр на 3-5 человек rb_matches хранит контейнер (game='rps', p1=создатель),
-- а участники и их ходы — здесь. Двухигровые игры (ttt) этой таблицей не пользуются.
--   status: playing (в игре) | out (выбыл в раунде) | dq (дисквал — не выбрал ход)
--           | winner (победитель) | left (отменил до старта)
--   choice: текущий выбор в раунде ('rock'|'scissors'|'paper'|NULL если ещё не выбрал)
--   staked: заморожена ли ставка этого игрока (для корректного возврата)
CREATE TABLE IF NOT EXISTS rb_match_players (
    id         BIGSERIAL PRIMARY KEY,
    mid        BIGINT NOT NULL REFERENCES rb_matches(id) ON DELETE CASCADE,
    tg_id      BIGINT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'playing',
    choice     TEXT,
    staked     BOOLEAN NOT NULL DEFAULT FALSE,
    joined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (mid, tg_id)
);
CREATE INDEX IF NOT EXISTS rb_match_players_mid_idx ON rb_match_players (mid);
CREATE INDEX IF NOT EXISTS rb_match_players_tg_idx ON rb_match_players (tg_id);

-- ---------- Вопросы викторины ----------
-- options — JSONB-массив из 4 строк, correct — индекс правильного (0-3).
CREATE TABLE IF NOT EXISTS rb_quiz (
    id       BIGSERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    options  JSONB NOT NULL,
    correct  SMALLINT NOT NULL,
    active   BOOLEAN NOT NULL DEFAULT TRUE,
    added_by BIGINT,
    points   SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- вопросы, добавленные админами, дают больше баллов (points=2)
ALTER TABLE rb_quiz ADD COLUMN IF NOT EXISTS points SMALLINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS rb_quiz_active_idx ON rb_quiz (active);

-- ==================== СИСТЕМА ДОСТИЖЕНИЙ, ПРОФИЛЯ, МАГАЗИНА ====================

-- --- Расширение профиля (rb_users) ---
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS nickname     TEXT;
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS nickname_set BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS active_title BIGINT;
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS active_emoji TEXT;
-- уникальность ника (частичный индекс — NULL не мешают)
CREATE UNIQUE INDEX IF NOT EXISTS rb_users_nickname_uidx
    ON rb_users (lower(nickname)) WHERE nickname IS NOT NULL;

-- --- Каталог титулов ---
CREATE TABLE IF NOT EXISTS rb_titles (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    is_admin_grant BOOLEAN NOT NULL DEFAULT FALSE,  -- уникальный титул от админа
    created_by     BIGINT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- --- Титулы игроков (что у кого есть) ---
CREATE TABLE IF NOT EXISTS rb_user_titles (
    id         BIGSERIAL PRIMARY KEY,
    tg_id      BIGINT NOT NULL,
    title_id   BIGINT NOT NULL REFERENCES rb_titles(id) ON DELETE CASCADE,
    granted_by BIGINT,          -- админ, или NULL если от достижения
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tg_id, title_id)
);
CREATE INDEX IF NOT EXISTS rb_user_titles_tg_idx ON rb_user_titles (tg_id);

-- --- Персональные эмодзи игроков ---
CREATE TABLE IF NOT EXISTS rb_user_emojis (
    id      BIGSERIAL PRIMARY KEY,
    tg_id   BIGINT NOT NULL,
    emoji   TEXT NOT NULL,       -- символ (премиум подставляется через маппинг)
    source  TEXT,                -- 'achievement' | 'shop'
    got_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tg_id, emoji)
);
CREATE INDEX IF NOT EXISTS rb_user_emojis_tg_idx ON rb_user_emojis (tg_id);

-- --- Счётчики событий (сердце достижений) ---
-- counter_type: 'pvp_played','pvp_won','withdraw_total','withdraw_count',
--   'cases_played','wheel_won_total','promo_entered','referrals', ... (см. код)
-- Пиковые счётчики (максимум за раз) имеют префикс 'max_' и берут MAX, не сумму.
CREATE TABLE IF NOT EXISTS rb_counters (
    tg_id        BIGINT NOT NULL,
    counter_type TEXT   NOT NULL,
    value        BIGINT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tg_id, counter_type)
);
CREATE INDEX IF NOT EXISTS rb_counters_type_idx ON rb_counters (counter_type);

-- --- Каталог достижений (создаёт админ) ---
CREATE TABLE IF NOT EXISTS rb_achievements (
    id             BIGSERIAL PRIMARY KEY,
    code           TEXT UNIQUE NOT NULL,
    title          TEXT NOT NULL,
    description    TEXT,
    hidden         BOOLEAN NOT NULL DEFAULT FALSE,  -- скрытое (условия не видны)
    trigger_type   TEXT NOT NULL,     -- какой счётчик отслеживать
    trigger_target BIGINT NOT NULL,   -- порог
    progress_style TEXT NOT NULL DEFAULT 'percent',  -- 'percent' | 'fraction'
    rewards        JSONB NOT NULL DEFAULT '[]',  -- [{type,...}, ...]
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_by     BIGINT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rb_ach_trigger_idx ON rb_achievements (trigger_type) WHERE active;
-- текст-подпись, который показывается игроку при получении награды
ALTER TABLE rb_achievements ADD COLUMN IF NOT EXISTS claim_text TEXT;

-- --- Прогресс игроков по достижениям ---
CREATE TABLE IF NOT EXISTS rb_user_achievements (
    id           BIGSERIAL PRIMARY KEY,
    tg_id        BIGINT NOT NULL,
    ach_id       BIGINT NOT NULL REFERENCES rb_achievements(id) ON DELETE CASCADE,
    progress     BIGINT NOT NULL DEFAULT 0,
    completed    BOOLEAN NOT NULL DEFAULT FALSE,
    claimed      BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at TIMESTAMPTZ,
    claimed_at   TIMESTAMPTZ,
    UNIQUE (tg_id, ach_id)
);
CREATE INDEX IF NOT EXISTS rb_user_ach_tg_idx ON rb_user_achievements (tg_id);
CREATE INDEX IF NOT EXISTS rb_user_ach_claim_idx ON rb_user_achievements (tg_id)
    WHERE completed AND NOT claimed;

-- --- Магазин (товары) ---
CREATE TABLE IF NOT EXISTS rb_shop (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    description TEXT,
    item_type  TEXT NOT NULL,     -- 'luck' | 'discount' | 'title' | 'emoji'
    price      BIGINT NOT NULL,
    currency   TEXT NOT NULL,     -- 'shimcoins' | 'mushrooms' | 'coins'
    payload    JSONB NOT NULL DEFAULT '{}',  -- luck:{minutes,scope,mult}; discount:{percent,target}; ...
    active     BOOLEAN NOT NULL DEFAULT TRUE,
    stock      INT,               -- NULL = безлимит
    created_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rb_shop_active_idx ON rb_shop (active);

-- --- Инвентарь (купленные/полученные бонусы, ещё не активированные) ---
CREATE TABLE IF NOT EXISTS rb_inventory (
    id        BIGSERIAL PRIMARY KEY,
    tg_id     BIGINT NOT NULL,
    item_type TEXT NOT NULL,      -- 'luck' | 'discount'
    payload   JSONB NOT NULL DEFAULT '{}',
    used      BOOLEAN NOT NULL DEFAULT FALSE,
    got_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS rb_inventory_tg_idx ON rb_inventory (tg_id) WHERE NOT used;

-- --- Активные бонусы (удача/скидка, действующие сейчас) ---
CREATE TABLE IF NOT EXISTS rb_active_bonuses (
    id         BIGSERIAL PRIMARY KEY,
    tg_id      BIGINT NOT NULL,
    bonus_type TEXT NOT NULL,     -- 'luck' | 'discount'
    scope      TEXT NOT NULL,     -- 'all' | 'roulette' | 'cases' | 'shine' | 'giveaway' | 'contest' | 'shop' | 'bank' | ...
    multiplier NUMERIC NOT NULL DEFAULT 2,
    expires_at TIMESTAMPTZ,       -- для удачи (суммируется время); NULL для скидки-до-использования
    payload    JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rb_active_bonuses_tg_idx ON rb_active_bonuses (tg_id);
CREATE INDEX IF NOT EXISTS rb_active_bonuses_exp_idx ON rb_active_bonuses (expires_at);




-- ---------- 4b. Рулетка (одобренные чаты) ----------
-- Рулетка !шайн работает ТОЛЬКО в чатах, где главный админ включил её через /шимм.
-- Отдельно от rb_chats СПЕЦИАЛЬНО: иначе чат с одной лишь рулеткой полез бы
-- в «Мою ссылку», в маршрутизацию выводов и т.д.

CREATE TABLE IF NOT EXISTS rb_roulette_chats (
    chat_id        BIGINT PRIMARY KEY,
    title          TEXT,
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    spins          INT NOT NULL DEFAULT 0,
    enabled_by     BIGINT,
    enabled_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at TIMESTAMPTZ,
    deactivated_by BIGINT,
    last_spin_at   TIMESTAMPTZ
);

-- Перенос уже включённых чатов из прежней «свободной» таблицы (если она была).
-- Незаблокированные становятся активными одобренными чатами.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='rb_free_chats') THEN
        INSERT INTO rb_roulette_chats (chat_id, title, active, spins, enabled_at)
        SELECT chat_id, title, NOT blocked, spins, first_seen
        FROM rb_free_chats
        ON CONFLICT (chat_id) DO NOTHING;
    END IF;
END $$;

-- Суточный бюджет рулетки по одобренным чатам (общий потолок-предохранитель).
CREATE TABLE IF NOT EXISTS rb_roulette_budget (
    day        DATE PRIMARY KEY,
    spent_mush BIGINT NOT NULL DEFAULT 0
);

-- Перенос израсходованного бюджета за сегодня из старой таблицы.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='rb_free_budget') THEN
        INSERT INTO rb_roulette_budget (day, spent_mush)
        SELECT day, spent_mush FROM rb_free_budget
        ON CONFLICT (day) DO NOTHING;
    END IF;
END $$;

-- Старые таблицы больше не нужны — «свободного» режима нет.
DROP TABLE IF EXISTS rb_free_chats;
DROP TABLE IF EXISTS rb_free_budget;

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

-- ---------- 4e. Розыгрыши (giveaway) ----------
-- Отдельная система от «конкурса активности» (/шимшайнуть). Тут: подписался ->
-- участвуешь -> случайные победители -> приз на баланс в боте. Управляет главный
-- админ из ЛС. Валюта режимами: mushrooms | coins | choice (игрок выбирает) |
-- both (и грибы и коины) | other (админ выдаёт лично).

CREATE TABLE IF NOT EXISTS rb_giveaways (
    id            BIGSERIAL PRIMARY KEY,
    title         TEXT NOT NULL,
    key_on        TEXT NOT NULL,          -- ключ привязки, напр. !шимм!
    key_off       TEXT NOT NULL,          -- ключ отвязки, напр. !отшимм!
    announce_text TEXT NOT NULL,          -- паста объявления
    finish_text   TEXT NOT NULL,          -- паста завершения
    reward_mode   TEXT NOT NULL,          -- mushrooms|coins|choice|both|other
    other_desc    TEXT,                   -- если mode=other: что за приз (для показа)
    places        INT  NOT NULL,          -- число призовых мест (1..50)
    prizes        JSONB NOT NULL,         -- [{place, mushrooms, coins}] на каждое место
    status        TEXT NOT NULL DEFAULT 'draft',  -- draft|running|finished
    announce_kb   TEXT,                   -- deep-link payload для кнопки «Участвовать»
    ends_at       TIMESTAMPTZ,            -- таймер автозавершения (NULL = вручную)
    created_by    BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ
);
-- Ключ привязки уникален только среди АКТИВНЫХ (draft|running). Завершённый
-- освобождает ключ. Частичный уникальный индекс это и обеспечивает.
CREATE UNIQUE INDEX IF NOT EXISTS rb_giveaways_key_active
    ON rb_giveaways (key_on) WHERE status IN ('draft', 'running');

-- Куда привязан розыгрыш (по ключу). Чат или канал — не важно, храним тип для
-- публикации (в канале постим от имени канала). invite — ссылка, созданная ботом.
CREATE TABLE IF NOT EXISTS rb_giveaway_chats (
    giveaway_id  BIGINT NOT NULL REFERENCES rb_giveaways(id) ON DELETE CASCADE,
    chat_id      BIGINT NOT NULL,
    title        TEXT,
    kind         TEXT NOT NULL DEFAULT 'chat',   -- chat|channel
    invite_link  TEXT,                            -- пригласительная, созданная ботом
    announce_msg BIGINT,                          -- id опубликованного объявления (для откр/удаления)
    result_msg   BIGINT,                          -- id поста с результатами
    PRIMARY KEY (giveaway_id, chat_id)
);

-- Участники розыгрыша. currency — выбранная валюта (для mode=choice), иначе NULL.
CREATE TABLE IF NOT EXISTS rb_giveaway_members (
    giveaway_id  BIGINT NOT NULL REFERENCES rb_giveaways(id) ON DELETE CASCADE,
    tg_id        BIGINT NOT NULL,
    currency     TEXT,                    -- выбор игрока при mode=choice
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_winner    BOOLEAN NOT NULL DEFAULT FALSE,
    place        INT,                     -- какое место занял (если победил)
    struck       BOOLEAN NOT NULL DEFAULT FALSE,  -- получил ли страйк за отписку
    PRIMARY KEY (giveaway_id, tg_id)
);

-- Страйки глобальные — храним счётчик прямо в rb_users. 3 страйка -> бан бота.
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS strikes INT NOT NULL DEFAULT 0;
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS wheel_anim TEXT NOT NULL DEFAULT 'runner';
ALTER TABLE rb_promo ADD COLUMN IF NOT EXISTS reward_kind TEXT NOT NULL DEFAULT 'rate';
-- Разрешаем отрицательный баланс (админ-штрафы). Снимаем старый CHECK, если он есть.
ALTER TABLE rb_balances DROP CONSTRAINT IF EXISTS rb_balances_amount_check;
ALTER TABLE rb_users ADD COLUMN IF NOT EXISTS free_spins INT NOT NULL DEFAULT 0;

-- title_html: название розыгрыша с премиум-эмодзи (для сообщений). Обычный title
-- остаётся plain — он идёт на кнопки, где HTML/премиум не рендерится.
ALTER TABLE rb_giveaways ADD COLUMN IF NOT EXISTS title_html TEXT;

-- Фото для объявления начала и итогов (Telegram file_id). NULL = без фото.
ALTER TABLE rb_giveaways ADD COLUMN IF NOT EXISTS announce_photo TEXT;
ALTER TABLE rb_giveaways ADD COLUMN IF NOT EXISTS finish_photo TEXT;

-- ---------- 5. Проверка ----------
SELECT
  (SELECT count(*) FROM pg_tables WHERE tablename ~ '^rb_')                   AS tables_expect_40,
  (SELECT count(*) FROM pg_type   WHERE typname ~ '^rb_' AND typtype = 'e')   AS enums_expect_3,
  (SELECT count(*) FROM pg_indexes WHERE indexname IN
     ('rb_referrals_alive_idx','rb_withdrawals_one_pending','rb_spins_daily',
      'rb_week_draws_uniq','rb_giveaways_key_active')) AS guards_expect_5,
  (SELECT count(*) FROM information_schema.columns
     WHERE table_name='rb_chats' AND column_name IN
       ('deactivated_at','deactivated_by'))                                    AS newcols_expect_2,
  current_database() AS db;
