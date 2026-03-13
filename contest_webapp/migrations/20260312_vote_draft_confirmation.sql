-- Draft + confirmation voting model for contest mini app
ALTER TABLE contest_votes ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ;
ALTER TABLE contest_entries ADD COLUMN IF NOT EXISTS penalty_votes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE contest_settings ADD COLUMN IF NOT EXISTS penalties_applied_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS contest_vote_drafts (
    voter_user_id BIGINT NOT NULL,
    entry_id BIGINT NOT NULL REFERENCES contest_entries(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(voter_user_id, entry_id)
);

CREATE INDEX IF NOT EXISTS idx_contest_vote_drafts_voter ON contest_vote_drafts(voter_user_id);

CREATE TABLE IF NOT EXISTS contest_vote_confirmations (
    voter_user_id BIGINT PRIMARY KEY,
    confirmed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    selected_entry_ids BIGINT[] NOT NULL,
    ip_hash TEXT,
    user_agent_hash TEXT,
    is_suspicious BOOLEAN NOT NULL DEFAULT FALSE,
    suspicion_reason TEXT
);