ALTER TABLE contest_votes
ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE contest_votes
DROP CONSTRAINT IF EXISTS contest_votes_status_check;

ALTER TABLE contest_votes
ADD CONSTRAINT contest_votes_status_check
CHECK (status IN ('active', 'invalidated_by_disqualification'));

CREATE INDEX IF NOT EXISTS idx_contest_votes_voter_status
ON contest_votes(voter_user_id, status);

CREATE INDEX IF NOT EXISTS idx_contest_votes_entry_status
ON contest_votes(entry_id, status);