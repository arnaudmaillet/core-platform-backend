-- Migration: 0003_appeal_outcome
-- Description: What an appellant reads of their appeal (DSA Art. 20(4)–(5)):
--              the reviewer's reasons once resolved (`outcome`), and their own
--              appeals newest first. One appeal per (decision, appellant): a
--              second FileAppeal returns the first instead of a duplicate.
--              Pure ANSI SQL.

ALTER TABLE appeals ADD COLUMN IF NOT EXISTS outcome TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_appeals_decision_actor ON appeals (decision_id, actor_id);

CREATE INDEX IF NOT EXISTS idx_appeals_actor_filed ON appeals (actor_id, filed_at DESC, id DESC);
