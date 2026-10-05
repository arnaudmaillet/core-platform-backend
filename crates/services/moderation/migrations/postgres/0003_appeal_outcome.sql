-- Migration: 0003_appeal_outcome
-- Description: What an appellant reads of their appeal (DSA Art. 20(4)–(5)):
--              the reviewer's reasons once resolved (`outcome`), and their own
--              appeals newest first. One appeal per (decision, appellant): a
--              second FileAppeal returns the first instead of a duplicate.
--              Pure ANSI SQL.

ALTER TABLE appeals ADD COLUMN IF NOT EXISTS outcome TEXT;

-- Before the unique index: nothing prevented duplicate appeals until now, and a
-- restored backup may hold some. Keep one per (decision, appellant) — the one
-- already resolved if any (its outcome is what the appellant was told), else
-- the earliest — so the index (and this migration) cannot fail on old data.
DELETE FROM appeals
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               row_number() OVER (
                   PARTITION BY decision_id, actor_id
                   ORDER BY (resolved_at IS NULL), filed_at, id
               ) AS rank
        FROM appeals
    ) ranked
    WHERE rank > 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_appeals_decision_actor ON appeals (decision_id, actor_id);

CREATE INDEX IF NOT EXISTS idx_appeals_actor_filed ON appeals (actor_id, filed_at DESC, id DESC);
