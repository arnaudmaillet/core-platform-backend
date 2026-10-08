-- wallet (#665): the daily curator envelope, shadow mode. Once a UTC day is
-- over, the positions settled that day share a fixed pool: each settlement
-- gets its outcome (its target in the day's top or not), its score and its
-- provisional gems — recorded, never minted.
ALTER TABLE settlements ADD COLUMN IF NOT EXISTS outcome          SMALLINT;
ALTER TABLE settlements ADD COLUMN IF NOT EXISTS score            DOUBLE PRECISION;
ALTER TABLE settlements ADD COLUMN IF NOT EXISTS provisional_gems BIGINT;
-- The day whose envelope it was counted in (null: not yet).
ALTER TABLE settlements ADD COLUMN IF NOT EXISTS envelope_day     DATE;

-- What is left to count, by day.
CREATE INDEX IF NOT EXISTS settlements_without_envelope ON settlements (settled_at) WHERE envelope_day IS NULL;

-- One row per day computed, on one shard only (the nil UUID's): leased to one replica
-- while it computes (`claimed_until`), then its summary.
CREATE TABLE IF NOT EXISTS envelope_days (
    day           DATE        PRIMARY KEY,
    claimed_until TIMESTAMPTZ,
    computed_at   TIMESTAMPTZ,
    pool          BIGINT,
    allocated     BIGINT,
    positions     BIGINT,
    targets       BIGINT,
    model         TEXT
);
