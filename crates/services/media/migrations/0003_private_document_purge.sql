-- Private documents (#777): when each is purged — a backstop from READY,
-- moved to the verification decision + the retention. Projected out of the
-- aggregate document so the sweeper reads only what is due.
ALTER TABLE assets ADD COLUMN IF NOT EXISTS purge_after TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_assets_purge_after
    ON assets (purge_after)
    WHERE purge_after IS NOT NULL AND state <> 'deleted';
