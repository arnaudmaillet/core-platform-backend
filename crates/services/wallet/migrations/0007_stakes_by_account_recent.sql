-- #886: the caller's own stakes, newest first stake first, paged by
-- (first_at, target) — ListMyStakes. Additive.
CREATE INDEX IF NOT EXISTS stakes_by_account_recent
    ON stakes (account_id, first_at DESC, target_kind DESC, target_id DESC);
