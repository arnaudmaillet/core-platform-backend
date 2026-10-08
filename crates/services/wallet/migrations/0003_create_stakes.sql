-- wallet (#665 part 3): likes are points. What each account has staked on
-- each post or comment (the 250-point room, "my likes"), and the key of its
-- first batch (the "X liked your post" notice goes out once).
CREATE TABLE IF NOT EXISTS stakes (
    account_id  UUID        NOT NULL,
    target_kind TEXT        NOT NULL CHECK (target_kind IN ('post', 'comment')),
    target_id   TEXT        NOT NULL,
    total       BIGINT      NOT NULL CHECK (total > 0),
    first_key   TEXT        NOT NULL,
    first_at    TIMESTAMPTZ NOT NULL,
    last_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, target_kind, target_id)
);

-- The hour's room: an account's stakes of the last hour.
CREATE INDEX IF NOT EXISTS wallet_transactions_recent_stakes
    ON wallet_transactions (account_id, created_at) WHERE kind = 'stake';
