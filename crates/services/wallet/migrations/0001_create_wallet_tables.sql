-- wallet (#665): the in-app economy's ledger. One wallet per account, both
-- currencies; every balance movement is an append-only transaction row, and
-- each balance always equals the sum of its currency's deltas.

CREATE TABLE IF NOT EXISTS wallets (
    account_id     UUID        PRIMARY KEY,
    points         BIGINT      NOT NULL DEFAULT 0 CHECK (points >= 0),
    gems           BIGINT      NOT NULL DEFAULT 0 CHECK (gems >= 0),
    points_earned  BIGINT      NOT NULL DEFAULT 0,
    points_spent   BIGINT      NOT NULL DEFAULT 0,
    gems_earned    BIGINT      NOT NULL DEFAULT 0,
    gems_spent     BIGINT      NOT NULL DEFAULT 0,
    -- The hourly claim (UTC days).
    last_claim_at  TIMESTAMPTZ,
    claimed_today  INTEGER     NOT NULL DEFAULT 0,
    claimed_day    DATE,
    streak_days    INTEGER     NOT NULL DEFAULT 0,
    -- Shots left in the x100 stake pack.
    stake_shots    INTEGER     NOT NULL DEFAULT 0 CHECK (stake_shots >= 0),
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet_transactions (
    id              UUID        PRIMARY KEY,
    account_id      UUID        NOT NULL,
    currency        TEXT        NOT NULL CHECK (currency IN ('points', 'gems')),
    delta           BIGINT      NOT NULL CHECK (delta <> 0),
    balance_after   BIGINT      NOT NULL CHECK (balance_after >= 0),
    kind            TEXT        NOT NULL,
    -- Client keys are 8-64 [A-Za-z0-9_-]; the service's own carry `sys:`.
    idempotency_key TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (account_id, idempotency_key)
);

-- The history, newest first (keyset on created_at, id).
CREATE INDEX IF NOT EXISTS wallet_transactions_history
    ON wallet_transactions (account_id, created_at DESC, id DESC);
