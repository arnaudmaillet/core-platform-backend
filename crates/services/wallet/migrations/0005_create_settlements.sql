-- wallet (#665): stake settlement, shadow mode. A position (an account's
-- stakes on one post or comment) settles once, a day after its first stake:
-- what the target came to is observed, the position scored and recorded.
-- No gems are minted yet (the daily envelope comes later). A position is
-- leased to one replica at a time (`settle_claimed_until`), like the outbox.
ALTER TABLE stakes ADD COLUMN IF NOT EXISTS settled_at           TIMESTAMPTZ;
ALTER TABLE stakes ADD COLUMN IF NOT EXISTS settle_claimed_until TIMESTAMPTZ;

-- What is left to settle, oldest first.
CREATE INDEX IF NOT EXISTS stakes_unsettled ON stakes (first_at) WHERE settled_at IS NULL;

CREATE TABLE IF NOT EXISTS settlements (
    account_id          UUID             NOT NULL,
    target_kind         TEXT             NOT NULL CHECK (target_kind IN ('post', 'comment')),
    target_id           TEXT             NOT NULL,
    -- The position's points when it settled.
    points              BIGINT           NOT NULL,
    first_at            TIMESTAMPTZ      NOT NULL,
    settled_at          TIMESTAMPTZ      NOT NULL,
    -- The target's like count just before the account's first like (null:
    -- unknown), and when the position settled.
    count_on_arrival    BIGINT,
    count_at_settlement BIGINT           NOT NULL,
    -- The share of everyone else's points that came after the account
    -- (null when its arrival is unknown), and the score before the day's
    -- outcome.
    earliness           DOUBLE PRECISION,
    pre_score           DOUBLE PRECISION,
    model               TEXT             NOT NULL,
    PRIMARY KEY (account_id, target_kind, target_id)
);

-- A day's settlements (the daily envelope).
CREATE INDEX IF NOT EXISTS settlements_by_day ON settlements (settled_at);
