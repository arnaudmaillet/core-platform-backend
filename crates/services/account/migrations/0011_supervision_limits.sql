-- Family supervision limits (#670 part 2). One shared set per teen (either
-- supervisor edits it; the last change applies), on the teen's shard. Lifted
-- when the teen's last supervision ends.
CREATE TABLE IF NOT EXISTS supervision_limits (
    teen_id            UUID        PRIMARY KEY,
    private_account    BOOLEAN     NOT NULL,
    -- 'followers' | 'mutuals' | 'no_one'; NULL: no floor.
    messages           TEXT,
    comments           TEXT,
    hidden_from_search BOOLEAN     NOT NULL,
    daily_minutes      SMALLINT,
    set_by             UUID        NOT NULL,
    set_at             TIMESTAMPTZ NOT NULL
);

-- A supervised teen's time on the app per local day, all devices together
-- (the app reports it). On the account's shard; kept a few weeks.
CREATE TABLE IF NOT EXISTS screen_time (
    account_id UUID    NOT NULL,
    day        DATE    NOT NULL,
    minutes    INTEGER NOT NULL,
    PRIMARY KEY (account_id, day)
);
