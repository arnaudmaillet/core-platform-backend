-- wallet (#665): the events a stake announces, written in the stake's own
-- transaction (a transactional outbox) and published from here: right after
-- the commit, and by a drainer for whatever is left — never lost, at least
-- once (consumers dedup on the event's key). A row is leased to one replica
-- at a time (`claimed_until`): the writer holds it while it publishes right
-- after the commit; the drainers claim the rest with SKIP LOCKED.
CREATE TABLE IF NOT EXISTS wallet_outbox (
    id           UUID        PRIMARY KEY,
    account_id   UUID        NOT NULL,
    event        JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    published_at  TIMESTAMPTZ,
    claimed_until TIMESTAMPTZ
);

-- What is left to publish, oldest first.
CREATE INDEX IF NOT EXISTS wallet_outbox_unpublished
    ON wallet_outbox (created_at) WHERE published_at IS NULL;
