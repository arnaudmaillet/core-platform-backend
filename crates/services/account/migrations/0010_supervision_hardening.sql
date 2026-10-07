-- Family supervision hardening (#670, after #846's review).
--
-- An invite is claimed by its acceptor with a compare-and-set: two accounts
-- racing on one code (a leaked teen code against the real parent) never both
-- pair; the same acceptor's retry completes.
ALTER TABLE supervision_invites ADD COLUMN IF NOT EXISTS claimed_by UUID;

-- Failed accepts (unknown or expired codes) per account per hour: a cap
-- closes code enumeration for something as sensitive as supervising a minor.
-- On the account's shard.
CREATE TABLE IF NOT EXISTS supervision_accept_failures (
    account_id UUID        NOT NULL,
    hour       TIMESTAMPTZ NOT NULL,
    failures   INTEGER     NOT NULL,
    PRIMARY KEY (account_id, hour)
);
