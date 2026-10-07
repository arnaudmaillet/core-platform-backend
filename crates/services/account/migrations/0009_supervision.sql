-- Family supervision (#670): a parent paired with a teen's account.
--
-- An invite lives on the shard of its own code (it is found from the code
-- alone), single use, 24 hours. A supervision lives on the TEEN's shard —
-- where "at most two supervisors" holds atomically and where coming of age
-- is read (the teen's accounts row is there) — with a reverse index on the
-- supervisor's shard for the supervisor's own list. Ending deletes both;
-- the history is in account.v1.events (supervision_started / _ended).
CREATE TABLE IF NOT EXISTS supervision_invites (
    code       TEXT        PRIMARY KEY,
    creator_id UUID        NOT NULL,
    -- The creator's side: 'supervisor' or 'teen'.
    role       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS supervision_invites_expiry ON supervision_invites (expires_at);

CREATE TABLE IF NOT EXISTS supervisions (
    teen_id       UUID        NOT NULL,
    supervisor_id UUID        NOT NULL,
    since         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (teen_id, supervisor_id)
);

CREATE TABLE IF NOT EXISTS supervisions_by_supervisor (
    supervisor_id UUID        NOT NULL,
    teen_id       UUID        NOT NULL,
    since         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (supervisor_id, teen_id)
);
