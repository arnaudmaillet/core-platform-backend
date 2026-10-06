-- Passkeys (#808): the WebAuthn credentials an account signs in with. Sharded
-- on the account like its sessions: a passkey sign-in names its account (the
-- credential's user handle), so the lookup goes straight to the shard. Erased
-- with the account (GDPR Art. 17).
CREATE TABLE IF NOT EXISTS passkeys (
    account_id      UUID        NOT NULL,
    credential_id   BYTEA       NOT NULL,
    -- SEC1 uncompressed P-256 point (ES256 only).
    public_key      BYTEA       NOT NULL,
    -- The authenticator's signature counter (0 for synced passkeys).
    sign_count      BIGINT      NOT NULL DEFAULT 0,
    name            TEXT        NOT NULL,
    -- The authenticator model (all zero for most synced passkeys).
    aaguid          UUID        NOT NULL,
    backup_eligible BOOLEAN     NOT NULL,
    backed_up       BOOLEAN     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    last_used_at    TIMESTAMPTZ,
    PRIMARY KEY (account_id, credential_id)
);
