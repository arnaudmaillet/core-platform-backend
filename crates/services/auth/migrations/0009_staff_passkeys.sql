-- #880: staff permissions only in sessions opened with a staff-enrolled passkey.
-- Additive.

-- How a session was opened: `passkey` | `apple` | `google` | `password` |
-- `email_code` | `phone_code` (null: a guest, or a session from before), and
-- the passkey when it was one — re-checked at every refresh.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS sign_in_method        TEXT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS passkey_credential_id BYTEA;

-- A passkey registered with a staff enrollment grant. Passkeys registered
-- before keep `false`: a role assigned later never makes them staff.
ALTER TABLE passkeys ADD COLUMN IF NOT EXISTS staff_enrolled BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS passkeys_staff_enrolled ON passkeys (account_id) WHERE staff_enrolled;

-- Staff enrollment grants: single use, short-lived, bound to one account. Only
-- the SHA-256 of the grant is kept. `issued_by` is null for the bootstrap.
CREATE TABLE IF NOT EXISTS staff_enrollment_grants (
    grant_hash TEXT        PRIMARY KEY,
    -- Names the grant in audit (the grant itself is a secret).
    grant_id   UUID        NOT NULL,
    account_id UUID        NOT NULL,
    issued_by  UUID,
    issued_at  TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS staff_enrollment_grants_account ON staff_enrollment_grants (account_id);
