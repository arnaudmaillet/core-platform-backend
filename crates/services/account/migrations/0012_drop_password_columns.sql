-- Passwords are retired platform-wide (#881): nothing signs in with one, so
-- the account keeps no password hash and no failed-login lockout state.
-- No account has ever existed (prod never ran), so there is nothing to carry.
ALTER TABLE accounts
    DROP COLUMN IF EXISTS password_hash,
    DROP COLUMN IF EXISTS password_changed_at,
    DROP COLUMN IF EXISTS failed_login_attempts,
    DROP COLUMN IF EXISTS locked_until;
