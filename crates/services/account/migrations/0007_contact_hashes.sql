-- Contact matching (#661): an app finds the accounts of its address book's
-- contacts by sending SHA-256 hashes of their normalized email addresses
-- (lower-cased, trimmed) and phone numbers (E.164), and the server keeps
-- nothing of them. The hashes of each account's own contacts are generated
-- columns, so every write and every existing row keeps them current, and
-- only verified contacts are indexed (unverified ones never match).
CREATE OR REPLACE FUNCTION account_contact_sha256(contact TEXT) RETURNS BYTEA
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS 'SELECT sha256(convert_to(contact, ''UTF8''))';

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS email_sha256 BYTEA
        GENERATED ALWAYS AS (account_contact_sha256(lower(trim(email)))) STORED,
    ADD COLUMN IF NOT EXISTS phone_sha256 BYTEA
        GENERATED ALWAYS AS (account_contact_sha256(phone)) STORED;

CREATE INDEX IF NOT EXISTS accounts_email_sha256_idx
    ON accounts (email_sha256) WHERE email_verified;
CREATE INDEX IF NOT EXISTS accounts_phone_sha256_idx
    ON accounts (phone_sha256) WHERE phone_verified;
