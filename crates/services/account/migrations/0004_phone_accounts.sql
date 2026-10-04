-- Phone-only accounts (guest mode B4c): an account signs up with a verified
-- phone number and no email. The email becomes optional, and a phone number
-- belongs to at most one account (like an email address already does).
ALTER TABLE accounts ALTER COLUMN email DROP NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS accounts_phone_uidx
    ON accounts (phone)
    WHERE phone IS NOT NULL;
