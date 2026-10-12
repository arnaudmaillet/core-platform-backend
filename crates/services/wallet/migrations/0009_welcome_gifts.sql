-- #827: the welcome gift a guest's device gets when it becomes a member, once
-- per device, whoever signs in. One row per device (SHA-256 of its App Attest
-- key, else its device id — never the raw id), on the nil UUID's shard only:
-- a device is claimed before its gift is credited on the account's shard.
-- `account_id` is cleared when the account is erased; the device stays
-- claimed (a deleted account's device is not welcomed twice).
CREATE TABLE IF NOT EXISTS welcome_gifts (
    device_key_hash TEXT        PRIMARY KEY,
    account_id      UUID,
    claimed_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS welcome_gifts_by_account ON welcome_gifts (account_id) WHERE account_id IS NOT NULL;
