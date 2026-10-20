-- Profile slots (#916): the slots an account owns — one free, more bought
-- with gems, or its profiles from before slots existed (grandfathered).
-- No row: one slot. Set once (LWT `IF NOT EXISTS`), then grown one slot at a
-- time (LWT `IF slots = n - 1`); kept when a profile is deleted, erased with
-- the account.
CREATE TABLE IF NOT EXISTS profile.profile_slots (
    account_id uuid PRIMARY KEY,
    slots      int
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
