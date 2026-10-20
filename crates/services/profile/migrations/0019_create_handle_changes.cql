-- Paid handle changes (#911): how many handle changes an account made
-- (counted by end users only), and the last one's time. The first change is
-- free; later ones cost gems; a caller who may not spend gems changes for free
-- once per 30 days. No row: no change yet. Grown one change at a time (LWT);
-- erased with the account.
CREATE TABLE IF NOT EXISTS profile.handle_changes (
    account_id     uuid PRIMARY KEY,
    changes        int,
    last_change_at timestamp
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
