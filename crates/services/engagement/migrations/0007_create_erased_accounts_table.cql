-- An account's likes are erased when it is deleted (#665): its entries go,
-- the targets' counts stay (the points are kept, anonymously). A stake it
-- made before the deletion may still land (the wallet outbox, a late batch):
-- the account is remembered here for 30 days so the stake consumer drops it.
CREATE TABLE IF NOT EXISTS engagement.erased_accounts (
    account_id uuid PRIMARY KEY,
    erased_at  timestamp
) WITH default_time_to_live = 2592000
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'Deleted accounts, while their stakes may still land.';
