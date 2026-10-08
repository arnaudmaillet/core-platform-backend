-- Likes are points (#665): each account's total on each post or comment,
-- from wallet.v1.events StakeCommitted. The durable copy of the Redis likes,
-- written with the stake's time as the write timestamp (the newer total
-- wins whatever the order events land in).
--
-- By target: who liked it, how much (the like count is their sum).
CREATE TABLE IF NOT EXISTS engagement.likes_by_target (
    target_kind text,
    target_id   text,
    account_id  uuid,
    total       bigint,
    PRIMARY KEY ((target_kind, target_id), account_id)
) WITH compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'Each account''s likes (points) on a post or comment.';

-- By account: what it liked (the GDPR export, Art. 15/20).
CREATE TABLE IF NOT EXISTS engagement.likes_by_account (
    account_id  uuid,
    target_kind text,
    target_id   text,
    total       bigint,
    profile_id  text,
    liked_at    timestamp,
    PRIMARY KEY ((account_id), target_kind, target_id)
) WITH compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'What an account liked, for the GDPR export.';
