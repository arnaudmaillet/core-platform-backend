-- The viewing history (#944): each profile's History tab, most recently
-- viewed first, each post once. Owner only. Every row expires 90 days after
-- its view (the default TTL); a re-view rewrites it. `account_id` (static)
-- names the profile's account, so a profile's deletion finds its rows in
-- `views_by_account`. Written with the view's time as the write timestamp;
-- a deletion (remove, clear, a profile's or an account's) is stamped later.
CREATE TABLE IF NOT EXISTS engagement.view_history_by_profile (
    profile_id text,
    viewed_at  timestamp,
    post_id    text,
    account_id uuid STATIC,
    PRIMARY KEY ((profile_id), viewed_at, post_id)
) WITH CLUSTERING ORDER BY (viewed_at DESC, post_id DESC)
  AND default_time_to_live = 7776000
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'The posts each profile viewed (its History tab), most recent first, 90 days.';

-- The last view of each post, by account then profile: the authority. A
-- history row whose time differs from this one's (a re-view racing another,
-- a partial write) is not listed; it is also what a re-view, a removal and
-- the GDPR export read. `profile_ids` (static) is every profile of the
-- account that viewed within the window: the erasure clears each one's
-- history.
CREATE TABLE IF NOT EXISTS engagement.views_by_account (
    account_id  uuid,
    profile_id  text,
    post_id     text,
    viewed_at   timestamp,
    profile_ids set<text> STATIC,
    PRIMARY KEY ((account_id), profile_id, post_id)
) WITH default_time_to_live = 7776000
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'Each account''s last view of each post, by profile then post, 90 days.';
