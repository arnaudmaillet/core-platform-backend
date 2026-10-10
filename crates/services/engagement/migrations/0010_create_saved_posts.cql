-- Saved posts (#872): each profile's Saved tab, most recently saved first.
-- `account_id` (static) names the profile's account, so a profile's deletion
-- finds its rows in `saves_by_account`. Written with the save's time as the
-- write timestamp; a deletion (unsave, a profile's or an account's) is stamped
-- later and removes them for good.
CREATE TABLE IF NOT EXISTS engagement.saved_posts_by_profile (
    profile_id text,
    saved_at   timestamp,
    post_id    text,
    account_id uuid STATIC,
    PRIMARY KEY ((profile_id), saved_at, post_id)
) WITH CLUSTERING ORDER BY (saved_at DESC, post_id DESC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'The posts each profile saved (its Saved tab), newest saves first.';

-- The saves of an account, by profile then post: the authority. A tab row
-- whose save time differs from this one's (a race, a partial write) is not
-- listed; it is also what unsave and the GDPR export read. `profile_ids`
-- (static) is every profile of the account that ever saved, never shrunk by
-- an unsave: the erasure deletes each one's whole tab, unconfirmed rows too.
CREATE TABLE IF NOT EXISTS engagement.saves_by_account (
    account_id  uuid,
    profile_id  text,
    post_id     text,
    saved_at    timestamp,
    profile_ids set<text> STATIC,
    PRIMARY KEY ((account_id), profile_id, post_id)
) WITH compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'Each account''s saved posts, by profile then post.';

-- Whether the owner shows the Saved tab (ProfileTabSettingsChanged
-- show_saved); NULL or no row ⇒ hidden (the default).
ALTER TABLE engagement.profile_tabs ADD show_saved boolean;
