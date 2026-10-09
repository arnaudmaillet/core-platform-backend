-- Profile tabs (#829): a profile's Likes tab — the posts the profile liked,
-- newest posts first (post ids are UUIDv7: their text sorts by time). Written
-- by the stake consumer with the stake's time as the write timestamp, like the
-- other copies, so an account's erasure (stamped later) removes it for good.
CREATE TABLE IF NOT EXISTS engagement.liked_posts_by_profile (
    profile_id text,
    post_id    text,
    total      bigint,
    liked_at   timestamp,
    PRIMARY KEY ((profile_id), post_id)
) WITH CLUSTERING ORDER BY (post_id DESC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'The posts each profile liked (its Likes tab).';

-- Every profile of the account that liked the target: the erasure removes
-- each one's Likes-tab row.
ALTER TABLE engagement.likes_by_account ADD profile_ids set<text>;

-- Which tabs each profile's owner shows (profile.v1.events
-- ProfileTabSettingsChanged); NULL or no row ⇒ shown (the default).
CREATE TABLE IF NOT EXISTS engagement.profile_tabs (
    profile_id text PRIMARY KEY,
    show_likes boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'Whether each profile shows its Likes tab to others.';
