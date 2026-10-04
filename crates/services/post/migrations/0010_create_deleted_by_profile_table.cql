-- "Recently deleted" (#663): an author's deleted posts, newest deletion first.
-- Rows expire after 30 days, the restore window; RestorePost removes its row.
-- The posts themselves stay tombstoned in post.posts (status Deleted).
CREATE TABLE IF NOT EXISTS post.deleted_by_profile (
    profile_id  uuid,
    deleted_at  timestamp,
    post_id     uuid,
    PRIMARY KEY (profile_id, deleted_at, post_id)
) WITH CLUSTERING ORDER BY (deleted_at DESC, post_id DESC)
  AND default_time_to_live = 2592000
  AND compression = {'sstable_compression': 'LZ4Compressor'};
