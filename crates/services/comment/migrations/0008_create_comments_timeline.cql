-- A post's comments as one timeline (#988): top-level comments and replies
-- alike, newest first, for a conversation-style reading. `comments_by_post`
-- cannot answer it: its first clustering column is the thread slot.
--
-- Denormalised like `comments_by_post` (no N+1 on a page) and kept with it:
-- written and purged in the same LOGGED BATCH, nulled on a tombstone, its held
-- flag set with it. Comments written before this table existed are indexed by
-- the opt-in backfill (COMMENT_BACKFILL_TIMELINE=true at startup).
--
-- Access pattern:
--   WHERE post_id = ? [AND created_at = ? AND comment_id > ?] LIMIT ?
--   WHERE post_id = ? AND created_at < ? LIMIT ?
CREATE TABLE IF NOT EXISTS comment.comments_timeline (
    post_id    uuid,
    created_at timestamp,
    comment_id uuid,
    parent_id  uuid,        -- nil UUID for top-level comments
    author_id  uuid,
    status     tinyint,
    body       text,
    gif_url    text,
    gif_width  int,
    gif_height int,
    held       boolean,
    PRIMARY KEY ((post_id), created_at, comment_id)
) WITH CLUSTERING ORDER BY (created_at DESC, comment_id ASC)
  AND compaction  = {
      'class'                  : 'TimeWindowCompactionStrategy',
      'compaction_window_unit' : 'DAYS',
      'compaction_window_size' : 7
  }
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'A post''s comments, replies included, newest first (#988).';
