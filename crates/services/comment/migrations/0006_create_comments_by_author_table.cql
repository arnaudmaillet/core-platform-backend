-- A profile's own comments, newest first (#653: the GDPR data export, Art.
-- 15/20, lists what a profile wrote). Only pointers: the comment itself is read
-- from comment.comments (the source of truth: its current status and body).
--
-- Written with every new comment, deleted with a purge; a tombstoned comment
-- keeps its row (its tombstone is what the export shows). Comments written
-- before this table existed are indexed by the opt-in backfill
-- (COMMENT_BACKFILL_AUTHOR_INDEX=true at startup).
--
-- Access pattern: WHERE author_id = ? [AND (created_at, comment_id) < (?, ?)] LIMIT ?
CREATE TABLE IF NOT EXISTS comment.comments_by_author (
    author_id  uuid,
    created_at timestamp,
    comment_id uuid,
    post_id    uuid,
    PRIMARY KEY ((author_id), created_at, comment_id)
) WITH CLUSTERING ORDER BY (created_at DESC, comment_id DESC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'A profile''s comments by time (pointers into comment.comments), for the GDPR export.';
