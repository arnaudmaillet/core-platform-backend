-- A profile's reactions, one row per post it reacted to (#653: the GDPR data
-- export, Art. 15/20, lists what a profile liked). The mirror of
-- engagement.post_reactions, written and deleted with it in one LOGGED BATCH
-- by the write-behind ledger. Reactions from before this table existed are
-- indexed by the opt-in backfill (ENGAGEMENT_BACKFILL_REACTIONS_BY_PROFILE).
--
-- Access pattern: WHERE profile_id = ? [AND post_id > ?] LIMIT ?
CREATE TABLE IF NOT EXISTS engagement.reactions_by_profile (
    profile_id uuid,
    post_id    uuid,
    kind       tinyint,
    reacted_at timestamp,
    PRIMARY KEY ((profile_id), post_id)
) WITH CLUSTERING ORDER BY (post_id ASC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'A profile''s reactions (mirror of post_reactions), for the GDPR export.';
