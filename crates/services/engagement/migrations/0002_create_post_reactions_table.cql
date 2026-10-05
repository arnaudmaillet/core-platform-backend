-- Durable reaction ledger: one row per (post, profile).
-- PRIMARY KEY enforces the one-active-reaction-per-profile-per-post invariant at storage level.
-- UPSERTs are last-write-wins — no IF conditions needed (idempotent for write-behind).
-- This table is the source of truth for cold-start Redis reconstruction.
CREATE TABLE IF NOT EXISTS engagement.post_reactions (
    post_id    uuid,
    profile_id uuid,
    kind       tinyint,
    weight     int,
    reacted_at timestamp,
    PRIMARY KEY ((post_id), profile_id)
) WITH CLUSTERING ORDER BY (profile_id ASC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'Engagement reaction ledger. Redis is authoritative for real-time scores, this table provides durability and recovery.';
