-- Migration 0009: reverse member index — the conversations a profile is a
-- member of (#653: the GDPR data export, Art. 15/20, gathers a profile's
-- conversations and their messages).
--
-- Partition key: member_id — bounded by how many groups/channels one profile
--   belongs to. Clustering: conversation_id ASC (paged by id).
--
-- Written and deleted with members_by_conversation in one LOGGED BATCH, so the
--   two never diverge. Memberships from before this table existed are indexed
--   by the opt-in backfill (CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER=true).
CREATE TABLE IF NOT EXISTS chat.conversations_by_member (
    member_id       uuid,
    conversation_id uuid,
    role            tinyint,
    joined_at       timestamp,
    PRIMARY KEY ((member_id), conversation_id)
) WITH CLUSTERING ORDER BY (conversation_id ASC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'Reverse member index (a profile''s conversations), for the GDPR export.';
