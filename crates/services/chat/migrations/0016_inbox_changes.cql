-- Migration 0016: each member's inbox change journal (#930).
--
-- One row per change of a member's inbox entry — a new last message, a move
-- between folders, the entry taken out (left, declined), the member's read
-- position or mute — clustered oldest first. SyncInbox reads the rows after a
-- client's watermark and answers each conversation's entry as it is now (or
-- its removal). Rows expire after 35 days (past the client's 30-day
-- retention): an older watermark resyncs in full (CHT-2013).
CREATE TABLE IF NOT EXISTS chat.inbox_changes (
    member_id       uuid,
    changed_at      timestamp,
    conversation_id uuid,
    PRIMARY KEY ((member_id), changed_at, conversation_id)
) WITH CLUSTERING ORDER BY (changed_at ASC, conversation_id ASC)
  AND default_time_to_live = 3024000
  AND compaction  = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'A member''s inbox change journal, for incremental sync (#930).';
