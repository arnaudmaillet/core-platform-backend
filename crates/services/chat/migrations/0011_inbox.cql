-- Migration 0011: each member's inbox (#656).
--
-- inbox_entries: one row per (member, conversation) — the authoritative
--   entry: its folder (0 = inbox, 1 = requests), last activity, kind, the
--   peer of a direct conversation, and the last delivered message's preview.
-- inbox_by_activity: the same entries clustered newest first per (member,
--   folder), for ListInbox. A move (new activity, new folder) deletes the old
--   clustering row and inserts the new one in the same LOGGED BATCH as the
--   entry, so the two never diverge.
--
-- Written by the inbox projector from chat's own facts (message sent, member
-- joined/left, conversation created): a withheld message moves only its
-- sender's entry.
CREATE TABLE IF NOT EXISTS chat.inbox_entries (
    member_id       uuid,
    conversation_id uuid,
    folder          tinyint,
    activity        timestamp,
    kind            tinyint,
    peer_id         uuid,
    last_message_id uuid,
    last_sender_id  uuid,
    last_content    tinyint,
    last_preview    text,
    PRIMARY KEY ((member_id), conversation_id)
) WITH compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'A member''s inbox entry per conversation (#656).';

CREATE TABLE IF NOT EXISTS chat.inbox_by_activity (
    member_id       uuid,
    folder          tinyint,
    activity        timestamp,
    conversation_id uuid,
    kind            tinyint,
    peer_id         uuid,
    last_message_id uuid,
    last_sender_id  uuid,
    last_content    tinyint,
    last_preview    text,
    PRIMARY KEY ((member_id, folder), activity, conversation_id)
) WITH CLUSTERING ORDER BY (activity DESC, conversation_id ASC)
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'A member''s inbox, newest activity first, per folder (#656).';
