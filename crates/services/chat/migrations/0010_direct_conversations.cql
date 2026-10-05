-- Migration 0010: direct conversations and message requests (#656).
--
-- chat.conversations gains a direct conversation's state (NULL for groups
-- and channels):
--   peer_id       the other member (owner_id opened it)
--   request_state 0 = open, 1 = a pending request, 2 = declined
--   requester_id  who asked (pending / declined)
--   request_sent  the request's one message is spent
--   declined_at   when it was declined (30 days before asking again)
-- Request transitions are compare-and-set (LWT IF request_state = ?), as is
-- spending the one message (IF request_sent = false): two taps never send two.
ALTER TABLE chat.conversations ADD peer_id uuid;
ALTER TABLE chat.conversations ADD request_state tinyint;
ALTER TABLE chat.conversations ADD requester_id uuid;
ALTER TABLE chat.conversations ADD request_sent boolean;
ALTER TABLE chat.conversations ADD declined_at timestamp;

-- A message shown to its sender only: the recipient blocks the sender, or
-- the reverse. NULL = shown to every member, as every message before it.
ALTER TABLE chat.messages_by_conversation ADD withheld boolean;

-- One direct conversation per pair of profiles: (low, high) by UUID order,
-- claimed with INSERT ... IF NOT EXISTS so two concurrent openers share it.
CREATE TABLE IF NOT EXISTS chat.direct_conversations (
    low_id          uuid,
    high_id         uuid,
    conversation_id uuid,
    PRIMARY KEY ((low_id, high_id))
) WITH compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND comment = 'The direct conversation of each pair of profiles (#656).';
