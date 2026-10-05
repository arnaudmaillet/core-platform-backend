-- Migration 0008: pending invitations to join a conversation's Member Plane
--
-- Joining a PRIVATE conversation requires a pending invitation issued by an
--   owner/admin (InviteMember); JoinAsMember consumes (deletes) it. Public
--   conversations stay open-join and never need a row here.
--
-- Partition key: conversation_id — the invitations of one conversation are
--   bounded by the roster cap in practice and expire on their own, so a single
--   partition is safe. Clustering: invitee_id — the join path is a point lookup
--   ("does X hold an invitation?").
--
-- Expiry: default_time_to_live = 7 days. Re-inviting overwrites the row and
--   refreshes the TTL; an unused invitation simply ages out (no reaper).
--
-- Compaction: LCS — small, point-read partitions with short-lived rows; LCS keeps
--   read amplification and tombstone spread low.
CREATE TABLE IF NOT EXISTS chat.invitations_by_conversation (
    conversation_id uuid,
    invitee_id      uuid,
    inviter_id      uuid,
    invited_at      timestamp,
    PRIMARY KEY ((conversation_id), invitee_id)
) WITH CLUSTERING ORDER BY (invitee_id ASC)
  AND default_time_to_live = 604800
  AND compaction  = {'class': 'LeveledCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'}
  AND gc_grace_seconds = 86400
  AND comment = 'Pending Member-Plane invitations (7-day TTL). Required to join a private conversation.';
