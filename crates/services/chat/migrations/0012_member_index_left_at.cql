-- Migration 0012: a membership's end, kept (#656 / #653).
--
-- Leaving a conversation removes the roster row but keeps the reverse-index
-- row with left_at set: the GDPR data export still finds the conversations a
-- profile left, and reads its own messages up to its departure
-- (GetFormerMemberHistory, mesh only). Rejoining clears left_at.
ALTER TABLE chat.conversations_by_member ADD left_at timestamp;
