-- Migration 0014: a member mutes a conversation's pushes (#654).
--
-- On the roster row, so leaving the conversation clears it and the per-message
-- push fan-out reads it with the roster it already lists. Null: not muted; a
-- far-future instant: until unmuted.
ALTER TABLE chat.members_by_conversation ADD muted_until timestamp;
