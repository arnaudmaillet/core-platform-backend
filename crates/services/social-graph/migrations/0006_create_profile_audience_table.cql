-- Audience projection of profile state, from profile.v1.events, read by the
-- access check (CheckAccess, and the follower/following lists).
--
--   private — the owner made the profile private (ProfileVisibilityChanged):
--             its content is for followers only.
--   hidden  — moderation, an account suspension or a deletion hid the profile
--             (ProfileHidden / ProfileDeleted; ProfileRestored clears it).
--
-- No row = public and not hidden, the default for every profile. Columns are
-- written independently (an upsert per fact), so a visibility change never
-- clobbers the hidden flag or the reverse.
--
-- Partition key: profile_id (O(1) point read; `IN` over a page of targets).
CREATE TABLE IF NOT EXISTS social_graph.profile_audience (
    profile_id    uuid PRIMARY KEY,
    private       boolean,
    hidden        boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
