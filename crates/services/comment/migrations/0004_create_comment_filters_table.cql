-- The post owners' comment filters (#660), projected from profile.v1.events
-- (ProfileCommentFiltersChanged): hidden words and the offensive filter. No
-- row ⇒ no hidden words, offensive filter on.
CREATE TABLE IF NOT EXISTS comment.comment_filters (
    profile_id        uuid PRIMARY KEY,
    hidden_words      list<text>,
    filter_offensive  boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
