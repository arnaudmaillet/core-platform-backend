-- Hidden words and the offensive filter (#810), projected from
-- profile.v1.events (ProfileCommentFiltersChanged): what a member hides from
-- the message requests they get. No row ⇒ no hidden words, offensive filter on.
CREATE TABLE IF NOT EXISTS chat.message_filters (
    profile_id       uuid PRIMARY KEY,
    hidden_words     list<text>,
    filter_offensive boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
