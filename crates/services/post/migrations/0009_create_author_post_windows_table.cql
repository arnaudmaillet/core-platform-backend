-- The authors' post history window (#664), projected from profile.v1.events
-- (ProfileTabSettingsChanged): others see only posts created within the last
-- window_days. No row (or NULL) ⇒ every post.
CREATE TABLE IF NOT EXISTS post.author_post_windows (
    profile_id   uuid PRIMARY KEY,
    window_days  int
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
