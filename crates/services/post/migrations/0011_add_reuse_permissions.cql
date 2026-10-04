-- Remix / original-sound reuse permissions (#669).
--   posts.allow_remix / allow_sound_reuse: a post's override (NULL ⇒ its
--     author's default).
--   author_reuse_settings: the authors' defaults, projected from
--     profile.v1.events (ProfileInteractionSettingsChanged); no row ⇒ allowed.
--   audio_origins: which post an original sound belongs to, so a post reusing
--     it can be checked against that post's permission.
ALTER TABLE post.posts ADD allow_remix boolean;
ALTER TABLE post.posts ADD allow_sound_reuse boolean;

CREATE TABLE IF NOT EXISTS post.author_reuse_settings (
    profile_id        uuid PRIMARY KEY,
    allow_remix       boolean,
    allow_sound_reuse boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS post.audio_origins (
    audio_id    uuid PRIMARY KEY,
    post_id     uuid,
    profile_id  uuid
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
