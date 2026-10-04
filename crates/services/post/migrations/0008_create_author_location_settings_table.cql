-- The authors' location sharing (#657), projected from profile.v1.events
-- (ProfileLocationSettingsChanged). Applied to every read of a post's location
-- by anyone but its author: ghost ⇒ none, city ⇒ the centre of its R5 cell.
-- No row ⇒ precise.
CREATE TABLE IF NOT EXISTS post.author_location_settings (
    profile_id uuid PRIMARY KEY,
    ghost      boolean,
    city       boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
