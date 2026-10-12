-- Which authors are private (#919 step 3), projected from profile.v1.events
-- (ProfileVisibilityChanged, also emitted when a profile is created private).
-- A private author's post media is delivered SIGNED only. No row ⇒ public.
CREATE TABLE IF NOT EXISTS post.author_visibility (
    profile_id uuid PRIMARY KEY,
    private    boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
