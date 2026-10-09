-- Profile tabs (#829): visitors see a profile's Reposts and Places tabs when its
-- owner shows them.
-- * posts_by_profile: whether each post is a repost (it has a parent) and
--   whether it carries a place, both fixed at creation. NULL (posts written
--   before) reads as false.
-- * author_post_windows: the owner's tab flags beside the post window, from
--   profile.v1.events ProfileTabSettingsChanged. NULL reads as shown (the
--   defaults).
ALTER TABLE post.posts_by_profile ADD is_repost boolean;
ALTER TABLE post.posts_by_profile ADD has_place boolean;
ALTER TABLE post.author_post_windows ADD show_reposts boolean;
ALTER TABLE post.author_post_windows ADD show_places boolean;
