-- Downloads and like counts (#809): the authors' settings, projected from
-- profile.v1.events (ProfileInteractionSettingsChanged) next to the reuse
-- defaults; NULL (no row, or a row from before) ⇒ allowed / shown.
ALTER TABLE post.author_reuse_settings ADD allow_downloads boolean;
ALTER TABLE post.author_reuse_settings ADD show_like_counts boolean;
