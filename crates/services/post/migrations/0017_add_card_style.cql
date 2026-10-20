-- A post's card style (#914): a wallet catalogue item id (`card.<name>`) its
-- author's account owns, applied by the author; NULL: the default card. On
-- the profile's post list too (the author's profile grid shows it).
ALTER TABLE post.posts ADD card_style_id text;
ALTER TABLE post.posts_by_profile ADD card_style_id text;
