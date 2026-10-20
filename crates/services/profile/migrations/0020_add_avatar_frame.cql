-- The avatar frame a profile wears (#913): a wallet catalogue item id
-- (`frame.<name>`) its account owns; NULL: none. On the account's profile
-- list too, which the app's account switcher draws.
ALTER TABLE profile.profiles ADD avatar_frame_id text;
ALTER TABLE profile.profiles_by_account ADD avatar_frame_id text;
