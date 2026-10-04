-- Feed controls (#662): the sensitive-content level, as one JSON column. NULL
-- ⇒ the default (less).
ALTER TABLE profile.profiles ADD feed_settings text;
