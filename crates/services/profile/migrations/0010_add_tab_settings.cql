-- Profile post history window and per-tab visibility (#664), as one JSON
-- column. NULL ⇒ the defaults (all posts; likes, reposts, places shown; saved
-- hidden).
ALTER TABLE profile.profiles ADD tab_settings text;
