-- Presence and discoverability (#661): activity status, read receipts and how
-- people can find the profile, as one JSON column. NULL ⇒ the defaults
-- (everything on).
ALTER TABLE profile.profiles ADD discovery_settings text;
