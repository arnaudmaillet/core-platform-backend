-- Ghost mode and location precision (#657), as one JSON text column. NULL ⇒
-- the defaults (not ghosted, precise). geo-discovery applies them to the map.
ALTER TABLE profile.profiles ADD location_settings text;
