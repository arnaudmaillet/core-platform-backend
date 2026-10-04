-- A brand profile's public contact card (#668), as one JSON column. NULL for
-- every other kind. (profile_kind itself becomes updatable by its owner.)
ALTER TABLE profile.profiles ADD business_info text;
