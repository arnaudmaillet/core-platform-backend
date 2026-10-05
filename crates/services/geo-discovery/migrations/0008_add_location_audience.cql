-- Migration 0008: who sees an author's location (#657): 0 = everyone,
-- 1 = followers, 2 = mutuals. NULL (rows from before) = everyone.
ALTER TABLE geo_discovery.location_settings ADD audience tinyint;
