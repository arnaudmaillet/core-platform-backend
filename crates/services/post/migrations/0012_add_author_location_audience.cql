-- Who sees an author's location (#657): 0 = everyone, 1 = followers,
-- 2 = mutuals. NULL (rows from before) = everyone.
ALTER TABLE post.author_location_settings ADD audience tinyint;
