-- The badges a profile shows (#912), in order: JSON
-- `[{"badge_id": "badge.continent_eu", "awarded_at": "<RFC 3339>"}]`, each a
-- wallet badge its account earned, at most three; NULL: none.
ALTER TABLE profile.profiles ADD badge_showcase text;
