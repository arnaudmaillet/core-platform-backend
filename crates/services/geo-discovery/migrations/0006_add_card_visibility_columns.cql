-- Online schema change: what the map may still show of a card, plus the post's
-- coordinates so a lifted suppression can rebuild the Radar pin.
--
-- suppressed:          0/NULL = shown · 1 = the post was deleted (permanent) ·
--                      2 = moderation removed or limited it (lifted by a reversal).
-- moderation_version:  moderation's per-subject EnforcementVersion that set or
--                      lifted the suppression; kept so a stale event cannot undo it.
-- lat / lng:           the post location (WGS-84). Absent on cards indexed before
--                      this migration; such a card cannot get its pin back after a
--                      reversal (it simply ages out with its 48 h TTL).
--
-- Every UPDATE of these columns carries `USING TTL` = the row's remaining life, so
-- a suppressed card still expires with its row. Metadata-only ALTERs; "already
-- exists" is a benign no-op for the migration runner (see 0005).
ALTER TABLE geo_discovery.map_post_cards ADD suppressed tinyint;
ALTER TABLE geo_discovery.map_post_cards ADD moderation_version bigint;
ALTER TABLE geo_discovery.map_post_cards ADD lat double;
ALTER TABLE geo_discovery.map_post_cards ADD lng double;
