-- Restrictions (#659): profiles an owner restricted. A restricted profile's
-- comments on the owner's posts are seen only by that profile and the owner
-- (comment's read gate asks ListRestrictedAmong). The restricted profile is
-- not told and nothing is severed.
--
--   1. "Whom does A restrict?"                — partition scan (ListRestricted)
--   2. "Which of these commenters does A restrict?" — IN on the clustering key
--
-- Partition key  : owner_id
-- Clustering key : restricted_id (paginates in UUID order)
CREATE TABLE IF NOT EXISTS social_graph.restrictions (
    owner_id       uuid,
    restricted_id  uuid,
    restricted_at  timestamp,
    PRIMARY KEY (owner_id, restricted_id)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
