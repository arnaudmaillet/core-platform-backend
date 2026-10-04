-- Mutes (#659): what one profile chose to stop seeing of another, per scope.
-- Unlike a block, the muted profile is not told and nothing is severed.
--
--   1. "What does A mute?"         — partition scan on muter_id (ListMutes, and
--                                    the mesh's ListMutedProfiles for the feeds)
--   2. "Does A mute B, and how?"   — point read (GetRelationStatus)
--
-- Partition key  : muter_id
-- Clustering key : muted_id (paginates in UUID order)
CREATE TABLE IF NOT EXISTS social_graph.mutes (
    muter_id   uuid,
    muted_id   uuid,
    posts      boolean,
    stories    boolean,
    messages   boolean,
    muted_at   timestamp,
    PRIMARY KEY (muter_id, muted_id)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
