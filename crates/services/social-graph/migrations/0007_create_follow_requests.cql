-- Pending follow requests to private profiles (a request is NOT a follow: it
-- never touches follow_status / followers / following, so every audience check
-- keeps ignoring it until the owner approves).
--
-- follow_requests — the owner's inbox: "Who asked to follow profile X?"
--   Partition key  : target_id (the private profile)
--   Clustering key : requested_at DESC, requester_id ASC (newest first; the id
--                    breaks same-millisecond ties)
--
-- follow_request_status — point lookup + delete-key store, like follow_status:
--   "Has A asked to follow B, and when?" (requested_at is the inbox's
--   clustering key, needed to delete the inbox row).
CREATE TABLE IF NOT EXISTS social_graph.follow_requests (
    target_id     uuid,
    requested_at  timestamp,
    requester_id  uuid,
    PRIMARY KEY (target_id, requested_at, requester_id)
) WITH CLUSTERING ORDER BY (requested_at DESC, requester_id ASC)
  AND compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS social_graph.follow_request_status (
    requester_id  uuid,
    target_id     uuid,
    requested_at  timestamp,
    PRIMARY KEY (requester_id, target_id)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
