-- CreatePost idempotency keys (#876): a retried CreatePost with the same key
-- answers the post the first call created instead of creating a second draft.
-- One row per (author profile, client key). Claimed by an LWT with a 60 s TTL
-- while its post is being written (created = false), then rewritten for 24 h
-- once the post is stored (created = true). A crashed attempt's claim expires.
CREATE TABLE IF NOT EXISTS post.create_keys (
    profile_id       uuid,
    idempotency_key  text,
    post_id          uuid,
    created          boolean,
    PRIMARY KEY ((profile_id, idempotency_key))
) WITH default_time_to_live = 86400
  AND compression = {'sstable_compression': 'LZ4Compressor'};
