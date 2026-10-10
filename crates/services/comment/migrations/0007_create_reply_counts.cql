-- Replies per top-level comment (#832): "View N more replies". A counter per
-- (post, parent), +1 when a reply is written, -1 when it is purged (a reply
-- has no replies, so its deletion is always a purge). Held replies count.
CREATE TABLE IF NOT EXISTS comment.reply_counts (
    post_id    uuid,
    parent_id  uuid,
    replies    counter,
    PRIMARY KEY ((post_id), parent_id)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
