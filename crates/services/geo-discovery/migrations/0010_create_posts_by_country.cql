-- A country's posts, newest first (#833): the place page of a country marker
-- lists them page by page, beyond the map's per-tile Top-K. One partition per
-- (country, UTC day) so no partition grows without bound; rows carry the
-- post's map retention as their TTL, like posts_by_tile.
CREATE TABLE IF NOT EXISTS geo_discovery.posts_by_country (
    country      text,
    day          date,
    published_at timestamp,
    post_id      uuid,
    PRIMARY KEY ((country, day), published_at, post_id)
) WITH CLUSTERING ORDER BY (published_at DESC, post_id ASC)
  AND compaction = {
      'class'                  : 'TimeWindowCompactionStrategy',
      'compaction_window_unit' : 'DAYS',
      'compaction_window_size' : 1
  }
  AND compression = {'sstable_compression': 'LZ4Compressor'};
