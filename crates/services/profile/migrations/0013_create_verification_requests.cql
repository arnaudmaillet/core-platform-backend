-- Verification requests (#668). The latest request per profile (the owner's
-- view of its status), and the staff queue of pending ones, oldest first.
CREATE TABLE IF NOT EXISTS profile.verification_requests (
    profile_id    uuid PRIMARY KEY,
    request       text
) WITH compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS profile.verification_queue (
    bucket        text,
    submitted_at  timestamp,
    profile_id    uuid,
    PRIMARY KEY ((bucket), submitted_at, profile_id)
) WITH CLUSTERING ORDER BY (submitted_at ASC, profile_id ASC)
  AND compression = {'sstable_compression': 'LZ4Compressor'};
