-- A profile's QR / share-link token (#661; `token` is reserved in CQL): random, revocable, independent
-- of the handle. share_token_by_profile holds each profile's current token
-- (issued with an LWT, rotated with a compare-and-set); share_tokens resolves
-- a token to its profile. A token resolves only while both agree, so a
-- rotated-out token never comes back, whatever a crash left behind.
CREATE TABLE IF NOT EXISTS profile.share_token_by_profile (
    profile_id  uuid PRIMARY KEY,
    share_token text
) WITH compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS profile.share_tokens (
    share_token text PRIMARY KEY,
    profile_id  uuid
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
