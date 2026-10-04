-- Migration 004: devices registered for push (#654).
--
-- push_devices        — a profile's devices (one row per install), read by the
--                       push sender (ResolvePushTargets).
-- push_device_tokens  — the reverse index: which profiles a push token is
--                       registered for, so registering a token for another
--                       account drops it from the previous one (a shared or
--                       handed-over phone never receives the old account's
--                       pushes).
CREATE TABLE IF NOT EXISTS notification.push_devices (
    profile_id     uuid,
    device_id      text,
    push_token     text,
    platform       text,
    environment    text,
    account_id     text,
    registered_at  timestamp,
    PRIMARY KEY (profile_id, device_id)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS notification.push_device_tokens (
    push_token  text,
    profile_id  uuid,
    device_id   text,
    account_id  text,
    PRIMARY KEY (push_token, profile_id)
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
