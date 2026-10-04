-- Migration 005: notification preferences (#654), one JSON document per
-- profile: push / email per category, pause, quiet hours, time zone. No row ⇒
-- the defaults (teens: quiet hours 22:00–07:00, written at their first device
-- registration).
CREATE TABLE IF NOT EXISTS notification.notification_preferences (
    profile_id   uuid PRIMARY KEY,
    preferences  text,
    updated_at   timestamp
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
