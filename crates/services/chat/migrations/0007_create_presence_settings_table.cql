-- Presence settings (#661), projected from profile.v1.events
-- (ProfileDiscoverySettingsChanged): whether a member's activity status and
-- read receipts reach the other members. No row ⇒ both shared (the default).
CREATE TABLE IF NOT EXISTS chat.presence_settings (
    profile_id       uuid PRIMARY KEY,
    activity_status  boolean,
    read_receipts    boolean
) WITH compression = {'sstable_compression': 'LZ4Compressor'};
