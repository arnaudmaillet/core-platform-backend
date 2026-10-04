-- Guest sessions (guest mode, B1): an anonymous installation browsing before
-- sign-up gets a session like a member's, minted with sub = "guest:<id>",
-- kind = "guest", perms = ["read:public"] and no profiles.

-- A session is a member's (an IdP-backed account) or a guest's. For a guest,
-- `account_id` holds the guest id: no account owns it, and nothing joins it to
-- `account` (there are no foreign keys here, by design).
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'member';

-- One row per guest: when and from which device it first appeared. Sign-up
-- reads it to credit the welcome gift once per device (B4); abuse controls
-- (B5) will verify the attestation and rate-limit per device.
CREATE TABLE IF NOT EXISTS guest_principals (
    guest_id         UUID         NOT NULL,
    device_id        TEXT         NOT NULL,
    -- An App Attest / DeviceCheck assertion was presented (not yet verified).
    attestation_sent BOOLEAN      NOT NULL DEFAULT FALSE,
    locale           TEXT,
    region_hint      TEXT,
    current_country  TEXT,
    first_seen_at    TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (guest_id)
);

CREATE INDEX IF NOT EXISTS idx_guest_principals_device
    ON guest_principals (device_id, first_seen_at);
