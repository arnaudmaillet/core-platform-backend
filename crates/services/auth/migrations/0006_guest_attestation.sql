-- App Attest (guest mode B5b): the attested key that vouched for a guest's
-- install, when App Attest verified it. It identifies an attested key, not a
-- device: the app can rotate it (generateKey + attestKey again; Apple
-- throttles attestations per device), so per-key counts are a speed bump, not
-- a once-per-device guarantee — that would need one attestation per install
-- then assertions with a counter.
ALTER TABLE guest_principals ADD COLUMN IF NOT EXISTS attest_key_id TEXT;
CREATE INDEX IF NOT EXISTS idx_guest_principals_attest_key
    ON guest_principals (attest_key_id)
    WHERE attest_key_id IS NOT NULL;
