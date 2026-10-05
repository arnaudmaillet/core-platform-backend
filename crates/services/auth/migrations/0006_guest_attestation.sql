-- App Attest (guest mode B5b): the attested key that vouched for a guest's
-- install, when App Attest verified it — the per-device identity abuse
-- controls (and the once-per-device welcome gift) can rely on, unlike the
-- client-chosen device_id.
ALTER TABLE guest_principals ADD COLUMN IF NOT EXISTS attest_key_id TEXT;
CREATE INDEX IF NOT EXISTS idx_guest_principals_attest_key
    ON guest_principals (attest_key_id)
    WHERE attest_key_id IS NOT NULL;
