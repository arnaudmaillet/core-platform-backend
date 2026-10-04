-- A guest that signed up or signed in on its device: which account it became.
-- The guest session ends at that moment (revocation reason `guest_upgraded`);
-- the row stays, linked, for the welcome gift's once-per-device rule and for
-- moving guest-keyed data later.
ALTER TABLE guest_principals ADD COLUMN IF NOT EXISTS upgraded_to_account_id UUID;
ALTER TABLE guest_principals ADD COLUMN IF NOT EXISTS upgraded_at TIMESTAMPTZ;
