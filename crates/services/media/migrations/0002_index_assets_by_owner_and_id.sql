-- A holder's assets in id order (#653: the GDPR data export lists every asset
-- an account owns, paged by id). Supersedes idx_assets_owner for that read.
CREATE INDEX IF NOT EXISTS idx_assets_owner_id ON assets (owner_id, id);
