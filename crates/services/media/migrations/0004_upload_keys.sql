-- IssueUploadTicket idempotency keys (#876): a ticket retried under the same
-- key answers the asset the first one reserved instead of a second asset. The
-- key is stored on that asset; one live (not deleted) asset per owner and key,
-- so aborting or deleting an upload frees its key.
ALTER TABLE assets ADD COLUMN IF NOT EXISTS upload_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_owner_upload_key
    ON assets (owner_id, upload_key)
    WHERE upload_key IS NOT NULL AND state <> 'deleted';
