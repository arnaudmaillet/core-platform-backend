-- Migration: 0005_gdpr_export_delivery
-- Description: the delivered GDPR data export (#653, Art. 15/20): its download
--              link (a credential, shown to the holder only) and when it stops
--              working, and a partial index for the export pass's batch —
--              accounts whose export was asked for and not delivered since.

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS gdpr_data_export_url TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS gdpr_data_export_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS accounts_gdpr_export_pending_idx
    ON accounts (gdpr_data_export_requested_at)
    WHERE gdpr_data_export_requested_at IS NOT NULL;
