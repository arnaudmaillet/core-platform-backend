-- Migration: 0006_gdpr_export_key
-- Description: keep the delivered export's object key, not a presigned link
--              (#653): a link is a bearer credential, so none sits at rest (in
--              backups, replicas, support tooling) — the link is signed when
--              the GDPR record is read, for what is left of the 7 days.

ALTER TABLE accounts RENAME COLUMN gdpr_data_export_url TO gdpr_data_export_key;
