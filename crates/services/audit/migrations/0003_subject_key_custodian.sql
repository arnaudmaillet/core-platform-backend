-- Which custodian wrapped each per-subject DEK (#485): 'local' (the env KEK,
-- AES-256-GCM) or 'kms'. The cipher unwraps a row with *its* custodian, not the
-- currently configured one, and re-wraps it under the configured one — so
-- switching KEK custody (env KEK → KMS) never strands an existing DEK.
--
-- Nullable + additive: a row written before this column is read by its shape
-- (an AES-GCM wrap carries a nonce, a KMS blob none) and tagged when re-wrapped.
ALTER TABLE subject_keys ADD COLUMN IF NOT EXISTS custodian TEXT;
