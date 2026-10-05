-- GDPR erasure (Art. 17): when `account` finally deletes an account, auth
-- deletes what it holds about it, including the guest it was before sign-up.
-- Guests are sharded on their own id, so that lookup visits every shard: index it.
CREATE INDEX IF NOT EXISTS idx_guest_principals_upgraded
    ON guest_principals (upgraded_to_account_id)
    WHERE upgraded_to_account_id IS NOT NULL;
