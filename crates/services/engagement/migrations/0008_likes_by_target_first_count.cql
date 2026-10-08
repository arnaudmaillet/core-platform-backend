-- Stake settlement (#665): how early an account liked a target — the
-- target's like count just before the account's first like. Written with
-- every record of the account's total (the same value each time); null for
-- likes recorded before it was kept.
ALTER TABLE engagement.likes_by_target ADD first_count bigint;
