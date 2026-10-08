-- wallet (#665 part 2): what a movement was for, when it names something
-- (a country unlock's country code).
ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS ref_id TEXT;
