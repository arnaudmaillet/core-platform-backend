-- #888: how sure a curator was (charter §6–§9), recorded and frozen for the
-- scoring to come (#889), with no effect on the stake. Per batch on its ledger
-- row, and the position's first batch's on the position. Level 1–4 (C1–C4);
-- NULL: a quick stake (no level chosen), or a row from before.
ALTER TABLE wallet_transactions ADD COLUMN IF NOT EXISTS conviction SMALLINT
    CHECK (conviction BETWEEN 1 AND 4);
ALTER TABLE stakes ADD COLUMN IF NOT EXISTS first_conviction SMALLINT
    CHECK (first_conviction BETWEEN 1 AND 4);
