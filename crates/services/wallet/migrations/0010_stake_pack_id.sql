-- #941: several stake packs are on sale; a wallet remembers the pack its
-- shots came from (`shots_<n>`). NULL before #941 (the default pack) or before
-- any pack; kept once the shots run out.
ALTER TABLE wallets ADD COLUMN IF NOT EXISTS stake_pack_id TEXT;
