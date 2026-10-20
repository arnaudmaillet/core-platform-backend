-- #892: the catalogue items an account bought with gems (themes, icons…), on
-- the account's shard. A free item is owned by every account and has no row.
-- Written with the purchase's ledger row in one transaction; erased with the
-- account.
CREATE TABLE IF NOT EXISTS owned_items (
    account_id  UUID        NOT NULL,
    item_id     TEXT        NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, item_id)
);
