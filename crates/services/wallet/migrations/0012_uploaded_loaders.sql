-- #975: the loaders an account uploaded, one per paid slot (cap 10, held at
-- once). Written with the slot's ledger row in one transaction (the wallet
-- row locked); a deleted loader's row goes and its slot is not given back.
-- `asset_id`: its media asset (`MEDIA_KIND_LOADER`), imported once;
-- `import_key`: the import's idempotency key. Erased with the account.
CREATE TABLE IF NOT EXISTS uploaded_loaders (
    account_id  UUID        NOT NULL,
    loader_id   UUID        NOT NULL,
    asset_id    UUID        NOT NULL,
    price_gems  BIGINT      NOT NULL,
    import_key  TEXT        NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, loader_id),
    UNIQUE (account_id, asset_id),
    UNIQUE (account_id, import_key)
);
