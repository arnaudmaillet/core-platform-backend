-- Third-party app authorisations (#667): what an account let a partner app do
-- ("Sign in with" this app). Sharded on the account like its sessions. A
-- revoked grant keeps its row (revoked_at) until the account is erased; the
-- proof of consent and of its withdrawal lives on the audit plane
-- (auth.v1.events: app_authorized / app_authorization_revoked).
CREATE TABLE IF NOT EXISTS app_authorizations (
    account_id   UUID        NOT NULL,
    app_id       TEXT        NOT NULL,
    display_name TEXT        NOT NULL,
    -- https URL of the app's icon; empty when it has none.
    icon_url     TEXT        NOT NULL DEFAULT '',
    scopes       TEXT[]      NOT NULL,
    granted_at   TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,
    PRIMARY KEY (account_id, app_id)
);
