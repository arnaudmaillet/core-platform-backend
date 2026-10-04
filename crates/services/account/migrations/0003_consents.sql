-- Migration: 0003_consents
-- Description: GDPR Art. 7 consent management. A third, separately-given
--              consent (product analytics) next to data processing and
--              marketing, and an append-only history of every consent given or
--              withdrawn — the controller must be able to demonstrate consent
--              (Art. 7(1)) and withdrawing must be as easy as giving it (7(3)).
--              The current state stays on `accounts`; the history is the
--              evidence. CockroachDB-compatible.

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS gdpr_analytics_consented_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS account_consent_history (
    account_id      UUID         NOT NULL,
    purpose         TEXT         NOT NULL, -- 'data_processing' | 'marketing' | 'analytics'
    granted         BOOLEAN      NOT NULL,
    policy_version  TEXT,
    changed_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (account_id, changed_at, purpose)
);
