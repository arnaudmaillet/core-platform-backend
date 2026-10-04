-- Migration: 0002_create_reports
-- Description: The reporter's own record of each report (DSA Art. 16(5): the
--              notifier is told what became of the notice). One row per
--              (reporter, subject) — `id` is the deterministic UUIDv5 the
--              SubmitReport RPC returns, so a re-report keeps the first row. The
--              outcome is not stored: it is read from the case (`case_id`) the
--              report fed. Member and guest ids share one UUID space, so the
--              reporter is the pair (reporter_kind, reporter_id). Pure ANSI SQL.

CREATE TABLE IF NOT EXISTS reports (
    id             UUID         NOT NULL,
    reporter_kind  TEXT         NOT NULL, -- 'member' | 'guest'
    reporter_id    UUID         NOT NULL,
    case_id        UUID         NOT NULL,
    entity_type    TEXT         NOT NULL,
    entity_id      TEXT         NOT NULL,
    actor_id       UUID         NOT NULL, -- the reported account
    surface        TEXT         NOT NULL,
    category       TEXT         NOT NULL,
    reason         TEXT         NOT NULL,
    reported_at    TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (id)
);

-- ListMyReports: a reporter's reports, newest first (keyset on reported_at, id).
CREATE INDEX IF NOT EXISTS idx_reports_reporter
    ON reports (reporter_kind, reporter_id, reported_at DESC, id DESC);
