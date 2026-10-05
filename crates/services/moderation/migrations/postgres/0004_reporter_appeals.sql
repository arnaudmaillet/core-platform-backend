-- Migration: 0004_reporter_appeals
-- Description: A reporter may appeal the outcome of their report (DSA Art.
--              20(1): the notifier is a complainant too). `appellant` says who
--              appealed — the sanctioned account (every appeal so far) or a
--              reporter. Pure ANSI SQL.

ALTER TABLE appeals ADD COLUMN IF NOT EXISTS appellant TEXT NOT NULL DEFAULT 'sanctioned';
