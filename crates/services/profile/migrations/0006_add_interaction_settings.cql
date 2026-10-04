-- Who may comment / mention / message, downloads and like counts (#656), as one
-- JSON text column. Existing rows read NULL ⇒ the defaults (everyone, downloads
-- on, like counts shown) — no backfill.
ALTER TABLE profile.profiles ADD interaction_settings text;
