-- Online schema change: the moderation outcome post holds, from moderation.v1.events.
--
-- moderation_restriction: 0=None 1=Limited 2=AgeGated 3=Removed (ModerationRestriction).
-- moderation_version:     moderation's per-subject EnforcementVersion that set it;
--                         kept on reversal so a stale enforcement cannot re-apply.
--
-- Denormalized into posts_by_profile so ListPostsByProfile can drop a removed post
-- for non-authors without a lookup per row. Absent values read as NULL → the
-- aggregate reconstitutes "never moderated" (None, version 0), which is correct
-- for every pre-existing row. Metadata-only ALTER; "already exists" is a benign
-- no-op for the migration runner (see 0006).
ALTER TABLE post.posts ADD moderation_restriction tinyint;
ALTER TABLE post.posts ADD moderation_version bigint;
ALTER TABLE post.posts_by_profile ADD moderation_restriction tinyint;
ALTER TABLE post.posts_by_profile ADD moderation_version bigint;
