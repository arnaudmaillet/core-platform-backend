-- Held comments (#669): a comment accepted while the post owner's temporary
-- interaction limit covers its author is held for the owner's review — seen
-- by its author and the owner only — until approved (or declined). NULL ⇒ not
-- held.
ALTER TABLE comment.comments ADD held boolean;
ALTER TABLE comment.comments_by_post ADD held boolean;
