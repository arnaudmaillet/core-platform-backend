-- #949: the post's kind on its card ('media' or 'text_only'), so a place page
-- can keep one kind. Metadata-only; NULL on older rows (unknown kind, kept by
-- every filter). An "already exists" ALTER is a benign no-op for the runner.
ALTER TABLE geo_discovery.map_post_cards
    ADD post_kind text;
