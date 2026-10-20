-- #963: the media asset a card's post opens with (its first attachment's),
-- so a video country cover can play its preview loop. Metadata-only; NULL on
-- older rows (no preview). An "already exists" ALTER is a benign no-op for
-- the runner.
ALTER TABLE geo_discovery.map_post_cards
    ADD cover_asset_id uuid;
