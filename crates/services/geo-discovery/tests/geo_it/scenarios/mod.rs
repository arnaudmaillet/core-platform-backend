//! Scenario groups for the geo-discovery live suite, mapping to the testing
//! standard's spatial/temporal-partitioning axis: H3 viewport indexing and the
//! spatial filter that bounds a query, plus what the map may still show
//! (deletes, moderation, the reader's audience) and for how long (retention).

mod radar_focus;
mod viewport_query;
mod map_visibility;
mod card_retention;
mod country_scope;
