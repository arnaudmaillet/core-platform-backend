//! Scenario groups for the post live suite, mapping to the testing standard's
//! axes: concurrency / dual-table consistency, lifecycle event emission and
//! viewer-aware reads, and the authors' location sharing.

mod dual_table_consistency;
mod lifecycle_events;
mod location_sharing;
mod post_window;
mod recently_deleted;
mod viewer_visibility;
mod sound_reuse;
mod mentions;
mod author_display;
