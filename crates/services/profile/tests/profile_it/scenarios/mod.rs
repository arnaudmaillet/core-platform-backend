//! Scenario groups for the profile live suite, mapping to the testing standard's
//! axes: concurrency (handle-claim race), cache invalidation, and outbound event
//! emission, viewer-aware reads and account-level masking.

mod cache_invalidation;
mod event_emission;
mod handle_claim_race;
mod list_by_account;
mod viewer_and_account_masking;
