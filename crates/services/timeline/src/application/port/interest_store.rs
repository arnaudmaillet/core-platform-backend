use async_trait::async_trait;

use crate::domain::value_object::{Interest, PostId, ProfileId};
use crate::error::TimelineError;

/// A profile's interest tags (#662), learnt from its reactions and decaying
/// with time (see [`crate::domain::value_object::interest`]).
#[async_trait]
pub trait InterestStore: Send + Sync + 'static {
    /// `profile` reacted to `post` (tagged `tags`) at `at_ms`: each tag gains
    /// weight, except those the profile removed. A post counts once per
    /// dedup window, so a redelivery or a react-again changes nothing.
    async fn reinforce(
        &self,
        profile: &ProfileId,
        post:    &PostId,
        tags:    &[String],
        at_ms:   i64,
    ) -> Result<(), TimelineError>;

    /// The heaviest `limit` tags at `now_ms`, heaviest first (those too light
    /// to matter left out).
    async fn top(&self, profile: &ProfileId, now_ms: i64, limit: usize) -> Result<Vec<Interest>, TimelineError>;

    /// Drops `tag` and keeps it out: later reactions no longer teach it.
    async fn remove(&self, profile: &ProfileId, tag: &str) -> Result<(), TimelineError>;

    /// Forgets everything about `profile`: weights, the posts counted and the
    /// removed tags (a reset, and the erasure of a deleted profile).
    async fn reset(&self, profile: &ProfileId) -> Result<(), TimelineError>;
}
