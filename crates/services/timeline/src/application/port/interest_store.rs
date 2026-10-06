use async_trait::async_trait;

use crate::domain::value_object::{Interest, PostId, ProfileId};
use crate::error::TimelineError;

/// A profile's interest tags (#662), learnt from its reactions and decaying
/// with time (see [`crate::domain::value_object::interest`]).
#[async_trait]
pub trait InterestStore: Send + Sync + 'static {
    /// `profile` reacted to `post` (tagged `tags`) at `at_ms`: each tag gains
    /// weight, except those the profile removed. A post counts once per
    /// dedup window, so a redelivery or a react-again changes nothing. A
    /// profile that opted out of personalisation learns nothing.
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

    /// Forgets what `profile` taught: weights, the posts counted and the
    /// removed tags (the holder's reset). An opt-out stays.
    async fn reset(&self, profile: &ProfileId) -> Result<(), TimelineError>;

    /// The holder's personalisation setting (#662, profile
    /// `FeedSettings.non_personalized`, enforced here): off erases what was
    /// learnt and stops learning until it is turned back on.
    async fn set_personalized(&self, profile: &ProfileId, on: bool) -> Result<(), TimelineError>;

    /// Forgets everything about a deleted profile, the opt-out included.
    async fn erase(&self, profile: &ProfileId) -> Result<(), TimelineError>;
}
