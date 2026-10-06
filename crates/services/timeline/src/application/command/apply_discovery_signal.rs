use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{DiscoveryPool, InterestStore};
use crate::domain::value_object::{AuthorId, ContentLevel, PostId, ProfileId, Restriction};
use crate::error::TimelineError;

/// What happened to a post, as far as the discovery pool is concerned.
#[derive(Debug, Clone, PartialEq)]
pub enum DiscoverySignal {
    /// `tags`: the caption's hashtags, normalized.
    Published { post_id: String, author_id: String, published_at_ms: i64, tags: Vec<String> },
    Deleted { post_id: String },
    Restricted { post_id: String, restriction: Restriction, version: i64 },
    Popularity { post_id: String, score: f64 },
    /// `profile_id` newly reacted to the post (#662): its tags gain weight in
    /// the profile's interests.
    Reacted { post_id: String, profile_id: String, at_ms: i64 },
    /// The holder's personalisation setting (profile `FeedSettings`): off
    /// erases the interests and stops learning.
    Personalization { profile_id: String, on: bool },
    /// The profile was deleted: its interests go with it.
    ProfileErased { profile_id: String },
}

/// Feeds the discovery pool and the interest tags. Issued by the discovery
/// worker from `post.v1.events`, `moderation.v1.events`,
/// `counter.v1.popularity`, `engagement.reactions` and `profile.v1.events`.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplyDiscoverySignalCommand {
    pub signal: DiscoverySignal,
}

impl Command for ApplyDiscoverySignalCommand {}

impl Validate for ApplyDiscoverySignalCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let post_id = match &self.signal {
            DiscoverySignal::Published { post_id, .. }
            | DiscoverySignal::Deleted { post_id }
            | DiscoverySignal::Restricted { post_id, .. }
            | DiscoverySignal::Popularity { post_id, .. }
            | DiscoverySignal::Reacted { post_id, .. } => post_id,
            DiscoverySignal::ProfileErased { profile_id } | DiscoverySignal::Personalization { profile_id, .. } => {
                if profile_id.trim().is_empty() {
                    return Err(vec![FieldViolation::new("profile_id", "TML-VAL-021", "profile_id must not be empty")]);
                }
                return Ok(());
            }
        };
        if post_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("post_id", "TML-VAL-020", "post_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct ApplyDiscoverySignalHandler {
    pub pool:      Arc<dyn DiscoveryPool>,
    pub interests: Arc<dyn InterestStore>,
}

impl CommandHandler<ApplyDiscoverySignalCommand> for ApplyDiscoverySignalHandler {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<ApplyDiscoverySignalCommand>) -> Result<(), TimelineError> {
        match &envelope.payload.signal {
            DiscoverySignal::Published { post_id, author_id, published_at_ms, tags } => {
                let post_id = PostId::try_from(post_id.as_str())?;
                let author_id = AuthorId::try_from(author_id.as_str())?;
                self.pool.record_published(&post_id, &author_id, *published_at_ms, tags).await
            }
            DiscoverySignal::Deleted { post_id } => {
                self.pool.record_deleted(&PostId::try_from(post_id.as_str())?).await
            }
            DiscoverySignal::Restricted { post_id, restriction, version } => {
                let post_id = PostId::try_from(post_id.as_str())?;
                self.pool.record_restriction(&post_id, *restriction, *version).await
            }
            DiscoverySignal::Popularity { post_id, score } => {
                self.pool.record_popularity(&PostId::try_from(post_id.as_str())?, *score).await
            }
            DiscoverySignal::Reacted { post_id, profile_id, at_ms } => {
                let post_id = PostId::try_from(post_id.as_str())?;
                let profile_id = ProfileId::try_from(profile_id.as_str())?;
                // Only what the pool knows (posts of the window) and could
                // show teaches anything: not a post taken down or deleted.
                let Some(meta) = self.pool.meta(std::slice::from_ref(&post_id)).await?.remove(&post_id) else {
                    return Ok(());
                };
                if meta.tags.is_empty() || !meta.shown_at(ContentLevel::Standard) {
                    return Ok(());
                }
                self.interests.reinforce(&profile_id, &post_id, &meta.tags, *at_ms).await
            }
            DiscoverySignal::Personalization { profile_id, on } => {
                self.interests.set_personalized(&ProfileId::try_from(profile_id.as_str())?, *on).await
            }
            DiscoverySignal::ProfileErased { profile_id } => {
                self.interests.erase(&ProfileId::try_from(profile_id.as_str())?).await
            }
        }
    }
}
