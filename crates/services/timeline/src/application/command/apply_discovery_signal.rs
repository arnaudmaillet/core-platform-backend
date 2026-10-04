use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::DiscoveryPool;
use crate::domain::value_object::{AuthorId, PostId, Restriction};
use crate::error::TimelineError;

/// What happened to a post, as far as the discovery pool is concerned.
#[derive(Debug, Clone, PartialEq)]
pub enum DiscoverySignal {
    Published { post_id: String, author_id: String, published_at_ms: i64 },
    Deleted { post_id: String },
    Restricted { post_id: String, restriction: Restriction, version: i64 },
    Popularity { post_id: String, score: f64 },
}

/// Feeds the discovery pool. Issued by the discovery worker from
/// `post.v1.events`, `moderation.v1.events` and `counter.v1.popularity`.
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
            | DiscoverySignal::Popularity { post_id, .. } => post_id,
        };
        if post_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("post_id", "TML-VAL-020", "post_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct ApplyDiscoverySignalHandler {
    pub pool: Arc<dyn DiscoveryPool>,
}

impl CommandHandler<ApplyDiscoverySignalCommand> for ApplyDiscoverySignalHandler {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<ApplyDiscoverySignalCommand>) -> Result<(), TimelineError> {
        match &envelope.payload.signal {
            DiscoverySignal::Published { post_id, author_id, published_at_ms } => {
                let post_id = PostId::try_from(post_id.as_str())?;
                let author_id = AuthorId::try_from(author_id.as_str())?;
                self.pool.record_published(&post_id, &author_id, *published_at_ms).await
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
        }
    }
}
