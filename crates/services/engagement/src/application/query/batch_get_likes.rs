//! The likes of several posts and comments at once (#665: a feed page, a
//! comment thread): each one's count — withheld on a post whose author hides
//! like counts (#809), unless the reader is the author — and the reader's own.

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{LikeLedger, LikeStore, LikeVisibility};
use crate::application::query::get_post_engagement::{read_likes, EngagementReader, LikeSummary};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// Targets one call may read.
pub const MAX_TARGETS: usize = 100;

pub struct BatchGetLikesQuery {
    pub targets: Vec<LikeTarget>,
    pub reader:  EngagementReader,
    /// The reader's account (a member on the edge): its own likes.
    pub account: Option<String>,
}

impl Query for BatchGetLikesQuery {
    type Response = Vec<LikeSummary>;
}

pub struct BatchGetLikesHandler {
    pub like_store:  Arc<dyn LikeStore>,
    /// The durable copy, for a reader whose likes expired from Redis.
    pub like_ledger: Option<Arc<dyn LikeLedger>>,
    pub likes:       Option<Arc<dyn LikeVisibility>>,
}

impl QueryHandler<BatchGetLikesQuery> for BatchGetLikesHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<BatchGetLikesQuery>) -> Result<Vec<LikeSummary>, EngagementError> {
        let query = &envelope.payload;
        if query.targets.len() > MAX_TARGETS {
            return Err(EngagementError::DomainViolation {
                field:   "targets".into(),
                message: format!("at most {MAX_TARGETS} per call"),
            });
        }
        read_likes(&self.like_store, self.like_ledger.as_ref(), self.likes.as_ref(), &query.reader, query.account.as_deref(), &query.targets).await
    }
}
