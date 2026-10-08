//! What an account liked (#653 GDPR export, #665: likes are points): each
//! post or comment, its points, the profile that liked and when — from the
//! durable copy, paged by target. Mesh only.

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{AccountLike, LikeLedger};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

pub const DEFAULT_LIMIT: i32 = 100;
pub const MAX_LIMIT: i32 = 500;

pub struct ListLikesByAccountQuery {
    pub account_id: String,
    /// `0` ⇒ [`DEFAULT_LIMIT`]; capped at [`MAX_LIMIT`].
    pub limit:      i32,
    /// The last target of the previous page (`kind:id`).
    pub after:      Option<LikeTarget>,
}

impl Query for ListLikesByAccountQuery {
    type Response = Vec<AccountLike>;
}

pub struct ListLikesByAccountHandler {
    /// `None`: this instance runs without the durable copy.
    pub ledger: Option<Arc<dyn LikeLedger>>,
}

impl QueryHandler<ListLikesByAccountQuery> for ListLikesByAccountHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListLikesByAccountQuery>) -> Result<Vec<AccountLike>, EngagementError> {
        let query = &envelope.payload;
        let ledger = self.ledger.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let limit = match query.limit {
            l if l <= 0 => DEFAULT_LIMIT,
            l => l.min(MAX_LIMIT),
        };
        ledger.list_by_account(&query.account_id, limit, query.after.as_ref()).await
    }
}
