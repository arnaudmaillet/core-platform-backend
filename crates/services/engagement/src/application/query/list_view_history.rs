//! A profile's viewing history (#944): the posts it viewed, most recently
//! viewed first, each once. Owner only: the gRPC layer binds the profile to
//! the edge token (the mesh reads any). And an account's history, every
//! profile's, for the GDPR export (mesh only).

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{ViewCursor, ViewHistory, ViewedPost};
use crate::application::query::list_saved_posts::{ACCOUNT_DEFAULT_LIMIT, ACCOUNT_MAX_LIMIT};
use crate::application::query::profile_tab::page_limit;
use crate::error::EngagementError;

pub struct ListViewHistoryQuery {
    pub profile_id: String,
    /// `0` ⇒ the default; capped (see [`page_limit`]).
    pub limit:      i32,
    /// Where the previous page ended.
    pub after:      Option<ViewCursor>,
}

/// A page of the history; `next` is where the following one starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewHistoryPage {
    pub posts: Vec<ViewedPost>,
    pub next:  Option<ViewCursor>,
}

impl Query for ListViewHistoryQuery {
    type Response = ViewHistoryPage;
}

pub struct ListViewHistoryByAccountQuery {
    pub account_id: String,
    /// `0` ⇒ [`ACCOUNT_DEFAULT_LIMIT`]; capped at [`ACCOUNT_MAX_LIMIT`].
    pub limit:      i32,
    /// The last `(profile_id, post_id)` of the previous page.
    pub after:      Option<(String, String)>,
}

impl Query for ListViewHistoryByAccountQuery {
    type Response = Vec<ViewedPost>;
}

pub struct ListViewHistoryHandler {
    /// `None`: this instance runs without the durable store.
    pub history: Option<Arc<dyn ViewHistory>>,
}

impl QueryHandler<ListViewHistoryQuery> for ListViewHistoryHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListViewHistoryQuery>) -> Result<ViewHistoryPage, EngagementError> {
        let query = &envelope.payload;
        if query.profile_id.is_empty() || query.profile_id.len() > 64 {
            return Err(EngagementError::DomainViolation { field: "profile_id".into(), message: query.profile_id.clone() });
        }
        let history = self.history.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let (posts, next) = history.list(&query.profile_id, page_limit(query.limit), query.after.as_ref()).await?;
        Ok(ViewHistoryPage { posts, next })
    }
}

impl QueryHandler<ListViewHistoryByAccountQuery> for ListViewHistoryHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<ListViewHistoryByAccountQuery>) -> Result<Vec<ViewedPost>, EngagementError> {
        let query = &envelope.payload;
        let history = self.history.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let limit = match query.limit {
            l if l <= 0 => ACCOUNT_DEFAULT_LIMIT,
            l => l.min(ACCOUNT_MAX_LIMIT),
        };
        let after = query.after.as_ref().map(|(profile, post)| (profile.as_str(), post.as_str()));
        history.list_by_account(&query.account_id, limit, after).await
    }
}
