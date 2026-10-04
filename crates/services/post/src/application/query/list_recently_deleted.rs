use std::sync::Arc;

use chrono::Utc;
use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{PostRepository, RecentlyDeleted},
    domain::{
        aggregate::{Post, RESTORE_WINDOW},
        value_object::{PostStatus, ProfileId},
    },
    error::PostError,
};

/// The author's deleted posts still restorable, newest deletion first (#663).
pub struct ListRecentlyDeletedQuery {
    pub profile_id: String,
    pub limit:      i32,
    pub page_token: Option<String>,
}

impl Query for ListRecentlyDeletedQuery {
    type Response = (Vec<Post>, Option<String>);
}

pub struct ListRecentlyDeletedHandler<R> {
    pub repository:       Arc<R>,
    pub recently_deleted: Arc<dyn RecentlyDeleted>,
}

impl<R: PostRepository> QueryHandler<ListRecentlyDeletedQuery> for ListRecentlyDeletedHandler<R> {
    type Error = PostError;

    /// Each entry is read back: one restored meanwhile (or not this author's)
    /// is dropped, so a page can come back short while the token stays valid.
    async fn handle(&self, envelope: Envelope<ListRecentlyDeletedQuery>) -> Result<(Vec<Post>, Option<String>), PostError> {
        let q = &envelope.payload;
        let author = ProfileId::try_from(q.profile_id.as_str())?;
        let (ids, next) = self.recently_deleted.list(&author, q.limit, q.page_token.as_deref()).await?;
        let now = Utc::now();
        let mut posts = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(post) = self.repository.find_by_id(&id).await?
                && post.status() == PostStatus::Deleted
                && post.profile_id().as_uuid() == author.as_uuid()
                && post.deleted_at().is_some_and(|at| now - at <= RESTORE_WINDOW)
            {
                posts.push(post);
            }
        }
        Ok((posts, next))
    }
}
