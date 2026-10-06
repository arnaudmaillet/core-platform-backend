//! Like-count visibility for a batch of posts (#809), for the services that
//! serve counts (engagement, counter): each post's author and whether that
//! author hides like counts from others. Mesh only.

use std::collections::HashMap;
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::future::try_join_all;
use validate_core::{FieldViolation, Validate};

use crate::application::port::{PostRepository, ReuseRegistry};
use crate::domain::value_object::{PostId, ProfileId};
use crate::error::PostError;

/// Most posts one call names.
pub const MAX_LIKE_VISIBILITY_POSTS: usize = 200;

pub struct GetLikeVisibilityQuery {
    pub post_ids: Vec<String>,
}

/// One post's author and whether they hide like counts from others.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeVisibility {
    pub post_id:            PostId,
    pub author_id:          ProfileId,
    pub like_counts_hidden: bool,
}

impl Query for GetLikeVisibilityQuery {
    /// Posts the store does not know are absent.
    type Response = Vec<LikeVisibility>;
}

impl Validate for GetLikeVisibilityQuery {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.post_ids.len() > MAX_LIKE_VISIBILITY_POSTS {
            return Err(vec![FieldViolation::new("post_ids", "PST-VAL-090", "at most 200 posts per call")]);
        }
        Ok(())
    }
}

pub struct GetLikeVisibilityHandler<R> {
    pub repository: Arc<R>,
    pub authors:    Arc<dyn ReuseRegistry>,
}

impl<R: PostRepository> QueryHandler<GetLikeVisibilityQuery> for GetLikeVisibilityHandler<R> {
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<GetLikeVisibilityQuery>) -> Result<Vec<LikeVisibility>, PostError> {
        let mut ids: Vec<PostId> =
            envelope.payload.post_ids.iter().map(|id| PostId::try_from(id.as_str())).collect::<Result<_, _>>()?;
        ids.sort_by_key(PostId::as_str);
        ids.dedup();
        let posts = try_join_all(ids.iter().map(|id| self.repository.find_by_id(id))).await?;
        let authors: Vec<ProfileId> = {
            let mut a: Vec<ProfileId> = posts.iter().flatten().map(|p| p.profile_id().clone()).collect();
            a.sort_by_key(ProfileId::as_str);
            a.dedup();
            a
        };
        let settings = try_join_all(authors.iter().map(|a| self.authors.defaults(a))).await?;
        let hidden: HashMap<ProfileId, bool> =
            authors.into_iter().zip(settings).map(|(a, s)| (a, !s.show_like_counts)).collect();
        Ok(posts
            .into_iter()
            .flatten()
            .map(|post| LikeVisibility {
                post_id:            post.id().clone(),
                author_id:          post.profile_id().clone(),
                like_counts_hidden: hidden.get(post.profile_id()).copied().unwrap_or(false),
            })
            .collect())
    }
}
