//! Whose posts these are (#809), for engagement's profile tabs. Like counts
//! are never hidden any more (#973): the answer only names each post's
//! author. Mesh only.

use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::future::try_join_all;
use validate_core::{FieldViolation, Validate};

use crate::application::port::PostRepository;
use crate::domain::value_object::{PostId, ProfileId};
use crate::error::PostError;

/// Most posts one call names.
pub const MAX_LIKE_VISIBILITY_POSTS: usize = 200;

pub struct GetLikeVisibilityQuery {
    pub post_ids: Vec<String>,
}

/// One post's author.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeVisibility {
    pub post_id:   PostId,
    pub author_id: ProfileId,
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
}

impl<R: PostRepository> QueryHandler<GetLikeVisibilityQuery> for GetLikeVisibilityHandler<R> {
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<GetLikeVisibilityQuery>) -> Result<Vec<LikeVisibility>, PostError> {
        let mut ids: Vec<PostId> =
            envelope.payload.post_ids.iter().map(|id| PostId::try_from(id.as_str())).collect::<Result<_, _>>()?;
        ids.sort_by_key(PostId::as_str);
        ids.dedup();
        let posts = try_join_all(ids.iter().map(|id| self.repository.find_by_id(id))).await?;
        Ok(posts
            .into_iter()
            .flatten()
            .map(|post| LikeVisibility { post_id: post.id().clone(), author_id: post.profile_id().clone() })
            .collect())
    }
}
