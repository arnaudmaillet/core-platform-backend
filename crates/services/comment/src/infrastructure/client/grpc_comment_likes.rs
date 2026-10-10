//! Comments' like counts over gRPC: engagement's `BatchGetLikes` (#832), asked
//! as the mesh (no reader), 100 targets per call.

use std::collections::HashMap;

use async_trait::async_trait;
use engagement_api::engagement_service_client::EngagementServiceClient;
use engagement_api::{like_target, BatchGetLikesRequest, LikeTarget};
use tonic::transport::Channel;

use crate::application::port::CommentLikes;
use crate::domain::value_object::CommentId;
use crate::error::CommentError;

/// `BatchGetLikes` targets per call.
const MAX_TARGETS_PER_CALL: usize = 100;

/// The channel is `Arc`-backed and must carry a request timeout (tonic has
/// none by default).
pub struct GrpcCommentLikes {
    engagement: EngagementServiceClient<Channel>,
}

impl GrpcCommentLikes {
    pub fn new(engagement: Channel) -> Self {
        Self { engagement: EngagementServiceClient::new(engagement) }
    }
}

#[async_trait]
impl CommentLikes for GrpcCommentLikes {
    async fn counts(&self, comments: &[CommentId]) -> Result<HashMap<CommentId, i64>, CommentError> {
        let mut counts = HashMap::with_capacity(comments.len());
        for chunk in comments.chunks(MAX_TARGETS_PER_CALL) {
            let request = BatchGetLikesRequest {
                targets: chunk
                    .iter()
                    .map(|id| LikeTarget { target: Some(like_target::Target::CommentId(id.as_str())) })
                    .collect(),
            };
            let likes = self
                .engagement
                .clone()
                .batch_get_likes(request)
                .await
                .map_err(|status| CommentError::LikesUnavailable { reason: status.to_string() })?
                .into_inner()
                .likes;
            // In the request's order.
            for (id, view) in chunk.iter().zip(likes) {
                counts.insert(id.clone(), view.count);
            }
        }
        Ok(counts)
    }
}
