//! [`LikeVisibility`] over post's mesh-only `BatchGetLikeVisibility` (#809),
//! with a short per-instance cache: a post's author never changes, and an
//! author's like-count setting reaches readers within [`CACHE_TTL`].

use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use post_api::post_service_client::PostServiceClient;
use tonic::transport::Channel;

use crate::application::port::{LikeVisibility, PostLikeVisibility};
use crate::error::EngagementError;

/// How long an answer is reused.
pub const CACHE_TTL: Duration = Duration::from_secs(60);
/// Entries kept before the cache is emptied (a bound, not an LRU).
const CACHE_CAP: usize = 100_000;

pub struct GrpcLikeVisibility {
    client: PostServiceClient<Channel>,
    cache:  DashMap<String, (Option<PostLikeVisibility>, Instant)>,
}

impl GrpcLikeVisibility {
    pub fn new(channel: Channel) -> Self {
        Self { client: PostServiceClient::new(channel), cache: DashMap::new() }
    }
}

#[async_trait]
impl LikeVisibility for GrpcLikeVisibility {
    async fn of(&self, post_id: &str) -> Result<Option<PostLikeVisibility>, EngagementError> {
        if let Some(hit) = self.cache.get(post_id).filter(|e| e.1.elapsed() < CACHE_TTL) {
            return Ok(hit.0.clone());
        }
        let request = post_api::BatchGetLikeVisibilityRequest { post_ids: vec![post_id.to_owned()] };
        let answer = self
            .client
            .clone()
            .batch_get_like_visibility(request)
            .await
            .map_err(|status| EngagementError::PostUnavailable { message: status.message().to_owned() })?
            .into_inner()
            .posts
            .into_iter()
            .find(|p| p.post_id == post_id)
            .map(|p| PostLikeVisibility { author_id: p.author_id, hidden: p.like_counts_hidden });
        if self.cache.len() >= CACHE_CAP {
            self.cache.clear();
        }
        self.cache.insert(post_id.to_owned(), (answer.clone(), Instant::now()));
        Ok(answer)
    }
}
