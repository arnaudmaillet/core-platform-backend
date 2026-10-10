//! [`LikeVisibility`] over post's mesh-only `BatchGetLikeVisibility` (#809),
//! with a short per-instance cache: a post's author never changes, and an
//! author's like-count setting reaches readers within [`CACHE_TTL`].

use std::collections::HashMap;
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
        Ok(self.of_many(std::slice::from_ref(&post_id.to_owned())).await?.pop().flatten())
    }

    /// The cached answers, and one call for the rest.
    async fn of_many(&self, post_ids: &[String]) -> Result<Vec<Option<PostLikeVisibility>>, EngagementError> {
        let cached = |post_id: &String| self.cache.get(post_id).filter(|e| e.1.elapsed() < CACHE_TTL).map(|e| e.0.clone());
        let mut answers: Vec<Option<Option<PostLikeVisibility>>> = post_ids.iter().map(cached).collect();
        let missing: Vec<String> =
            post_ids.iter().zip(&answers).filter(|(_, a)| a.is_none()).map(|(p, _)| p.clone()).collect();
        if !missing.is_empty() {
            let fetched: HashMap<String, PostLikeVisibility> = self
                .client
                .clone()
                .batch_get_like_visibility(post_api::BatchGetLikeVisibilityRequest { post_ids: missing.clone() })
                .await
                .map_err(|status| EngagementError::PostUnavailable { message: status.message().to_owned() })?
                .into_inner()
                .posts
                .into_iter()
                .map(|p| (p.post_id, PostLikeVisibility { author_id: p.author_id, hidden: p.like_counts_hidden }))
                .collect();
            if self.cache.len() + missing.len() > CACHE_CAP {
                self.cache.clear();
            }
            let now = Instant::now();
            for post_id in &missing {
                self.cache.insert(post_id.clone(), (fetched.get(post_id).cloned(), now));
            }
            for (post_id, answer) in post_ids.iter().zip(answers.iter_mut()) {
                if answer.is_none() {
                    *answer = Some(fetched.get(post_id).cloned());
                }
            }
        }
        Ok(answers.into_iter().map(Option::flatten).collect())
    }
}
