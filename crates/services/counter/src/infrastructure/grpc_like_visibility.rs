//! [`LikeVisibility`] over post's mesh-only `BatchGetLikeVisibility` (#809),
//! with a short per-instance cache: a post's author never changes, and an
//! author's like-count setting reaches readers within [`CACHE_TTL`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use post_api::post_service_client::PostServiceClient;
use tonic::transport::Channel;

use crate::application::port::{LikeVisibility, PostLikeVisibility};
use crate::error::CounterError;

/// How long an answer is reused.
pub const CACHE_TTL: Duration = Duration::from_secs(60);
/// Entries kept before the cache is emptied (a bound, not an LRU).
const CACHE_CAP: usize = 100_000;
/// post's per-call bound.
const BATCH: usize = 200;

type Cached = (Option<PostLikeVisibility>, Instant);

pub struct GrpcLikeVisibility {
    client: PostServiceClient<Channel>,
    cache:  Mutex<HashMap<String, Cached>>,
}

impl GrpcLikeVisibility {
    pub fn new(channel: Channel) -> Self {
        Self { client: PostServiceClient::new(channel), cache: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl LikeVisibility for GrpcLikeVisibility {
    async fn of(&self, post_ids: &[String]) -> Result<HashMap<String, PostLikeVisibility>, CounterError> {
        let mut known = HashMap::new();
        let mut missing = Vec::new();
        {
            let cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
            for id in post_ids {
                match cache.get(id).filter(|(_, at)| at.elapsed() < CACHE_TTL) {
                    Some((Some(v), _)) => {
                        known.insert(id.clone(), v.clone());
                    }
                    Some((None, _)) => {}
                    None => missing.push(id.clone()),
                }
            }
        }
        missing.sort();
        missing.dedup();
        for chunk in missing.chunks(BATCH) {
            let request = post_api::BatchGetLikeVisibilityRequest { post_ids: chunk.to_vec() };
            let found: HashMap<String, PostLikeVisibility> = self
                .client
                .clone()
                .batch_get_like_visibility(request)
                .await
                .map_err(|status| CounterError::PostUnavailable { reason: status.message().to_owned() })?
                .into_inner()
                .posts
                .into_iter()
                .map(|p| (p.post_id, PostLikeVisibility { author_id: p.author_id, hidden: p.like_counts_hidden }))
                .collect();
            let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
            if cache.len() >= CACHE_CAP {
                cache.clear();
            }
            let now = Instant::now();
            for id in chunk {
                cache.insert(id.clone(), (found.get(id).cloned(), now));
            }
            known.extend(found);
        }
        Ok(known)
    }
}
