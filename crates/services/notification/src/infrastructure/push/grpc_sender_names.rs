//! [`SenderNames`] over profile's `GetProfileById` on the mesh, with a short
//! per-instance cache: a name change reaches alerts within [`CACHE_TTL`].

use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use profile_api::profile_service_client::ProfileServiceClient;
use profile_api::GetProfileByIdRequest;
use tonic::transport::Channel;

use crate::application::port::SenderNames;
use crate::domain::value_object::ProfileId;

/// How long a name is reused.
pub const CACHE_TTL: Duration = Duration::from_secs(300);
/// Entries kept before the cache is emptied (a bound, not an LRU).
const CACHE_CAP: usize = 50_000;

pub struct GrpcSenderNames {
    client: ProfileServiceClient<Channel>,
    cache:  DashMap<ProfileId, (Option<String>, Instant)>,
}

impl GrpcSenderNames {
    pub fn new(channel: Channel) -> Self {
        Self { client: ProfileServiceClient::new(channel), cache: DashMap::new() }
    }
}

#[async_trait]
impl SenderNames for GrpcSenderNames {
    async fn display_name(&self, profile: &ProfileId) -> Option<String> {
        if let Some(hit) = self.cache.get(profile).filter(|e| e.1.elapsed() < CACHE_TTL) {
            return hit.0.clone();
        }
        let request = GetProfileByIdRequest { profile_id: profile.as_str() };
        let name = match self.client.clone().get_profile_by_id(request).await {
            Ok(response) => {
                let view = response.into_inner();
                // The display name, else the handle.
                Some(view.display_name).filter(|n| !n.trim().is_empty()).or(Some(view.handle).filter(|h| !h.is_empty()))
            }
            Err(status) if status.code() == tonic::Code::NotFound => None,
            // Unreachable: no name this time, and not cached.
            Err(status) => {
                tracing::debug!(code = ?status.code(), "push without a name: profile unavailable");
                return None;
            }
        };
        if self.cache.len() >= CACHE_CAP {
            self.cache.clear();
        }
        self.cache.insert(*profile, (name.clone(), Instant::now()));
        name
    }
}
