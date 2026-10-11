//! The realtime composition roots.
//!
//! [`Adapters::build`] is the I/O variant that constructs the live port adapters
//! from config (the Redis routing fabric + node hop, the auth-context verifier),
//! plus the concrete [`RedisClient`] / [`RedisSubscriber`] the runtime needs for
//! the health probe and the node-channel subscription. Both binaries build from
//! it: the gateway uses every field; the dispatcher uses only the registry + node
//! channel for fan-out.

use transport::grpc::revocation::{RevocationFailure, Revocations};
use std::sync::Arc;

use auth_context::{spawn_edge_decoder, AuthContextConfig, JwtDecoder, OidcClaimsExtractor};
use redis_storage::{RedisClient, RedisClientBuilder, RedisSubscriber, RedisSubscriberBuilder};

use crate::application::port::{ConnectionRegistry, NodeChannel, TokenVerifier};
use crate::config::RealtimeConfig;
use crate::infrastructure::auth_context_verifier::AuthContextTokenVerifier;
use crate::infrastructure::redis_connection_registry::RedisConnectionRegistry;
use crate::infrastructure::redis_node_channel::RedisNodeChannel;

/// The live adapter set the binaries build from.
pub struct Adapters {
    pub registry: Arc<dyn ConnectionRegistry>,
    pub node_channel: Arc<dyn NodeChannel>,
    pub verifier: Arc<dyn TokenVerifier>,
    /// Retained for the runtime's hot-tier liveness probe.
    pub redis: RedisClient,
    /// A dedicated subscriber connection for the gateway's node-channel
    /// (`SSUBSCRIBE`) loop.
    pub subscriber: RedisSubscriber,
    /// `auth`'s revocation state (#903), when `EDGE_REVOCATION_REDIS_HOSTS` is
    /// set: checked at the handshake and on every heartbeat. Fail open (TIER-1).
    pub revocations: Option<Revocations>,
}

impl Adapters {
    pub async fn build(config: &RealtimeConfig) -> anyhow::Result<Self> {
        let redis = RedisClientBuilder::new(config.redis.clone()).build().await?;
        let subscriber = RedisSubscriberBuilder::new(config.redis.clone()).build().await?;

        let registry: Arc<dyn ConnectionRegistry> = Arc::new(RedisConnectionRegistry::new(
            redis.clone(),
            config.registry_ttl.as_millis() as i64,
        ));
        let node_channel: Arc<dyn NodeChannel> = Arc::new(RedisNodeChannel::new(redis.clone()));
        let revocations = service_runtime::edge_revocation()
            .map(|check| Revocations::new(check, RevocationFailure::Open));
        let mut verifier = AuthContextTokenVerifier::new(build_decoder(&config.auth), config.device_claim.clone());
        match &revocations {
            Some(revocations) => verifier = verifier.with_revocations(revocations.clone()),
            None => tracing::warn!("EDGE_REVOCATION_REDIS_HOSTS unset: a revoked token keeps its socket (#903)"),
        }
        let verifier: Arc<dyn TokenVerifier> = Arc::new(verifier);

        Ok(Self {
            registry,
            node_channel,
            verifier,
            redis,
            subscriber,
            revocations,
        })
    }
}

/// Build the ES256 edge-token decoder and start its JWKS refresher — the shared
/// `auth-context` edge decoder (ES256 only: listing RS256 next to it made
/// `jsonwebtoken` reject every valid token as InvalidAlgorithm, so the handshake
/// failed closed on real tokens; see `auth_context::EDGE_ALGORITHMS`).
///
/// The refresher is detached (its `JoinHandle` is dropped, which keeps the task
/// running for the process lifetime); the cache it warms is shared with the
/// decoder. A cold start does not require the IdP to be reachable — verification
/// fails closed (`RTM-1001`) until the first successful JWKS fetch.
fn build_decoder(
    auth: &AuthContextConfig,
) -> Arc<JwtDecoder<auth_context::OidcClaims, OidcClaimsExtractor>> {
    spawn_edge_decoder(auth)
}
