//! Adapts the chat composition root to the fleet [`service_runtime::Service`]
//! contract so the shared runtime can host it.
//!
//! All domain wiring stays in [`crate::app`]; this module only maps env → config,
//! defers to [`App::build`], registers the concrete tonic services, and reports
//! backend liveness via the storage crates' own probe constructors
//! ([`scylla_storage::health::probe`] / [`redis_storage::health::probe`]) over the
//! clients `App` retains. It is the seam that lets `chat-server` be a one-liner
//! over the shared runtime while the integration harness keeps driving
//! [`App::build`] directly.
//!
//! The probe abstraction ([`HealthProbe`]) lives in the `health` foundation crate,
//! so each storage crate exposes a ready-made probe for its client — no per-service
//! closures.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use infra_config::InfraRegistry;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, Service};
use service_runtime::edge::authenticated;
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::client::KafkaClientConfig;

use crate::app::{App, AppConfig, Backends};
use crate::application::port::InteractionGate;
use crate::infrastructure::client::GrpcInteractionGate;
use crate::config::ChatConfig;
use crate::infrastructure::grpc::handler::{ChatServiceHandler, ChatServiceServer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;

/// The concrete tonic server type for chat, named once so both the health key
/// and the reflection registration agree.
type ChatServer = ChatServiceServer<ChatServiceHandler<InMemoryCommandBus, InMemoryQueryBus>>;

/// The chat service as hosted by [`service_runtime`]. Owns the wired [`App`]
/// until it is consumed into the gRPC router.
pub struct ChatService {
    app: App,
}

#[async_trait]
impl Service for ChatService {
    const NAME: &'static str = "chat";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <ChatServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // Every actor field is bound to the caller in the handler
    // (`edge::require_profile`), StreamPublic's `subscriber_id` included: it
    // keys the audience shard and gates the member-only "not public" answer.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/chat.v1.ChatService/CreateConversation"),
        authenticated("/chat.v1.ChatService/OpenDirectConversation"),
        authenticated("/chat.v1.ChatService/RespondToMessageRequest"),
        authenticated("/chat.v1.ChatService/ToggleVisibility"),
        authenticated("/chat.v1.ChatService/JoinAsMember"),
        authenticated("/chat.v1.ChatService/InviteMember"),
        authenticated("/chat.v1.ChatService/Subscribe"),
        authenticated("/chat.v1.ChatService/Unsubscribe"),
        authenticated("/chat.v1.ChatService/SendMessage"),
        authenticated("/chat.v1.ChatService/MarkRead"),
        authenticated("/chat.v1.ChatService/SendTyping"),
        authenticated("/chat.v1.ChatService/Heartbeat"),
        authenticated("/chat.v1.ChatService/GetHistory"),
        authenticated("/chat.v1.ChatService/ListMembers"),
        authenticated("/chat.v1.ChatService/ListSubscriptions"),
        authenticated("/chat.v1.ChatService/ListInbox"),
        authenticated("/chat.v1.ChatService/StreamConversation"),
        authenticated("/chat.v1.ChatService/StreamPublic"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        // chat reads its tuning from the environment today; `_infra` is the seam
        // for migrating these knobs onto hot-reloadable `[traffic]`/`[cache]`
        // sections later without touching this signature.
        let config = ChatConfig::from_env();

        let app_config = AppConfig {
            max_page_size:               config.max_page_size,
            hot_tail_cache_size:         config.hot_tail_cache_size,
            message_bucket_hours:        config.message_bucket_hours,
            member_stream_buffer_size:   config.member_stream_buffer_size,
            audience_stream_buffer_size: config.audience_stream_buffer_size,
            audience_shard_count:        config.audience_shard_count,
            presence_ttl_secs:           config.presence_ttl_secs,
            typing_ttl_secs:             config.typing_ttl_secs,
            // Production reuses the presence TTL for the Audience Plane.
            audience_ttl_secs:           config.presence_ttl_secs,
            visibility_consumer_group:   "chat-visibility-consumer".to_owned(),
            presence_settings_consumer_group: "chat-presence-settings".to_owned(),
            inbox_consumer_group:        "chat-inbox".to_owned(),
        };

        let backends = Backends {
            scylla:           ScyllaConfig::from_env(),
            redis:            RedisConfig::from_env(),
            kafka:            Some(KafkaClientConfig::from_env()),
            interaction_gate: interaction_gate_from_env()?,
        };

        // `App::build` errors are `Box<dyn Error>` (not `Send + Sync`), so flatten
        // to a message rather than propagating the box into `anyhow`.
        let app = App::build(&app_config, backends)
            .await
            .map_err(|e| anyhow::anyhow!("chat app build: {e}"))?;

        // Memberships from before `conversations_by_member` existed (#653):
        // opt-in, once, idempotent (`CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER=true`).
        if std::env::var("CHAT_BACKFILL_CONVERSATIONS_BY_MEMBER").is_ok_and(|v| matches!(v.trim(), "1" | "true" | "yes")) {
            let members = Arc::clone(&app.member_repo);
            tokio::spawn(async move {
                use crate::application::port::MemberRepository;
                match members.backfill_member_index().await {
                    Ok(written) => tracing::info!(written, "conversations_by_member backfill done"),
                    Err(error) => tracing::error!(%error, "conversations_by_member backfill failed"),
                }
            });
        }

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe(self.app.redis.clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(ChatServiceServer::new(self.app.handler));
        Ok(())
    }
}

/// social-graph's `CheckInteraction`, from `CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT`
/// (#656). Unset: direct conversations off, invitations unchecked.
pub(crate) fn interaction_gate_from_env() -> anyhow::Result<Option<Arc<dyn InteractionGate>>> {
    let Some(endpoint) = std::env::var("CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT").ok().filter(|e| !e.trim().is_empty()) else {
        tracing::warn!("CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT unset: direct messages off, group invitations unchecked");
        return Ok(None);
    };
    let ms = |var: &str, default: u64| {
        std::time::Duration::from_millis(std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
    };
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid CHAT_SOCIAL_GRAPH_GRPC_ENDPOINT: {e}"))?
        .timeout(ms("CHAT_SOCIAL_GRAPH_RPC_TIMEOUT_MS", 1_000))
        .connect_timeout(ms("CHAT_SOCIAL_GRAPH_CONNECT_TIMEOUT_MS", 1_000))
        .connect_lazy();
    Ok(Some(Arc::new(GrpcInteractionGate::new(channel))))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A profile's conversations are the GDPR export's (#653): never on the
    /// edge.
    /// Direct messages (#656) are the caller's own profile's.
    #[test]
    fn direct_messages_are_on_the_edge_for_members() {
        for method in [
            "/chat.v1.ChatService/OpenDirectConversation",
            "/chat.v1.ChatService/RespondToMessageRequest",
            "/chat.v1.ChatService/ListInbox",
        ] {
            assert!(ChatService::EDGE_POLICY.iter().any(|rule| rule.method == method), "{method}");
        }
    }

    #[test]
    fn listing_by_member_is_mesh_only() {
        let method = "/chat.v1.ChatService/ListConversationsByMember";
        assert!(ChatService::EDGE_POLICY.iter().all(|rule| rule.method != method));
    }
}
