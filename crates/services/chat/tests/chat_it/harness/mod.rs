//! Integration harness: boots the shared infra, wires a real service graph
//! against it through the production composition root, and exposes the handler
//! plus assertion back-doors.
//!
//! The graph is assembled by [`chat::app::App::build`] — the *same* entrypoint
//! production uses — with container-backed configs and per-scenario knobs
//! ([`HarnessOptions`]). The harness holds the same `Arc`s the handler holds, so
//! a test reads the live state the handler mutates (presence sets, the shard
//! registry, the in-process broadcast registries), never a parallel copy.
//!
//! Infra orchestration (the `OnceCell` containers, the RF=1 migration runner, and
//! the `await_until` anti-flake primitive) lives in the shared [`test_support`]
//! crate; only the chat-specific graph wiring and assertion helpers live here.
//!
//! Tests drive the service through the generated [`ChatService`] gRPC trait
//! in-process, which gives deterministic control over a stream's lifetime —
//! essential for the RAII and backpressure scenarios.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::broadcast;
use uuid::Uuid;

use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use transport::kafka::config::client::KafkaClientConfig;

use chat::app::{App, AppConfig, Backends};
use chat::application::port::{HotTailCache, PresenceSettingsStore, PresenceStore, RoutingRegistry};
use chat::infrastructure::grpc::handler::ChatServiceHandler;
use chat::infrastructure::streaming::ConversationBroadcastRegistry;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;

// ── Re-exports the scenarios drive the service through ───────────────────────

pub use chat::domain::value_object::{ConversationId, MessageId, ProfileId};
pub use chat::infrastructure::grpc::handler::chat_handler::proto;
pub use chat::infrastructure::grpc::handler::chat_handler::proto::chat_service_server::ChatService;
pub use chat::infrastructure::grpc::handler::chat_handler::StreamingParams;
pub use chat::infrastructure::routing::PlaneEvent;
pub use test_support::await_until;
pub use tonic::{Request, Status};

/// Generous default patience for a cross-component assertion (Redis pub/sub fan-out
/// round-trip, async Drop cleanup, etc.).
pub const DEADLINE: Duration = Duration::from_secs(10);
/// Patience for assertions that span a Kafka consumer-group join + consume.
pub const KAFKA_DEADLINE: Duration = Duration::from_secs(30);

/// ScyllaDB keyspace the migrations provision; passed to the RF=1 runner.
const KEYSPACE: &str = "chat";
/// ScyllaDB message-log bucket size (hours). Mirrors the production default.
const BUCKET_HOURS: u32 = 24;
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// Per-scenario knobs. Defaults mirror production; scenarios shrink buffers/TTLs
/// to make overflow and liveness assertions complete in seconds.
#[derive(Debug, Clone)]
pub struct HarnessOptions {
    pub member_buffer:        usize,
    pub audience_buffer:      usize,
    pub audience_shard_count: u16,
    pub presence_ttl_secs:    u64,
    pub typing_ttl_secs:      u64,
    pub audience_ttl_secs:    u64,
    pub hot_tail_cap:         u16,
    pub max_page_size:        i32,
    /// Boot Kafka, publish via the durable publisher, and run the
    /// [`VisibilityWorker`](chat::infrastructure::worker::VisibilityWorker). Off
    /// by default so scenarios 1–3 never boot Kafka.
    pub with_kafka:           bool,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            member_buffer:        256,
            audience_buffer:      1024,
            // One shard keeps every guest on shard 0, so audience refcount/privacy
            // assertions are deterministic without hunting for hash collisions.
            audience_shard_count: 1,
            presence_ttl_secs:    30,
            typing_ttl_secs:      6,
            audience_ttl_secs:    30,
            hot_tail_cap:         200,
            max_page_size:        100,
            with_kafka:           false,
        }
    }
}

/// A fully-wired chat service bound to ephemeral infra, plus assertion handles.
pub struct TestHarness {
    pub handler:           ChatServiceHandler<InMemoryCommandBus, InMemoryQueryBus>,
    pub presence:          Arc<dyn PresenceStore>,
    pub presence_settings: Arc<dyn PresenceSettingsStore>,
    pub routing:           Arc<dyn RoutingRegistry>,
    pub hot_tail:          Arc<dyn HotTailCache>,
    pub member_registry:   Arc<ConversationBroadcastRegistry>,
    pub audience_registry: Arc<ConversationBroadcastRegistry>,
    pub params:            StreamingParams,
}

impl TestHarness {
    /// Boots/reuses the shared containers, applies migrations, and assembles the
    /// service graph for these options through the production composition root.
    pub async fn start(opts: HarnessOptions) -> Self {
        // ── Shared containers (booted once per binary by `test_support`) ────────
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let kafka = if opts.with_kafka {
            let brokers = test_support::containers::kafka_brokers().await;
            test_support::containers::ensure_topics(
                &brokers,
                &[
                    "chat.conversation.created",
                    "chat.conversation.published",
                    "chat.conversation.unpublished",
                    "chat.conversation.unpublished.dlq",
                    "chat.member.joined",
                    "chat.member.left",
                    "chat.message.sent",
                ],
            )
            .await;
            Some(KafkaClientConfig::new(brokers))
        } else {
            None
        };

        // ── Backends + per-scenario config ──────────────────────────────────────
        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
            redis: RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
            kafka,
        };

        let config = AppConfig {
            max_page_size:               opts.max_page_size,
            hot_tail_cache_size:         opts.hot_tail_cap,
            message_bucket_hours:        BUCKET_HOURS,
            member_stream_buffer_size:   opts.member_buffer,
            audience_stream_buffer_size: opts.audience_buffer,
            audience_shard_count:        opts.audience_shard_count,
            presence_ttl_secs:           opts.presence_ttl_secs,
            typing_ttl_secs:             opts.typing_ttl_secs,
            audience_ttl_secs:           opts.audience_ttl_secs,
            // UUID-suffixed so parallel scenarios never share a consumer group.
            visibility_consumer_group:   format!("chat-it-visibility-{}", Uuid::now_v7()),
            presence_settings_consumer_group: format!("chat-it-presence-{}", Uuid::now_v7()),
        };

        let App {
            handler,
            presence,
            presence_settings,
            routing,
            hot_tail,
            member_registry,
            audience_registry,
            params,
            // Storage clients are retained on `App` for the runtime's readiness
            // probes; the harness drives the graph directly and doesn't need them.
            ..
        } = App::build(&config, backends).await.expect("integration: build chat app");

        Self {
            handler,
            presence,
            presence_settings,
            routing,
            hot_tail,
            member_registry,
            audience_registry,
            params,
        }
    }

    // ── Driving helpers (through the gRPC trait) ────────────────────────────────

    /// Creates a `Channel` (born `Public`); the owner is its sole roster member.
    pub async fn create_public_channel(&self, owner: &ProfileId) -> ConversationId {
        let resp = ChatService::create_conversation(
            &self.handler,
            Request::new(proto::CreateConversationRequest { kind: 1, owner_id: owner.as_str() }),
        )
        .await
        .expect("create_conversation");
        ConversationId::try_from(resp.into_inner().conversation_id.as_str())
            .expect("valid conversation id")
    }

    /// Creates a `Group` (born `Private`); the owner is its sole roster member.
    pub async fn create_private_group(&self, owner: &ProfileId) -> ConversationId {
        let resp = ChatService::create_conversation(
            &self.handler,
            Request::new(proto::CreateConversationRequest { kind: 0, owner_id: owner.as_str() }),
        )
        .await
        .expect("create_conversation");
        ConversationId::try_from(resp.into_inner().conversation_id.as_str())
            .expect("valid conversation id")
    }

    /// `JoinAsMember` for `profile`, returning the raw RPC outcome.
    pub async fn join(&self, conv: &ConversationId, profile: &ProfileId) -> Result<(), Status> {
        ChatService::join_as_member(
            &self.handler,
            Request::new(proto::JoinAsMemberRequest {
                conversation_id: conv.as_str(),
                profile_id:      profile.as_str(),
            }),
        )
        .await
        .map(drop)
    }

    /// `InviteMember` of `invitee` by `inviter`, returning the raw RPC outcome.
    pub async fn invite(
        &self,
        conv:    &ConversationId,
        inviter: &ProfileId,
        invitee: &ProfileId,
    ) -> Result<(), Status> {
        ChatService::invite_member(
            &self.handler,
            Request::new(proto::InviteMemberRequest {
                conversation_id: conv.as_str(),
                inviter_id:      inviter.as_str(),
                invitee_id:      invitee.as_str(),
            }),
        )
        .await
        .map(drop)
    }

    /// The roster's profile ids, as seen by `requester` (who must be a member).
    pub async fn roster(&self, conv: &ConversationId, requester: &ProfileId) -> Vec<String> {
        ChatService::list_members(
            &self.handler,
            Request::new(proto::ListMembersRequest {
                conversation_id: conv.as_str(),
                requester_id:    requester.as_str(),
            }),
        )
        .await
        .expect("list_members")
        .into_inner()
        .members
        .into_iter()
        .map(|m| m.profile_id)
        .collect()
    }

    /// Sends a text message and returns its server-minted id.
    pub async fn send_text(&self, conv: &ConversationId, sender: &ProfileId, body: &str) -> MessageId {
        let resp = ChatService::send_message(
            &self.handler,
            Request::new(proto::SendMessageRequest {
                conversation_id: conv.as_str(),
                sender_id:       sender.as_str(),
                content_type:    0, // Text
                body:            body.to_owned(),
                media_ref:       String::new(),
                reply_to:        String::new(),
            }),
        )
        .await
        .expect("send_message");
        MessageId::try_from(resp.into_inner().message_id.as_str()).expect("valid message id")
    }

    /// Opens a Member-Plane stream and returns the raw response stream.
    pub async fn open_member_stream(
        &self,
        conv:   &ConversationId,
        member: &ProfileId,
    ) -> ResponseStream<proto::StreamConversationResponse> {
        ChatService::stream_conversation(
            &self.handler,
            Request::new(proto::StreamConversationRequest {
                conversation_id: conv.as_str(),
                member_id:       member.as_str(),
            }),
        )
        .await
        .expect("stream_conversation")
        .into_inner()
    }

    /// Opens an Audience-Plane stream and returns the raw response stream.
    pub async fn open_public_stream(
        &self,
        conv:       &ConversationId,
        subscriber: &ProfileId,
    ) -> ResponseStream<proto::StreamPublicResponse> {
        ChatService::stream_public(
            &self.handler,
            Request::new(proto::StreamPublicRequest {
                conversation_id: conv.as_str(),
                subscriber_id:   subscriber.as_str(),
            }),
        )
        .await
        .expect("stream_public")
        .into_inner()
    }
}

/// Concrete shape of the handler's boxed server-streaming response.
pub type ResponseStream<T> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

// ── Test utilities ───────────────────────────────────────────────────────────

/// A fresh random profile id.
pub fn random_profile() -> ProfileId {
    ProfileId::from_uuid(Uuid::now_v7())
}

/// A fresh random message id (string form), for signal RPCs that take one.
pub fn random_message_id() -> String {
    Uuid::now_v7().to_string()
}

/// Current epoch-ms, as the service stamps liveness scores.
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Receives the next item from a response stream within `deadline`, or `None` on
/// timeout / stream end.
pub async fn recv<T>(stream: &mut ResponseStream<T>, deadline: Duration) -> Option<Result<T, Status>> {
    tokio::time::timeout(deadline, futures::StreamExt::next(stream))
        .await
        .ok()
        .flatten()
}

/// Receives the next in-process plane event from a raw registry tap within
/// `deadline`, or `None` on timeout.
pub async fn recv_event(
    rx:       &mut broadcast::Receiver<Arc<PlaneEvent>>,
    deadline: Duration,
) -> Option<Arc<PlaneEvent>> {
    match tokio::time::timeout(deadline, rx.recv()).await {
        Ok(Ok(event)) => Some(event),
        _ => None,
    }
}
