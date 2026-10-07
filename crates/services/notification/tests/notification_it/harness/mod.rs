//! Integration harness: boots the shared infra, wires a real notification graph
//! against it through the production composition root, and exposes the gRPC
//! handler (for streams + queries), the command bus (for creates), the broadcast
//! registry (for reclamation assertions), and the unread counter.
//!
//! Kafka is never booted: scenarios create notifications by dispatching
//! [`CreateNotificationCommand`] directly — the same command the workers route —
//! which is faster and fully deterministic.
#![allow(dead_code)]

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::Stream;
use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, Envelope};
use redis_storage::{RedisClient, RedisConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig};
use tonic::{Request, Status};

use notification::app::{App, Backends, PushBackends};
use notification::application::command::create_notification::CreateNotificationCommand;
use notification::application::port::{NotificationRepository, PushOutcome, PushSender, SenderNames, UnreadCounter};
use notification::domain::device::Device;
use notification::domain::push_message::PushMessage;
use notification::error::NotificationError;
use notification::config::NotificationConfig;
use notification::infrastructure::streaming::BroadcastRegistry;

pub use notification::domain::value_object::ProfileId;
pub use notification::infrastructure::grpc::handler::notification_handler::{proto, NotificationServiceHandler};
pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (Redis round-trip,
/// broadcast fan-out, async sender reclamation).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "notification";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// Notification kinds / subject kinds as the create command expects them.
pub const KIND_REACTION: i32 = 1;
pub const SUBJECT_POST: i32 = 1;

/// The concrete gRPC handler type, with both buses shared by `Arc`.
pub type Handler =
    NotificationServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>, BroadcastRegistry>;

/// Concrete shape of the handler's boxed server-streaming response.
pub type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<proto::StreamNotificationsResponse, Status>> + Send + 'static>>;

/// A fully-wired notification service bound to ephemeral infra, plus handles.
/// APNs, as far as the suite goes (#654): every push sent, by device token.
#[derive(Default)]
pub struct CapturingPush {
    sent: Mutex<Vec<(String, PushMessage)>>,
}

impl CapturingPush {
    pub fn sent_to(&self, token: &str) -> Vec<PushMessage> {
        self.sent.lock().unwrap().iter().filter(|(t, _)| t == token).map(|(_, m)| m.clone()).collect()
    }
}

#[async_trait::async_trait]
impl PushSender for CapturingPush {
    async fn send(&self, device: &Device, message: &PushMessage) -> Result<PushOutcome, NotificationError> {
        self.sent.lock().unwrap().push((device.token.clone(), message.clone()));
        Ok(PushOutcome::Delivered)
    }
}

/// Every sender is called Alice.
pub struct AliceNames;

#[async_trait::async_trait]
impl SenderNames for AliceNames {
    async fn display_name(&self, _profile: &ProfileId) -> Option<String> {
        Some("Alice".into())
    }
}

pub struct TestHarness {
    pub handler:         Handler,
    /// The pushes the service sent.
    pub pushes:          Arc<CapturingPush>,
    pub command_bus:     Arc<InMemoryCommandBus>,
    pub stream_registry: Arc<BroadcastRegistry>,
    pub counter:         Arc<dyn UnreadCounter>,
    pub repository:      Arc<dyn NotificationRepository>,
    pub config:          Arc<NotificationConfig>,
    /// The live clients, to wire a worker against the same stores.
    pub scylla:          Arc<ScyllaClient>,
    pub redis:           RedisClient,
}

impl TestHarness {
    /// Boots/reuses the shared containers, applies migrations, and assembles the
    /// service graph (no Kafka).
    pub async fn start() -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let pushes = Arc::new(CapturingPush::default());
        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
            redis: RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
            kafka: None,
            push:  PushBackends { sender: Some(Arc::clone(&pushes) as _), names: Arc::new(AliceNames) },
        };

        let config = Arc::new(NotificationConfig::from_env());
        let app = App::build(Arc::clone(&config), backends)
            .await
            .expect("integration: build notification app");

        let handler = NotificationServiceHandler::new(
            Arc::clone(&app.command_bus),
            Arc::clone(&app.query_bus),
            Arc::clone(&app.stream_registry),
        );

        Self {
            handler,
            pushes,
            command_bus:     app.command_bus,
            stream_registry: app.stream_registry,
            counter:         app.counter,
            repository:      app.repository,
            config,
            scylla:          app.scylla,
            redis:           app.redis,
        }
    }

    /// Opens a broadcast stream for `profile` and returns the raw response stream.
    pub async fn open_stream(&self, profile: &ProfileId) -> ResponseStream {
        self.handler
            .stream_notifications(Request::new(proto::StreamNotificationsRequest {
                profile_id: profile.as_str(),
            }))
            .await
            .expect("stream_notifications")
            .into_inner()
    }

    /// Creates a notification from `sender` targeting `target` (server-minted id).
    pub async fn create(&self, target: &ProfileId, sender: &ProfileId) {
        dispatch_create(
            Arc::clone(&self.command_bus),
            target.as_str(),
            sender.as_str(),
        )
        .await
        .expect("create_notification");
    }
}

/// Dispatches a create on a shared bus — a free function so scenarios can fire
/// many concurrently from spawned tasks.
pub async fn dispatch_create(
    command_bus: Arc<InMemoryCommandBus>,
    target:      String,
    sender:      String,
) -> Result<(), cqrs::CqrsError> {
    let cmd = CreateNotificationCommand {
        notification_id:   Uuid::now_v7().to_string(),
        target_profile_id: target,
        sender_profile_id: sender,
        kind:              KIND_REACTION,
        subject_kind:      SUBJECT_POST,
        subject_id:        Uuid::now_v7().to_string(),
    };
    command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

/// A fresh random profile id.
pub fn random_profile() -> ProfileId {
    ProfileId::from_uuid(Uuid::now_v7())
}

/// Receives the next item from a response stream within `deadline`, or `None` on
/// timeout / stream end.
pub async fn recv(stream: &mut ResponseStream, deadline: Duration) -> Option<Result<proto::StreamNotificationsResponse, Status>> {
    tokio::time::timeout(deadline, futures::StreamExt::next(stream))
        .await
        .ok()
        .flatten()
}
