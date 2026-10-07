use std::sync::Arc;

use fred::interfaces::KeysInterface;
use once_cell::sync::Lazy;
use redis_storage::RedisClient;
use regex::Regex;
use serde::Deserialize;
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::producer::KafkaProducerHandle;
use uuid::Uuid;

use crate::application::port::{BlockCache, NotificationRepository, StreamRegistry, UnreadCounter};
use crate::application::port::stream_registry::NotificationPayload;
use crate::application::port::{NoPush, PushNotifier};
use crate::config::NotificationConfig;
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{
    NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind,
};
use crate::error::NotificationError;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "post.published";

// ── Mention regex ─────────────────────────────────────────────────────────────

/// Matches `[@handle](profile:UUID)` mention tokens in post captions.
/// Capture group 1 is the profile UUID string.
static MENTION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\[@[^\]]*\]\(profile:([0-9a-fA-F-]{36})\)")
        .expect("mention regex is valid")
});

// ── Payload shape ─────────────────────────────────────────────────────────────

/// Minimal projection of `post.published` Kafka events.
/// Unknown fields from the post service schema are silently ignored.
#[derive(Debug, Deserialize)]
pub struct PostPublishedPayload {
    pub post_id:   String,
    pub author_id: String,
    /// The raw post caption, used for mention token extraction.
    pub caption:   Option<String>,
    /// Publication timestamp (Unix ms), used as the notification `created_at` so
    /// the value is deterministic across redeliveries. Absent → 0.
    #[serde(default)]
    pub published_at_ms: i64,
}

// ── Key builders ──────────────────────────────────────────────────────────────

fn post_author_key(post_id: &str) -> String {
    format!("notification:pa:{}", post_id)
}

// ── Worker ────────────────────────────────────────────────────────────────────

/// Consumes `post.published` events and:
///
/// 1. Caches `notification:pa:{post_id}` → `author_profile_id` in Redis so the
///    `ReactionNotificationWorker` can resolve the notification target without a
///    cross-service call.
/// 2. Parses `[@handle](profile:UUID)` mention tokens from the post caption and
///    writes one `MENTION` notification per unique mentioned profile.
pub struct MentionNotificationWorker<R, B, U, S> {
    kafka_config: KafkaClientConfig,
    redis:        RedisClient,
    repository:   Arc<R>,
    block_cache:  Arc<B>,
    counter:      Arc<U>,
    stream_reg:   Arc<S>,
    push:         Arc<dyn PushNotifier>,
    config:       Arc<NotificationConfig>,
    group_id:     String,
}

impl<R, B, U, S> MentionNotificationWorker<R, B, U, S>
where
    R: NotificationRepository,
    B: BlockCache,
    U: UnreadCounter,
    S: StreamRegistry,
{
    /// Sends each notification written for the first time as a push (#654).
    pub fn with_push(mut self, push: Arc<dyn PushNotifier>) -> Self {
        self.push = push;
        self
    }

    #[allow(clippy::too_many_arguments)] // aggregate/worker constructor — same precedent as chat
    pub fn new(
        kafka_config: KafkaClientConfig,
        redis:        RedisClient,
        repository:   Arc<R>,
        block_cache:  Arc<B>,
        counter:      Arc<U>,
        stream_reg:   Arc<S>,
        config:       Arc<NotificationConfig>,
        group_id:     impl Into<String>,
    ) -> Self {
        Self {
            kafka_config,
            redis,
            repository,
            block_cache,
            counter,
            stream_reg,
            push: Arc::new(NoPush),
            config,
            group_id: group_id.into(),
        }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — mention notification consumer not started");
                return;
            }
        };

        // `Arc<Self>` so the per-message closure can capture an owned handle.
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => {
                    tracing::warn!("mention notification consumer exited cleanly — restarting");
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "mention notification consumer error — restarting after 5 s"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe(TOPIC)
            .build()
            .map_err(|e| e.to_string())?;

        tracing::info!(topic = TOPIC, group = %self.group_id, "mention notification consumer started");

        // `process` returns Ok for intentional skips (self-mention, block, empty
        // caption), so those commit cleanly; transient failures retry then
        // dead-letter, and malformed ids are dead-lettered as poison.
        let policy = RetryPolicy::default();
        run_consumer::<PostPublishedPayload, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &PostPublishedPayload) -> Result<(), NotificationError> {
        // Step 1: cache post author for the reaction worker.
        self.cache_post_author(&event.post_id, &event.author_id).await;

        // Step 2: extract mention UUIDs from caption.
        let caption = match event.caption.as_deref().filter(|c| !c.is_empty()) {
            Some(c) => c,
            None    => return Ok(()),
        };

        let sender_id = ProfileId::try_from(event.author_id.as_str())?;
        let subject_id = SubjectId::try_from(event.post_id.as_str())?;

        let mut seen: Vec<Uuid> = Vec::new();

        for cap in MENTION_REGEX.captures_iter(caption) {
            let uuid_str = &cap[1];
            let mentioned_uuid = match Uuid::parse_str(uuid_str) {
                Ok(u)  => u,
                Err(_) => {
                    tracing::warn!(uuid = uuid_str, "invalid UUID in mention token — skipping");
                    continue;
                }
            };

            // Deduplicate mentions of the same profile within one post.
            if seen.contains(&mentioned_uuid) {
                continue;
            }
            seen.push(mentioned_uuid);

            let target_id = ProfileId::from_uuid(mentioned_uuid);

            // Self-mention guard.
            if sender_id == target_id {
                continue;
            }

            // Block gate.
            match self.block_cache.is_blocked(&sender_id, &target_id).await {
                Ok(true) => {
                    tracing::debug!(
                        sender_id = %sender_id,
                        target_id = %target_id,
                        "mention notification suppressed by block"
                    );
                    continue;
                }
                Ok(false) => {}
                Err(err) => {
                    tracing::warn!(error = %err, "block cache error — proceeding without block check");
                }
            }

            // A post mentions each profile at most once, so (post, mentioned) is a
            // stable business key: deterministic id (idempotent INSERT) + claim-gated
            // unread increment. created_at is the post's publication time.
            let business_key = format!("mention:{}:{}", event.post_id, mentioned_uuid);
            let ntf_id       = NotificationId::deterministic(&business_key);
            let created_at   = chrono::DateTime::from_timestamp_millis(event.published_at_ms)
                .unwrap_or_default();

            let notification = Notification::create(
                ntf_id,
                target_id,
                sender_id,
                NotificationKind::Mention,
                SubjectKind::Post,
                subject_id,
                created_at,
            );

            self.repository.insert(&notification).await?;
            let first = self.counter.increment_once(&target_id, &business_key).await?;

            let payload = Arc::new(NotificationPayload {
                notification_id:   notification.id().as_uuid(),
                target_profile_id: notification.target_profile_id().as_uuid(),
                sender_profile_id: notification.sender_profile_id().as_uuid(),
                sample_sender_ids: notification.sample_sender_ids().to_vec(),
                sender_count:      notification.sender_count(),
                kind:              notification.kind(),
                subject_kind:      notification.subject_kind(),
                subject_id:        notification.subject_id().as_uuid(),
                created_at_ms:     notification.created_at().timestamp_millis(),
            });
            if first {
                self.push.notify(Arc::clone(&payload));
            }
            self.stream_reg.broadcast(&target_id, payload);

            tracing::debug!(
                post_id   = %event.post_id,
                mentioned = %target_id,
                "mention notification written"
            );
        }

        Ok(())
    }

    async fn cache_post_author(&self, post_id: &str, author_id: &str) {
        let key = post_author_key(post_id);
        let ttl = self.config.post_author_cache_ttl_secs;
        let _: Result<(), _> = self.redis.inner
            .set(
                &key,
                author_id,
                Some(fred::types::Expiration::EX(ttl as i64)),
                None,
                false,
            )
            .await;
    }
}
