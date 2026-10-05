use std::sync::Arc;

use fred::interfaces::KeysInterface;
use redis_storage::RedisClient;
use serde::Deserialize;
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::{BlockCache, NotificationRepository, StreamRegistry, UnreadCounter};
use crate::application::port::stream_registry::NotificationPayload;
use crate::config::NotificationConfig;
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{
    NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind,
};
use crate::error::NotificationError;
use crate::infrastructure::worker::build_dlq_producer;

/// Only `comment.created` drives notifications; deletions are handled by other
/// services, so this consumer does not subscribe to `comment.deleted`.
const TOPIC_CREATED: &str = "comment.created";

// ── Minimal event projection ──────────────────────────────────────────────────

/// Projection of `comment.created` / `comment.deleted` Kafka events.
/// Unknown fields from the comment service schema are silently ignored.
#[derive(Debug, Deserialize)]
pub struct CommentEventPayload {
    pub comment_id:    String,
    pub post_id:       String,
    pub author_id:     String,
    /// `None` for top-level comments; `Some(comment_id)` for replies.
    pub parent_id:     Option<String>,
    pub created_at_ms: i64,
    /// The post's owner restricted the author (#659): nobody is notified.
    /// Absent from older events (`false`).
    #[serde(default)]
    pub quiet:         bool,
}

// ── Key builders ──────────────────────────────────────────────────────────────

fn post_author_key(post_id: &str) -> String {
    format!("notification:pa:{}", post_id)
}

fn comment_author_key(comment_id: &str) -> String {
    format!("notification:ca:{}", comment_id)
}

// ── Worker ────────────────────────────────────────────────────────────────────

/// Consumes `comment.created` events and produces:
/// - `COMMENT` notifications → the post author (when a top-level comment is added).
/// - `REPLY` notifications   → the parent comment's author.
///
/// Side effect: caches comment-author entries in Redis so future reply events
/// can look up the parent's author without a cross-service call.
pub struct CommentNotificationWorker<R, B, U, S> {
    kafka_config: KafkaClientConfig,
    redis:        RedisClient,
    repository:   Arc<R>,
    block_cache:  Arc<B>,
    counter:      Arc<U>,
    stream_reg:   Arc<S>,
    config:       Arc<NotificationConfig>,
    group_id:     String,
}

impl<R, B, U, S> CommentNotificationWorker<R, B, U, S>
where
    R: NotificationRepository,
    B: BlockCache,
    U: UnreadCounter,
    S: StreamRegistry,
{
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
            config,
            group_id: group_id.into(),
        }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — comment notification consumer not started");
                return;
            }
        };

        // `Arc<Self>` so the per-message closure can capture an owned handle (the
        // returned futures then borrow only the event, satisfying the runner bound).
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => {
                    tracing::warn!("comment notification consumer exited cleanly — restarting");
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "comment notification consumer error — restarting after 5 s"
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
            .subscribe(TOPIC_CREATED)
            .build()
            .map_err(|e| e.to_string())?;

        tracing::info!(
            topic = TOPIC_CREATED,
            group = %self.group_id,
            "comment notification consumer started"
        );

        // The runner owns the decode → process → retry → dead-letter → commit loop.
        // `process` already folds intentional suppressions into `Ok`, so they commit
        // cleanly; transient failures retry then dead-letter; poison is dead-lettered.
        let policy = RetryPolicy::default();
        run_consumer::<CommentEventPayload, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Handles one `comment.created` event, as the runner hands it (public so
    /// the integration suite drives it without a broker).
    pub async fn process(&self, event: &CommentEventPayload) -> Result<(), NotificationError> {
        let sender_id  = ProfileId::try_from(event.author_id.as_str())?;
        let subject_id = SubjectId::try_from(event.post_id.as_str())?;

        // Always cache this comment's author for future reply lookups.
        self.cache_comment_author(&event.comment_id, &sender_id).await;

        // A restricted author's comment (#659) is seen by them and the post's
        // owner only, and notifies nobody — an intentional suppression.
        if event.quiet {
            tracing::debug!(comment_id = %event.comment_id, "comment notification suppressed: restricted author");
            return Ok(());
        }

        let (target_id, kind) = if let Some(ref parent_id_str) = event.parent_id {
            // REPLY: find the parent comment's author.
            let parent_author_key = comment_author_key(parent_id_str);
            let author_str: Option<String> = self.redis.inner.get(&parent_author_key).await
                .unwrap_or(None);

            // Cache miss is an intentional suppression (parent author not yet cached),
            // not a failure: succeed so the offset commits and we do not dead-letter.
            let target_str = match author_str {
                Some(s) => s,
                None => {
                    tracing::debug!(comment_id = %event.comment_id, "reply notification suppressed: comment-author cache miss");
                    return Ok(());
                }
            };

            let target_id = ProfileId::try_from(target_str.as_str())?;
            (target_id, NotificationKind::Reply)
        } else {
            // COMMENT: find the post author.
            let post_author_key = post_author_key(&event.post_id);
            let author_str: Option<String> = self.redis.inner.get(&post_author_key).await
                .unwrap_or(None);

            let target_str = match author_str {
                Some(s) => s,
                None => {
                    tracing::debug!(comment_id = %event.comment_id, "comment notification suppressed: post-author cache miss");
                    return Ok(());
                }
            };

            let target_id = ProfileId::try_from(target_str.as_str())?;
            (target_id, NotificationKind::Comment)
        };

        // Self-notification guard — an intentional suppression, not a failure.
        if sender_id == target_id {
            tracing::debug!(comment_id = %event.comment_id, "comment notification suppressed: self-notification");
            return Ok(());
        }

        // Block gate — an intentional suppression, not a failure.
        if self.block_cache.is_blocked(&sender_id, &target_id).await? {
            tracing::debug!(comment_id = %event.comment_id, "comment notification suppressed: sender blocked");
            return Ok(());
        }

        // One comment produces exactly one notification, so the comment id is a
        // stable business key: the id is deterministic (idempotent INSERT) and the
        // unread increment is claim-gated against redelivery. created_at is the
        // comment's own timestamp.
        let business_key = format!("comment:{}", event.comment_id);
        let ntf_id       = NotificationId::deterministic(&business_key);
        let created_at   = chrono::DateTime::from_timestamp_millis(event.created_at_ms)
            .unwrap_or_default();

        let notification = Notification::create(
            ntf_id,
            target_id,
            sender_id,
            kind,
            SubjectKind::Post,
            subject_id,
            created_at,
        );

        self.repository.insert(&notification).await?;
        self.counter.increment_once(&target_id, &business_key).await?;

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
        self.stream_reg.broadcast(&target_id, payload);

        tracing::debug!(
            comment_id        = %event.comment_id,
            target_profile_id = %target_id,
            kind              = kind.as_str(),
            "comment notification written"
        );

        Ok(())
    }

    async fn cache_comment_author(&self, comment_id: &str, author_id: &ProfileId) {
        let key = comment_author_key(comment_id);
        let ttl = self.config.comment_author_cache_ttl_secs;
        let _: Result<(), _> = self.redis.inner
            .set(
                &key,
                author_id.as_str(),
                Some(fred::types::Expiration::EX(ttl as i64)),
                None,
                false,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_quiet_flag_reads_false_on_events_from_before_it() {
        let old = r#"{"comment_id":"c","post_id":"p","author_id":"a","parent_id":null,"created_at_ms":1}"#;
        assert!(!serde_json::from_str::<CommentEventPayload>(old).unwrap().quiet);
        let quiet = r#"{"comment_id":"c","post_id":"p","author_id":"a","parent_id":null,"created_at_ms":1,"quiet":true}"#;
        assert!(serde_json::from_str::<CommentEventPayload>(quiet).unwrap().quiet);
    }
}
