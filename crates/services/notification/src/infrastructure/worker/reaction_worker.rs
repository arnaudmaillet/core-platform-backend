use std::collections::HashMap;
use std::sync::Arc;

use fred::interfaces::{LuaInterface, SortedSetsInterface};
use redis_storage::RedisClient;
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
use crate::infrastructure::worker::collapse::{CollapseBuffer, CollapseKey, SCHEDULE_KEY};

/// Likes are points (#665): a like notice comes from the wallet's stakes.
const TOPIC: &str = "wallet.v1.events";

// ── Lua scripts ───────────────────────────────────────────────────────────────

/// Atomically increments the subject heat counter with a rolling 5-minute TTL.
///
/// KEYS[1] = notification:hot:{subject_id}
/// ARGV[1] = hot_ttl_secs (e.g. 300)
/// Returns: the new counter value.
const INCR_HOT_SCRIPT: &str = r#"
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local v   = redis.call('INCR', key)
if v == 1 then
    redis.call('EXPIRE', key, ttl)
end
return v
"#;

/// Accumulates a sender into the cross-batch collapse window.
///
/// Both keys share a Redis Cluster hash tag (see [`CollapseKey::redis_window_key`]),
/// so the script is slot-safe. The flush schedule ZSET is intentionally NOT touched
/// here — it lives on a different slot and is updated by a separate `ZADD` in
/// [`ReactionNotificationWorker::accumulate_in_window`].
///
/// All three keys share the window hash tag, so the script is slot-safe. The
/// unique-sender SET makes accumulation idempotent: a sender that already reacted
/// within this window is a no-op, so a redelivered reaction never double-counts.
///
/// KEYS[1] = notification:cw:{target:subject:kind}            (INCR counter)
/// KEYS[2] = notification:cw:{target:subject:kind}_:senders   (RPUSH sample list)
/// KEYS[3] = notification:cw:{target:subject:kind}_:sset      (unique-sender SET)
/// ARGV[1] = sender_id (string)
/// ARGV[2] = window_ttl_secs
/// ARGV[3] = max_sample_senders
/// Returns: count of unique senders in the window.
const COLLAPSE_ACCUMULATE_SCRIPT: &str = r#"
local window_key      = KEYS[1]
local senders_key     = KEYS[2]
local senders_set_key = KEYS[3]
local sender_id       = ARGV[1]
local ttl             = tonumber(ARGV[2])
local max_sample      = tonumber(ARGV[3])

-- Idempotency gate: skip senders already counted in this window.
if redis.call('SADD', senders_set_key, sender_id) == 0 then
    local existing = redis.call('GET', window_key)
    if existing then return tonumber(existing) end
    return 0
end

local count = redis.call('INCR', window_key)
if count == 1 then
    redis.call('EXPIRE', window_key,      ttl)
    redis.call('EXPIRE', senders_key,     ttl + 10)
    redis.call('EXPIRE', senders_set_key, ttl + 10)
end

local slen = redis.call('LLEN', senders_key)
if slen < max_sample then
    redis.call('RPUSH', senders_key, sender_id)
end

return count
"#;

/// Checks the hourly notification cap and increments it atomically.
///
/// KEYS[1] = notification:cap:{profile}:{subject}:{kind}:{yyyymmddhh}
/// ARGV[1] = max_per_hour
/// Returns: 1 if the notification is allowed, 0 if capped.
const HOURLY_CAP_SCRIPT: &str = r#"
local key = KEYS[1]
local max = tonumber(ARGV[1])
local v   = redis.call('INCR', key)
if v == 1 then
    redis.call('EXPIRE', key, 3600)
end
if v > max then
    return 0
end
return 1
"#;

// ── Payload shape (from wallet.v1.events, #665) ───────────────────────────────

/// The wallet's events, tagged on `type`; only a stake matters here.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ReactionKafkaEvent {
    StakeCommitted(StakeCommittedPayload),
    #[serde(other)]
    Other,
}

/// A batch of likes: who liked (`profile_id`), what (`target_kind`,
/// `target_id`), whose (`author_profile_id`), and whether it is the account's
/// first batch on it — the only one that notifies.
#[derive(Debug, Deserialize)]
struct StakeCommittedPayload {
    pub profile_id:        String,
    pub target_kind:       String,
    pub target_id:         String,
    pub author_profile_id: String,
    pub first:             bool,
    pub staked_at:         chrono::DateTime<chrono::Utc>,
}

// ── Key builders ──────────────────────────────────────────────────────────────

fn hot_key(subject_id: &SubjectId) -> String {
    format!("notification:hot:{}", subject_id)
}

fn cap_key(target_id: &ProfileId, subject_id: &SubjectId, kind: &str) -> String {
    let hour = chrono::Utc::now().format("%Y%m%d%H");
    format!("notification:cap:{}:{}:{}:{}", target_id, subject_id, kind, hour)
}

// ── Worker ────────────────────────────────────────────────────────────────────

pub struct ReactionNotificationWorker<R, B, U, S> {
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

impl<R, B, U, S> ReactionNotificationWorker<R, B, U, S>
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
                tracing::error!(error = %e, "failed to build DLQ producer — reaction notification consumer not started");
                return;
            }
        };

        // `Arc<Self>` so the per-message closure can capture an owned handle.
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => {
                    tracing::warn!("reaction notification consumer exited cleanly — restarting");
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "reaction notification consumer error — restarting after 5 s"
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

        tracing::info!(topic = TOPIC, group = %self.group_id, "reaction notification consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<ReactionKafkaEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { worker.process_one(event).await })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Processes one reaction event: accumulate it into a fresh single-event collapse
    /// batch and flush it. Removals, malformed ids, self-reactions and suppressions are
    /// no-ops that succeed; a transient write failure is retried then dead-lettered.
    ///
    /// The stream yields one message at a time, so cross-event collapse is handled by
    /// the Redis-backed window in `accumulate_in_window`, not this in-memory batch.
    async fn process_one(&self, event: &ReactionKafkaEvent) -> ProcessOutcome {
        let mut batch: HashMap<CollapseKey, CollapseBuffer> = HashMap::new();
        self.accumulate(event, &mut batch).await;

        match self.flush_batch(&mut batch).await {
            Ok(())  => ProcessOutcome::Done,
            Err(()) => ProcessOutcome::Retry("reaction notification write failed".to_string()),
        }
    }

    /// Decodes one reaction event and, when it should produce a notification,
    /// accumulates it into the collapse `batch`. Intentional skips (removals,
    /// malformed IDs, self-reactions, post-author cache misses) are no-ops — the
    /// caller still commits the offset for them.
    async fn accumulate(
        &self,
        event: &ReactionKafkaEvent,
        batch: &mut HashMap<CollapseKey, CollapseBuffer>,
    ) {
        // Only a liker's first batch on a post or comment notifies (#665);
        // later batches and other wallet events never do.
        let (post_id, sender_str, event_at_ms, subject_kind, author) = match event {
            ReactionKafkaEvent::StakeCommitted(e) if e.first => {
                let kind = match e.target_kind.as_str() {
                    "post" => SubjectKind::Post,
                    "comment" => SubjectKind::Comment,
                    _ => return,
                };
                (e.target_id.as_str(), e.profile_id.as_str(), e.staked_at.timestamp_millis(), kind, e.author_profile_id.as_str())
            }
            ReactionKafkaEvent::StakeCommitted(_) | ReactionKafkaEvent::Other => return,
        };

        let sender_uuid = match Uuid::parse_str(sender_str) {
            Ok(u) => u,
            Err(_) => {
                tracing::warn!(profile_id = sender_str, "invalid sender UUID — skipping");
                return;
            }
        };

        // The author comes with the stake (the wallet checked the target).
        let target_uuid = match Uuid::parse_str(author) {
            Ok(u) => u,
            Err(_) => {
                tracing::warn!(post_id, target = author, "invalid author UUID — skipping");
                return;
            }
        };

        // Self-notification guard.
        if sender_uuid == target_uuid {
            return;
        }

        let subject_uuid = match Uuid::parse_str(post_id) {
            Ok(u) => u,
            Err(_) => {
                tracing::warn!(post_id, "invalid post UUID — skipping");
                return;
            }
        };

        let key = CollapseKey::new(
            target_uuid,
            subject_uuid,
            subject_kind,
            NotificationKind::Reaction,
        );

        batch
            .entry(key)
            .and_modify(|buf| {
                buf.push(sender_uuid, event_at_ms, self.config.max_sample_senders)
            })
            .or_insert_with(|| {
                CollapseBuffer::new(
                    sender_uuid,
                    event_at_ms,
                    self.config.max_sample_senders,
                )
            });
    }

    /// Writes every accumulated collapse entry. Returns `Err(())` if any entry
    /// failed with an unexpected (transient) error, so the caller withholds the
    /// commit and lets the event redeliver. Suppression-class errors are terminal
    /// and treated as success.
    async fn flush_batch(
        &self,
        batch: &mut HashMap<CollapseKey, CollapseBuffer>,
    ) -> Result<(), ()> {
        let drained: Vec<(CollapseKey, CollapseBuffer)> = batch.drain().collect();

        let mut had_hard_error = false;
        for (key, buf) in drained {
            if let Err(err) = self.process_collapsed(&key, &buf).await {
                match err {
                    NotificationError::SenderBlocked { .. }
                    | NotificationError::SelfNotification { .. } => {
                        tracing::debug!(error = %err, "notification suppressed by gate");
                    }
                    other => {
                        tracing::error!(
                            error             = %other,
                            target_profile_id = %key.target_profile_id,
                            subject_id        = %key.subject_id,
                            "reaction notification write failed"
                        );
                        had_hard_error = true;
                    }
                }
            }
        }

        if had_hard_error { Err(()) } else { Ok(()) }
    }

    async fn process_collapsed(
        &self,
        key: &CollapseKey,
        buf: &CollapseBuffer,
    ) -> Result<(), NotificationError> {
        let target_id = ProfileId::from_uuid(key.target_profile_id);
        let sender_id = ProfileId::from_uuid(buf.primary_sender());
        let subject_id = SubjectId::from_uuid(key.subject_id);

        // Block gate.
        if self.block_cache.is_blocked(&sender_id, &target_id).await? {
            return Err(NotificationError::SenderBlocked {
                sender_id: sender_id.as_str(),
                target_id: target_id.as_str(),
            });
        }

        // Subject heat detection — activate cross-batch window for hot subjects.
        let hot_key_str  = hot_key(&subject_id);
        let heat: i64 = self.redis.inner
            .eval(
                INCR_HOT_SCRIPT,
                vec![hot_key_str],
                vec![300u64.to_string()],
            )
            .await
            .unwrap_or(0i64);

        let is_hot = heat >= self.config.hot_subject_threshold as i64;

        if is_hot && buf.total_count == 1 {
            // For hot subjects with a single sender in this batch, defer to the
            // cross-batch collapse window instead of writing immediately.
            return self.accumulate_in_window(key, buf, &sender_id).await;
        }

        // Hourly hard cap check.
        let cap_key_str = cap_key(&target_id, &subject_id, key.kind.as_str());
        let allowed: i64 = self.redis.inner
            .eval(
                HOURLY_CAP_SCRIPT,
                vec![cap_key_str],
                vec![self.config.max_notifications_per_subject_per_hour.to_string()],
            )
            .await
            .unwrap_or(1i64);

        if allowed == 0 {
            tracing::debug!(
                target_profile_id = %target_id,
                subject_id        = %subject_id,
                "notification capped by hourly rate limit"
            );
            return Ok(());
        }

        // Immediate (non-hot) path: one reaction → one notification. (subject, sender)
        // is the stable business key, so the id is deterministic (idempotent INSERT)
        // and the unread increment is claim-gated. created_at is the reaction's time.
        let business_key = format!("reaction:{}:{}", subject_id, sender_id);
        let ntf_id       = NotificationId::deterministic(&business_key);
        let created_at   = chrono::DateTime::from_timestamp_millis(buf.first_at_ms)
            .unwrap_or_default();

        let notification = Notification::create_collapsed(
            ntf_id,
            target_id,
            sender_id,
            buf.sample_sender_ids(),
            buf.sender_count(),
            key.kind,
            key.subject_kind,
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

        Ok(())
    }

    async fn accumulate_in_window(
        &self,
        key:       &CollapseKey,
        _buf:      &CollapseBuffer,
        sender_id: &ProfileId,
    ) -> Result<(), NotificationError> {
        let window_key      = key.redis_window_key();
        let senders_key     = key.redis_senders_key();
        let senders_set_key = key.redis_senders_set_key();
        let ttl             = self.config.collapse_window_secs;
        let max_sample      = self.config.max_sample_senders;
        let sched_member    = key.schedule_member();
        let expiry_score    = (chrono::Utc::now().timestamp_millis()
            + (ttl as i64 * 1000)) as f64;

        // Atomically bump the unique-sender count + sample list. All three keys share
        // a hash tag, so this multi-key script is slot-safe in Redis Cluster.
        let _: i64 = self.redis.inner
            .eval(
                COLLAPSE_ACCUMULATE_SCRIPT,
                vec![window_key, senders_key, senders_set_key],
                vec![
                    sender_id.as_str(),
                    ttl.to_string(),
                    max_sample.to_string(),
                ],
            )
            .await
            .map_err(|e| NotificationError::Redis(redis_storage::RedisStorageError::from(e)))?;

        // Register the window in the global flush schedule as a *separate* command —
        // the schedule key lives on a different slot than the window and cannot be
        // touched from inside the script above. `NX` keeps the original flush
        // deadline (a fixed window from the first event) and makes this idempotent
        // under at-least-once redelivery: a retry after a mid-accumulate crash still
        // schedules the window even though the counter is no longer 1.
        let _: i64 = self.redis.inner
            .zadd(
                SCHEDULE_KEY,
                Some(fred::types::SetOptions::NX),
                None,
                false,
                false,
                (expiry_score, sched_member),
            )
            .await
            .map_err(|e| NotificationError::Redis(redis_storage::RedisStorageError::from(e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wallet's JSON as it publishes it (#665).
    fn stake(first: bool, kind: &str) -> ReactionKafkaEvent {
        serde_json::from_str(&format!(
            r#"{{"type":"stake_committed","account_id":"a","profile_id":"p","target_kind":"{kind}","target_id":"t",
                "author_profile_id":"x","points":3,"total":3,"first":{first},"stake_key":"stake:k",
                "staked_at":"2026-10-08T12:00:00Z"}}"#
        ))
        .unwrap()
    }

    #[test]
    fn a_stake_decodes_with_its_author_and_first_flag_other_events_are_other() {
        let ReactionKafkaEvent::StakeCommitted(e) = stake(true, "comment") else { panic!() };
        assert_eq!((e.first, e.target_kind.as_str(), e.author_profile_id.as_str()), (true, "comment", "x"));
        assert!(matches!(stake(false, "post"), ReactionKafkaEvent::StakeCommitted(StakeCommittedPayload { first: false, .. })));
        let other: ReactionKafkaEvent = serde_json::from_str(r#"{"type":"something_new"}"#).unwrap();
        assert!(matches!(other, ReactionKafkaEvent::Other));
    }
}
