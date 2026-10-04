use std::sync::Arc;
use std::time::Duration;

use fred::interfaces::{LuaInterface, SortedSetsInterface};
use redis_storage::RedisClient;
use uuid::Uuid;

use crate::application::port::{NotificationRepository, StreamRegistry, UnreadCounter};
use crate::application::port::stream_registry::NotificationPayload;
use crate::config::NotificationConfig;
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{
    NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind,
};
use crate::error::NotificationError;
use crate::infrastructure::worker::collapse::{CollapseKey, SCHEDULE_KEY};

// ── Lua scripts ───────────────────────────────────────────────────────────────

/// Atomically reads and deletes a collapse window.
///
/// Both keys share a Redis Cluster hash tag (see `CollapseKey::redis_window_key`),
/// so this multi-key script is slot-safe.
///
/// KEYS[1] = notification:cw:{target:subject:kind}
/// KEYS[2] = notification:cw:{target:subject:kind}_:senders
/// KEYS[3] = notification:cw:{target:subject:kind}_:sset
/// Returns: [count_string, sender1, sender2, ...]
///          where count_string is "0" if the window is empty or already flushed.
const DRAIN_WINDOW_SCRIPT: &str = r#"
local window_key      = KEYS[1]
local senders_key     = KEYS[2]
local senders_set_key = KEYS[3]

local count = redis.call('GET', window_key)
if not count or count == '0' then
    return {'0'}
end

local senders = redis.call('LRANGE', senders_key, 0, -1)
redis.call('DEL', window_key)
redis.call('DEL', senders_key)
redis.call('DEL', senders_set_key)

local result = {count}
for _, s in ipairs(senders) do
    table.insert(result, s)
end
return result
"#;

// ── Worker ────────────────────────────────────────────────────────────────────

const SCAN_BATCH: usize = 100;

/// Periodic worker that settles cross-batch Redis collapse windows into ScyllaDB.
///
/// Every `flush_interval`, it queries the `notification:window_schedule` sorted
/// set for windows whose expiry score (Unix ms) has passed. For each settled
/// window it:
/// 1. Drains the Redis count + sender sample atomically via `DRAIN_WINDOW_SCRIPT`.
/// 2. Writes one collapsed `notifications_by_profile` row to ScyllaDB.
/// 3. Increments the unread counter.
/// 4. Broadcasts to any active gRPC stream subscriber.
/// 5. Removes the member from the schedule ZSET.
pub struct CollapseFlushWorker<R, U, S> {
    redis:          RedisClient,
    repository:     Arc<R>,
    counter:        Arc<U>,
    stream_reg:     Arc<S>,
    _config:        Arc<NotificationConfig>,
    flush_interval: Duration,
}

impl<R, U, S> CollapseFlushWorker<R, U, S>
where
    R: NotificationRepository,
    U: UnreadCounter,
    S: StreamRegistry,
{
    pub fn new(
        redis:          RedisClient,
        repository:     Arc<R>,
        counter:        Arc<U>,
        stream_reg:     Arc<S>,
        config:         Arc<NotificationConfig>,
        flush_interval: Duration,
    ) -> Self {
        Self { redis, repository, counter, stream_reg, _config: config, flush_interval }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.flush_cycle().await;
        }
    }

    async fn flush_cycle(&self) {
        let now_score = chrono::Utc::now().timestamp_millis() as f64;

        // Fetch all due schedule members WITH their scores (the window deadline,
        // Unix ms). The deadline is the window's stable epoch — it makes the flushed
        // notification id deterministic, so a retried flush overwrites the same row.
        let members: Vec<(String, f64)> = match self.redis.inner
            // f64 bounds: a numeric string would go to fred as a LEX bound
            // ("[1759…"), which Redis rejects for ZRANGEBYSCORE — every flush
            // cycle failed that way and collapsed notifications never left.
            .zrangebyscore(
                SCHEDULE_KEY,
                f64::NEG_INFINITY,
                now_score,
                true,
                Some((0i64, SCAN_BATCH as i64)),
            )
            .await
        {
            Ok(m) => m,
            Err(err) => {
                tracing::error!(error = %err, "CollapseFlushWorker: ZRANGEBYSCORE failed");
                return;
            }
        };

        if members.is_empty() {
            return;
        }

        tracing::debug!(count = members.len(), "collapse flush cycle: windows ready");

        for (member, score) in &members {
            let deadline_ms = *score as i64;
            if let Err(err) = self.flush_window(member, deadline_ms).await {
                tracing::error!(
                    error  = %err,
                    window = member,
                    "collapse window flush failed"
                );
            }

            // Remove from schedule regardless of flush outcome — a failed flush
            // leaves no window data in Redis (DRAIN_WINDOW deletes it), so the
            // notification is silently dropped rather than double-written.
            let _: Result<i64, _> = self.redis.inner.zrem(SCHEDULE_KEY, member.as_str()).await;
        }
    }

    async fn flush_window(&self, member: &str, deadline_ms: i64) -> Result<(), NotificationError> {
        // `member` format: `{target_uuid}:{subject_uuid}:{kind_str}`
        let parts: Vec<&str> = member.splitn(3, ':').collect();
        if parts.len() != 3 {
            tracing::warn!(member, "invalid schedule member format — skipping");
            return Ok(());
        }

        let target_uuid  = Uuid::parse_str(parts[0])
            .map_err(|_| NotificationError::InvalidProfileId(parts[0].to_owned()))?;
        let subject_uuid = Uuid::parse_str(parts[1])
            .map_err(|_| NotificationError::InvalidSubjectId(parts[1].to_owned()))?;
        let kind = match parts[2] {
            "reaction" => NotificationKind::Reaction,
            "comment"  => NotificationKind::Comment,
            "reply"    => NotificationKind::Reply,
            "mention"  => NotificationKind::Mention,
            other => {
                tracing::warn!(kind = other, "unknown kind in schedule member — skipping");
                return Ok(());
            }
        };

        // Rebuild the keys through CollapseKey so they always match what the
        // accumulator wrote — including the cluster hash tag.
        let collapse_key    = CollapseKey::new(target_uuid, subject_uuid, SubjectKind::Post, kind);
        let window_key      = collapse_key.redis_window_key();
        let senders_key     = collapse_key.redis_senders_key();
        let senders_set_key = collapse_key.redis_senders_set_key();

        let result: Vec<String> = self.redis.inner
            .eval(
                DRAIN_WINDOW_SCRIPT,
                vec![window_key.clone(), senders_key, senders_set_key],
                Vec::<String>::new(),
            )
            .await
            .map_err(|e| NotificationError::CollapseFlushFailed {
                window_key: window_key.clone(),
                message:    e.to_string(),
            })?;

        if result.is_empty() || result[0] == "0" {
            return Ok(());
        }

        let count: i32 = result[0].parse().unwrap_or(1);
        let sender_uuids: Vec<Uuid> = result[1..]
            .iter()
            .filter_map(|s| Uuid::parse_str(s).ok())
            .collect();

        if sender_uuids.is_empty() {
            return Ok(());
        }

        let target_id      = ProfileId::from_uuid(target_uuid);
        let subject_id     = SubjectId::from_uuid(subject_uuid);
        let primary_sender = ProfileId::from_uuid(sender_uuids[0]);

        // The (window member, deadline) pair identifies this specific window
        // instance: the id is deterministic so a retried flush overwrites the same
        // row, and created_at is the window deadline. A later window for the same
        // target/subject/kind gets a new deadline and therefore a distinct row.
        let business_key = format!("reaction-window:{}:{}", member, deadline_ms);
        let ntf_id       = NotificationId::deterministic(&business_key);
        let created_at   = chrono::DateTime::from_timestamp_millis(deadline_ms)
            .unwrap_or_default();

        let notification = Notification::create_collapsed(
            ntf_id,
            target_id,
            primary_sender,
            sender_uuids.clone(),
            count,
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
            target_profile_id = %target_id,
            subject_id        = %subject_id,
            sender_count      = count,
            kind              = kind.as_str(),
            "collapse window flushed to ScyllaDB"
        );

        Ok(())
    }
}
