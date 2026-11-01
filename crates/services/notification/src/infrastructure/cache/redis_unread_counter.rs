use std::sync::Arc;

use async_trait::async_trait;
use fred::interfaces::{KeysInterface, LuaInterface};
use redis_storage::RedisClient;

use crate::application::port::{NotificationRepository, UnreadCounter};
use crate::config::NotificationConfig;
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

// ── Lua scripts ───────────────────────────────────────────────────────────────

/// Atomically increments the unread counter, capped at `ARGV[1]`.
///
/// KEYS[1] = notification:unread:{profile_id}
/// ARGV[1] = cap (e.g. 99)
/// Returns: the new counter value (capped).
const INCR_CAPPED_SCRIPT: &str = r#"
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local v   = redis.call('INCR', key)
if v > cap then
    redis.call('SET', key, tostring(cap))
    return cap
end
return v
"#;

/// Idempotent capped increment: claims `dedupe_key` and only increments when the
/// claim is newly set. Both keys carry the `{profile_id}` hash tag (see
/// `unread_key` / `claim_key`) so they share a Redis Cluster slot and this
/// multi-key script is slot-safe.
///
/// KEYS[1] = notification:unread:{profile_id}             (counter)
/// KEYS[2] = notification:dedupe:{profile_id}:business    (one-shot claim)
/// ARGV[1] = cap (e.g. 99)
/// ARGV[2] = dedupe_ttl_secs
/// Returns: 1 if the counter was incremented, 0 if the event was a duplicate.
const INCR_CAPPED_CLAIMED_SCRIPT: &str = r#"
local unread_key = KEYS[1]
local claim_key  = KEYS[2]
local cap        = tonumber(ARGV[1])
local ttl        = tonumber(ARGV[2])

if redis.call('SET', claim_key, '1', 'NX', 'EX', ttl) == false then
    return 0
end

local v = redis.call('INCR', unread_key)
if v > cap then
    redis.call('SET', unread_key, tostring(cap))
end
return 1
"#;

/// Atomically decrements the unread counter, floored at 0.
///
/// KEYS[1] = notification:unread:{profile_id}
/// Returns: the new counter value (floored at 0).
const DECR_FLOORED_SCRIPT: &str = r#"
local key = KEYS[1]
local v   = tonumber(redis.call('GET', key) or '0')
if v <= 0 then
    redis.call('SET', key, '0')
    return 0
end
local nv = redis.call('DECR', key)
if nv < 0 then
    redis.call('SET', key, '0')
    return 0
end
return nv
"#;

// ── Key builders ──────────────────────────────────────────────────────────────

/// Per-profile unread counter. The `{profile_id}` Redis Cluster hash tag pins it
/// to the same slot as the profile's `claim_key`, so the claim-gated increment
/// script can touch both atomically without CROSSSLOT.
fn unread_key(profile_id: &ProfileId) -> String {
    format!("notification:unread:{{{profile_id}}}")
}

/// One-shot idempotency claim for a `(profile, business_key)` pair. Shares the
/// `{profile_id}` hash tag with `unread_key`.
fn claim_key(profile_id: &ProfileId, business_key: &str) -> String {
    format!("notification:dedupe:{{{profile_id}}}:{business_key}")
}

fn horizon_key(profile_id: &ProfileId) -> String {
    format!("notification:read_horizon:{}", profile_id)
}

// ── Implementation ────────────────────────────────────────────────────────────

pub struct RedisUnreadCounter<R> {
    client:     RedisClient,
    repository: Arc<R>,
    config:     Arc<NotificationConfig>,
}

impl<R: NotificationRepository> RedisUnreadCounter<R> {
    pub fn new(client: RedisClient, repository: Arc<R>, config: Arc<NotificationConfig>) -> Self {
        Self { client, repository, config }
    }

    fn redis_err(e: fred::error::Error) -> NotificationError {
        NotificationError::Redis(redis_storage::RedisStorageError::from(e))
    }
}

#[async_trait]
impl<R: NotificationRepository> UnreadCounter for RedisUnreadCounter<R> {
    async fn increment(&self, profile_id: &ProfileId) -> Result<(), NotificationError> {
        let key = unread_key(profile_id);
        let cap = self.config.unread_cap;

        let _: i64 = self.client
            .inner
            .eval(
                INCR_CAPPED_SCRIPT,
                vec![key],
                vec![cap.to_string()],
            )
            .await
            .map_err(Self::redis_err)?;

        self.repository.increment_counter(profile_id).await?;
        Ok(())
    }

    async fn increment_once(
        &self,
        profile_id: &ProfileId,
        dedupe_key: &str,
    ) -> Result<bool, NotificationError> {
        // Atomic claim + Redis increment (both keys share the {profile_id} slot).
        let incremented: i64 = self.client
            .inner
            .eval(
                INCR_CAPPED_CLAIMED_SCRIPT,
                vec![unread_key(profile_id), claim_key(profile_id, dedupe_key)],
                vec![self.config.unread_cap.to_string(), self.config.dedupe_ttl_secs.to_string()],
            )
            .await
            .map_err(Self::redis_err)?;

        if incremented == 0 {
            // Duplicate event — the claim already existed. Nothing to do.
            return Ok(false);
        }

        // Mirror the increment into the durable ScyllaDB counter. A crash between
        // the Redis increment and this write leaves Redis (the L1 source of truth)
        // ahead by one — a bounded, accepted divergence.
        self.repository.increment_counter(profile_id).await?;
        Ok(true)
    }

    async fn decrement(&self, profile_id: &ProfileId) -> Result<(), NotificationError> {
        let key = unread_key(profile_id);

        let _: i64 = self.client
            .inner
            .eval(
                DECR_FLOORED_SCRIPT,
                vec![key],
                Vec::<String>::new(),
            )
            .await
            .map_err(Self::redis_err)?;

        self.repository.decrement_counter(profile_id).await?;
        Ok(())
    }

    async fn set(&self, profile_id: &ProfileId, count: i64) -> Result<(), NotificationError> {
        let key = unread_key(profile_id);
        let count = count.clamp(0, self.config.unread_cap);
        let _: () = self.client.inner.set(&key, count.to_string(), None, None, false).await.map_err(Self::redis_err)?;
        Ok(())
    }

    async fn reset(&self, profile_id: &ProfileId) -> Result<(), NotificationError> {
        let key = unread_key(profile_id);
        let _: () = self.client.inner
            .set(
                &key,
                "0",
                None,
                None,
                false,
            )
            .await
            .map_err(Self::redis_err)?;
        Ok(())
    }

    async fn get(&self, profile_id: &ProfileId) -> Result<i64, NotificationError> {
        let key = unread_key(profile_id);
        let raw: Option<String> = self.client.inner
            .get(&key)
            .await
            .map_err(Self::redis_err)?;

        if let Some(v) = raw {
            return Ok(v.parse::<i64>().unwrap_or(0));
        }

        // Cache miss: read from ScyllaDB counter and repopulate.
        let count = self.repository.read_counter(profile_id).await?;
        let capped = count.min(self.config.unread_cap);

        let _: () = self.client.inner
            .set(&key, capped.to_string(), None, None, false)
            .await
            .map_err(Self::redis_err)?;

        Ok(capped)
    }

    async fn set_read_horizon(
        &self,
        profile_id: &ProfileId,
        horizon_ms: i64,
    ) -> Result<(), NotificationError> {
        let key = horizon_key(profile_id);
        let _: () = self.client.inner
            .set(&key, horizon_ms.to_string(), None, None, false)
            .await
            .map_err(Self::redis_err)?;
        Ok(())
    }

    async fn get_read_horizon(&self, profile_id: &ProfileId) -> Result<i64, NotificationError> {
        let key = horizon_key(profile_id);
        let raw: Option<String> = self.client.inner
            .get(&key)
            .await
            .map_err(Self::redis_err)?;

        Ok(raw.and_then(|v| v.parse::<i64>().ok()).unwrap_or(0))
    }
}
