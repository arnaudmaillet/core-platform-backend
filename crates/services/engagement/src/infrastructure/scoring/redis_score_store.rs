use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashSet;
use fred::interfaces::{KeysInterface, LuaInterface};
use fred::types::Value as FredValue;
use redis_storage::RedisClient;
use uuid::Uuid;

use crate::application::port::{PostEngagementSnapshot, ScoreStore};
use crate::domain::value_object::PostId;
use crate::error::EngagementError;

// ── Lua scripts ───────────────────────────────────────────────────────────────

/// Atomically snapshots and resets a counter key to 0.
///
/// KEYS[1] = counter key (e.g. engagement:views:{post_id})
/// Returns: the previous value as a string, or "0" if the key did not exist.
const GETSET_ZERO_SCRIPT: &str = r#"
local key = KEYS[1]
local v   = redis.call('GET', key)
if v then
    redis.call('SET', key, '0')
    return v
else
    return '0'
end
"#;

// ── Key builders ──────────────────────────────────────────────────────────────
//
// Every per-post key embeds `{post_id}` as a Redis Cluster hash tag so that all
// keys for one post hash to the same slot. Different posts still distribute
// across slots, preserving sharding.

/// Per-post view counter. `pub(crate)` so `CounterFlushWorker` builds the exact
/// same key instead of re-formatting it (which would silently drift).
pub(crate) fn views_key(post_id: &PostId) -> String {
    format!("engagement:{{{post_id}}}:views")
}

/// Per-post share counter. See [`views_key`] for the visibility rationale.
pub(crate) fn shares_key(post_id: &PostId) -> String {
    format!("engagement:{{{post_id}}}:shares")
}

fn comments_key(post_id: &PostId) -> String {
    format!("engagement:{{{post_id}}}:comments")
}

// ── DirtyPostTracker ──────────────────────────────────────────────────────────

/// Thread-safe set of post UUIDs that have pending view/share counter increments.
///
/// Populated by `incr_view`/`incr_share` in the hot path.
/// Drained atomically by `CounterFlushWorker` every flush interval.
#[derive(Clone, Default)]
pub struct DirtyPostTracker {
    inner: Arc<DashSet<Uuid>>,
}

impl DirtyPostTracker {
    pub fn new() -> Self {
        Self { inner: Arc::new(DashSet::new()) }
    }

    pub fn mark(&self, post_id: &PostId) {
        self.inner.insert(post_id.as_uuid());
    }

    /// Drains all dirty post IDs and returns them. The set is cleared atomically.
    pub fn drain_all(&self) -> Vec<Uuid> {
        self.inner.iter().map(|r| *r.key()).collect::<Vec<_>>()
            .into_iter()
            .inspect(|id| { self.inner.remove(id); })
            .collect()
    }
}

// ── RedisScoreStore ───────────────────────────────────────────────────────────

pub struct RedisScoreStore {
    client:  RedisClient,
    tracker: DirtyPostTracker,
}

impl RedisScoreStore {
    pub fn new(client: RedisClient, tracker: DirtyPostTracker) -> Self {
        Self { client, tracker }
    }

    /// Atomically gets and resets a counter key. Returns the previous value.
    pub async fn getset_zero(&self, key: &str) -> Result<i64, EngagementError> {
        let result: String = self.client
            .inner
            .eval(GETSET_ZERO_SCRIPT, vec![key.to_owned()], Vec::<String>::new())
            .await
            .map_err(|e| EngagementError::Redis(redis_storage::RedisStorageError::from(e)))?;

        result.parse::<i64>().map_err(|_| EngagementError::ScriptReturnInvalid)
    }
}

fn fred_err(e: fred::error::Error) -> EngagementError {
    EngagementError::Redis(redis_storage::RedisStorageError::from(e))
}

#[async_trait]
impl ScoreStore for RedisScoreStore {
    async fn incr_view(&self, post_id: &PostId) -> Result<(), EngagementError> {
        let _: i64 = self.client.inner.incr(views_key(post_id)).await.map_err(fred_err)?;
        self.tracker.mark(post_id);
        Ok(())
    }

    async fn incr_share(&self, post_id: &PostId) -> Result<(), EngagementError> {
        let _: i64 = self.client.inner.incr(shares_key(post_id)).await.map_err(fred_err)?;
        self.tracker.mark(post_id);
        Ok(())
    }

    async fn incr_comment(&self, post_id: &PostId) -> Result<(), EngagementError> {
        let _: i64 = self.client.inner.incr(comments_key(post_id)).await.map_err(fred_err)?;
        Ok(())
    }

    async fn decr_comment(&self, post_id: &PostId) -> Result<(), EngagementError> {
        let _: i64 = self.client.inner.decr(comments_key(post_id)).await.map_err(fred_err)?;
        Ok(())
    }

    async fn get_snapshot(&self, post_id: &PostId) -> Result<PostEngagementSnapshot, EngagementError> {
        let (views_raw, shares_raw, comments_raw) = tokio::try_join!(
            async {
                self.client
                    .inner
                    .get::<Option<i64>, _>(views_key(post_id))
                    .await
                    .map_err(fred_err)
            },
            async {
                self.client
                    .inner
                    .get::<Option<i64>, _>(shares_key(post_id))
                    .await
                    .map_err(fred_err)
            },
            async {
                self.client
                    .inner
                    .get::<Option<i64>, _>(comments_key(post_id))
                    .await
                    .map_err(fred_err)
            },
        )?;

        Ok(PostEngagementSnapshot {
            view_count:    views_raw.unwrap_or(0),
            share_count:   shares_raw.unwrap_or(0),
            comment_count: comments_raw.unwrap_or(0),
        })
    }
}

// Unused but ensures FredValue is importable for future EVALSHA migration.
#[allow(dead_code)]
fn _phantom(_: FredValue) {}
