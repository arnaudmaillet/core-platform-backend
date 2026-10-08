use std::sync::Arc;
use std::time::Duration;

use crate::application::port::CounterLedger;
use crate::domain::value_object::PostId;
use crate::infrastructure::scoring::redis_score_store::{
    shares_key, views_key, DirtyPostTracker, RedisScoreStore,
};

/// Periodic write-behind worker for high-volume view and share counters.
///
/// Every `flush_interval`, drains the `DirtyPostTracker` to discover which
/// posts have pending counter increments, snapshots the Redis delta atomically
/// via a `GETSET 0` Lua script, and applies the delta to the ScyllaDB counter
/// table. If ScyllaDB is unavailable, the counters remain in Redis and the
/// next flush cycle will retry (delta accumulates correctly).
///
/// # Approximate semantics
///
/// ScyllaDB counter tables are NOT idempotent. If the worker crashes after
/// GETSET (zeroing Redis) but before the ScyllaDB write completes, that
/// delta window is lost. This is acceptable for approximate analytics —
/// Redis is the authoritative real-time store.
pub struct CounterFlushWorker<L> {
    store:          Arc<RedisScoreStore>,
    ledger:         Arc<L>,
    tracker:        DirtyPostTracker,
    flush_interval: Duration,
}

impl<L: CounterLedger> CounterFlushWorker<L> {
    pub fn new(
        store:          Arc<RedisScoreStore>,
        ledger:         Arc<L>,
        tracker:        DirtyPostTracker,
        flush_interval: Duration,
    ) -> Self {
        Self { store, ledger, tracker, flush_interval }
    }

    /// Runs indefinitely. Call inside `tokio::spawn`.
    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.flush_cycle().await;
        }
    }

    async fn flush_cycle(&self) {
        let dirty = self.tracker.drain_all();
        if dirty.is_empty() {
            return;
        }

        tracing::debug!(count = dirty.len(), "counter flush cycle started");

        for uuid in dirty {
            let post_id = PostId::from_uuid(uuid);
            if let Err(err) = self.flush_post(&post_id).await {
                tracing::error!(
                    error   = %err,
                    post_id = %post_id,
                    "counter flush failed — delta remains in Redis for next cycle"
                );
                // Re-mark dirty so the next cycle picks it up.
                self.tracker.mark(&post_id);
            }
        }
    }

    async fn flush_post(&self, post_id: &PostId) -> Result<(), crate::error::EngagementError> {
        // Reuse the score store's key builders so the flush path can never drift
        // from the hot path (both must produce the same hash-tagged keys).
        let views_key  = views_key(post_id);
        let shares_key = shares_key(post_id);

        let (view_delta, share_delta) = tokio::try_join!(
            self.store.getset_zero(&views_key),
            self.store.getset_zero(&shares_key),
        )?;

        if view_delta == 0 && share_delta == 0 {
            return Ok(());
        }

        self.ledger
            .apply_interaction_delta(post_id, view_delta, share_delta, 0)
            .await?;

        tracing::debug!(
            post_id     = %post_id,
            view_delta,
            share_delta,
            "interaction counters flushed to ScyllaDB"
        );

        Ok(())
    }
}
