//! Retention of guest data. A guest principal records an installation's device
//! id, locale and country, and a guest session its device and IP: personal data
//! kept only as long as it serves. After `retention` the guests that never
//! became an account (and have no live session) are deleted, with the guest
//! sessions that ended by then. Guests that became an account are kept: the
//! welcome gift is once per device (B4).

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};

use crate::application::port::GuestRegistry;
use crate::error::AuthError;

/// Rows deleted per shard per statement; a run loops until a batch comes back short.
const BATCH: i64 = 1_000;

#[derive(Clone)]
pub struct GuestRetention {
    guests:    Arc<dyn GuestRegistry>,
    retention: Duration,
}

impl GuestRetention {
    pub fn new(guests: Arc<dyn GuestRegistry>, retention: Duration) -> Self {
        Self { guests, retention }
    }

    /// One pass: purges everything past retention at `now`; returns the guests
    /// deleted.
    pub async fn run_once(&self, now: DateTime<Utc>) -> Result<u64, AuthError> {
        let cutoff = now - self.retention;
        let mut total = 0;
        loop {
            let purged = self.guests.purge_stale(cutoff, now, BATCH).await?;
            total += purged;
            if purged < BATCH as u64 {
                return Ok(total);
            }
        }
    }

    /// Runs a pass now and every `every`, logging failures (the next pass retries).
    pub async fn run(self, every: std::time::Duration) {
        let mut ticks = tokio::time::interval(every);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            match self.run_once(Utc::now()).await {
                Ok(0) => {}
                Ok(purged) => tracing::info!(purged, "stale guest records deleted"),
                Err(e) => tracing::warn!(error = %e, "guest retention pass failed; retried next pass"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::InMemoryGuestRegistry;
    use crate::application::port::GuestRecord;
    use crate::domain::value_object::AccountId;

    fn guest(days_ago: i64, now: DateTime<Utc>) -> GuestRecord {
        GuestRecord {
            guest_id: AccountId::from_uuid(uuid::Uuid::now_v7()),
            device_id: "d".into(),
            attestation_sent: false,
            attest_key_id: None,
            locale: None,
            region_hint: None,
            current_country: None,
            first_seen_at: now - Duration::days(days_ago),
        }
    }

    #[tokio::test]
    async fn guests_past_retention_go_recent_and_upgraded_ones_stay() {
        let now = Utc::now();
        let registry = Arc::new(InMemoryGuestRegistry::default());
        let (old, recent, upgraded) = (guest(100, now), guest(10, now), guest(200, now));
        for g in [&old, &recent, &upgraded] {
            registry.record(g).await.unwrap();
        }
        registry.mark_upgraded(&upgraded.guest_id, &AccountId::from_uuid(uuid::Uuid::now_v7()), now).await.unwrap();

        let retention = GuestRetention::new(Arc::clone(&registry) as _, Duration::days(90));
        assert_eq!(retention.run_once(now).await.unwrap(), 1);
        let left: Vec<_> = registry.records.lock().unwrap().iter().map(|r| r.guest_id).collect();
        assert_eq!(left, vec![recent.guest_id, upgraded.guest_id]);
    }
}
