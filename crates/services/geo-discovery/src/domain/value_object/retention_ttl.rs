use std::time::Duration;

/// Post retention duration.
///
/// Governs both the ScyllaDB `USING TTL` value and the Redis card key `EX`.
/// Default is 48 hours. Premium creators may receive extended retention
/// (e.g. 7 days) signalled via the Kafka `post.published` event.
///
/// The maximum TTL accepted is 30 days (2 592 000 s) — beyond that the
/// space-amplification tradeoff with TWCS breaks down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionTtl(Duration);

impl RetentionTtl {
    pub const DEFAULT_SECS: u64 = 172_800; // 48 h
    pub const MAX_SECS:     u64 = 2_592_000; // 30 days

    pub fn default_ttl() -> Self {
        Self(Duration::from_secs(Self::DEFAULT_SECS))
    }

    pub fn from_secs(secs: u64) -> Self {
        let clamped = secs.clamp(1, Self::MAX_SECS);
        Self(Duration::from_secs(clamped))
    }

    /// Seconds as i32 for ScyllaDB `USING TTL` bind parameter.
    pub fn as_scylla_ttl(&self) -> i32 {
        self.0.as_secs().min(i32::MAX as u64) as i32
    }

    /// Seconds as u64 for Redis `EX` argument.
    pub fn as_redis_ex(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }

    /// What is left of this retention for a post published at
    /// `published_at_ms`, at `now_ms`: the retention runs from the publication,
    /// not from when the event is processed, so a late or repeated
    /// `post.published` (a restore re-announces an old post) cannot put it back
    /// on the map for a fresh window. `None` once it has run out. A publication
    /// time ahead of `now` (clock skew) keeps the whole retention.
    pub fn remaining(&self, published_at_ms: i64, now_ms: i64) -> Option<Self> {
        let age_secs = (now_ms.saturating_sub(published_at_ms).max(0) / 1000) as u64;
        let left = self.0.as_secs().checked_sub(age_secs).filter(|s| *s > 0)?;
        Some(Self(Duration::from_secs(left)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR_MS: i64 = 3_600_000;

    #[test]
    fn the_retention_runs_from_the_publication() {
        let now = 1_800_000_000_000;
        let ttl = RetentionTtl::default_ttl();
        assert_eq!(ttl.remaining(now, now), Some(ttl), "just published: all of it");
        assert_eq!(ttl.remaining(now - 10 * HOUR_MS, now).map(|t| t.as_redis_ex()), Some(38 * 3600));
        assert_eq!(ttl.remaining(now - 48 * HOUR_MS, now), None, "run out");
        assert_eq!(ttl.remaining(now - 30 * 24 * HOUR_MS, now), None, "a restored month-old post stays off the map");
        assert_eq!(ttl.remaining(now + HOUR_MS, now), Some(ttl), "clock skew");
    }
}
