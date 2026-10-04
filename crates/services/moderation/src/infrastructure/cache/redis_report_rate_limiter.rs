use async_trait::async_trait;
use chrono::Utc;
use fred::interfaces::KeysInterface;
use redis_storage::RedisClient;

use crate::application::port::ReportRateLimiter;
use crate::error::ModerationError;

use super::keys::report_quota_key;

/// Fixed-window report quotas in Redis: one counter per reporter per hour and
/// per day (`INCR` + `EXPIRE` on first hit). Single-key commands, slot-local.
///
/// **Fails open**: if Redis does not answer, the report is admitted (and
/// logged). Reporting illegal content is a legal right (DSA Art. 16) and lands
/// in a review queue, so an outage must not silence it; the quota only exists
/// to stop floods.
#[derive(Clone)]
pub struct RedisReportRateLimiter {
    client: RedisClient,
    per_hour: i64,
    per_day: i64,
}

impl RedisReportRateLimiter {
    pub fn new(client: RedisClient, per_hour: u32, per_day: u32) -> Self {
        Self { client, per_hour: per_hour as i64, per_day: per_day as i64 }
    }

    async fn count(&self, key: String, ttl_secs: i64) -> Result<i64, fred::error::Error> {
        let n: i64 = self.client.incr(&key).await?;
        if n == 1 {
            let _: bool = self.client.expire(&key, ttl_secs, None).await?;
        }
        Ok(n)
    }
}

#[async_trait]
impl ReportRateLimiter for RedisReportRateLimiter {
    async fn admit(&self, reporter_key: &str) -> Result<bool, ModerationError> {
        let now = Utc::now().timestamp();
        let hour = report_quota_key(reporter_key, "h", now / 3_600);
        let day = report_quota_key(reporter_key, "d", now / 86_400);
        let counted = async {
            let in_hour = self.count(hour, 3_600).await?;
            let in_day = self.count(day, 86_400).await?;
            Ok::<_, fred::error::Error>(in_hour <= self.per_hour && in_day <= self.per_day)
        };
        match counted.await {
            Ok(admitted) => Ok(admitted),
            Err(error) => {
                tracing::warn!(%error, "report quota unavailable; admitting the report (fail-open)");
                Ok(true)
            }
        }
    }
}
