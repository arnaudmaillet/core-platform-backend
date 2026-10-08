use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use fred::interfaces::{HashesInterface, LuaInterface};
use redis_storage::RedisClient;

use crate::application::port::CountryActivityStore;
use crate::domain::country_standing::CountryActivity;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// A day's counts are kept a little past the longest window read.
const DAY_TTL_SECS: i64 = 40 * 24 * 3600;
/// How long a counted reaction is remembered: past any redelivery (a crash
/// or rebalance before the commit, a retry), not for the whole window — there
/// is one marker per reaction.
const LIKE_MARKER_TTL_SECS: i64 = 48 * 3600;
/// A counted post is remembered as long as its day, so a late re-announced
/// `post.published` counts nothing more either (posts are few).
const POST_MARKER_TTL_SECS: i64 = DAY_TTL_SECS;

/// A day's counts are spread over this many hashes (by event), so a day's
/// hearts never all land on one cluster slot; a read sums them.
const BUCKETS: u64 = 16;

/// `sg:geo:cact:{YYYYMMDD:b}` — one hash per UTC day and bucket: `l:{CC}`
/// likes, `p:{CC}` posts. The braces are the cluster hash tag: the event's
/// dedup marker shares its slot, so one script touches both.
fn day_key(day: NaiveDate, bucket: u64) -> String {
    format!("sg:geo:cact:{{{}:{bucket}}}", day.format("%Y%m%d"))
}

/// `sg:geo:cact:{YYYYMMDD:b}:seen:{event}` — the event was counted that day.
fn seen_key(day: NaiveDate, bucket: u64, event: &str) -> String {
    format!("{}:seen:{event}", day_key(day, bucket))
}

/// The bucket an event counts in (FNV-1a: stable across processes).
fn bucket_of(event: &str) -> u64 {
    event.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)) % BUCKETS
}

/// Counts once: KEYS[1] the day's hash, KEYS[2] the event's marker; ARGV
/// likes field, likes, posts field, posts, the day's ttl, the marker's ttl.
/// Returns 1 when counted, 0 when the event was already.
const ADD_ONCE_SCRIPT: &str = r#"
if redis.call('SET', KEYS[2], '1', 'NX', 'EX', ARGV[6]) == false then
  return 0
end
if tonumber(ARGV[2]) ~= 0 then redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]) end
if tonumber(ARGV[4]) ~= 0 then redis.call('HINCRBY', KEYS[1], ARGV[3], ARGV[4]) end
redis.call('EXPIRE', KEYS[1], ARGV[5])
return 1
"#;

fn fred_err(e: fred::error::Error) -> GeoDiscoveryError {
    GeoDiscoveryError::Redis(redis_storage::RedisStorageError::from(e))
}

pub struct RedisCountryActivity {
    client: RedisClient,
}

impl RedisCountryActivity {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CountryActivityStore for RedisCountryActivity {
    async fn add(
        &self,
        country: CountryCode,
        day: NaiveDate,
        likes: i64,
        posts: i64,
        event: &str,
    ) -> Result<(), GeoDiscoveryError> {
        let _: i64 = self
            .client
            .inner
            .eval(
                ADD_ONCE_SCRIPT,
                vec![day_key(day, bucket_of(event)), seen_key(day, bucket_of(event), event)],
                vec![
                    format!("l:{country}"),
                    likes.to_string(),
                    format!("p:{country}"),
                    posts.to_string(),
                    DAY_TTL_SECS.to_string(),
                    if posts != 0 { POST_MARKER_TTL_SECS } else { LIKE_MARKER_TTL_SECS }.to_string(),
                ],
            )
            .await
            .map_err(fred_err)?;
        Ok(())
    }

    async fn totals(&self, days: &[NaiveDate]) -> Result<HashMap<CountryCode, CountryActivity>, GeoDiscoveryError> {
        let reads = days.iter().flat_map(|day| (0..BUCKETS).map(move |bucket| (*day, bucket))).map(|(day, bucket)| {
            let client = self.client.clone();
            async move { client.inner.hgetall::<HashMap<String, i64>, _>(day_key(day, bucket)).await }
        });
        let mut totals: HashMap<CountryCode, CountryActivity> = HashMap::new();
        for fields in futures::future::try_join_all(reads).await.map_err(fred_err)? {
            for (field, count) in fields {
                // An unreadable field counts for nothing.
                let Some((kind, code)) = field.split_once(':') else { continue };
                let Ok(country) = CountryCode::try_from(code) else { continue };
                let entry = totals.entry(country).or_default();
                match kind {
                    "l" => entry.likes += count,
                    "p" => entry.posts += count,
                    _ => {}
                }
            }
        }
        Ok(totals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_and_its_marker_share_a_slot_and_buckets_spread() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 8).unwrap();
        let b = bucket_of("l:post:reactor:1:1");
        assert!(seen_key(day, b, "x").starts_with(&day_key(day, b)), "same hash tag");
        assert_eq!(day_key(day, 3), "sg:geo:cact:{20261008:3}");
        let buckets: std::collections::HashSet<_> = (0..200).map(|i| bucket_of(&format!("p:{i}"))).collect();
        assert!(buckets.len() > 8, "events spread over the buckets");
    }
}
