use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use fred::interfaces::{HashesInterface, KeysInterface};
use redis_storage::RedisClient;

use crate::application::port::CountryActivityStore;
use crate::domain::country_standing::CountryActivity;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// A day's counts are kept a little past the longest window read.
const DAY_TTL_SECS: i64 = 40 * 24 * 3600;

/// `sg:geo:cact:{YYYYMMDD}` — one hash per UTC day: `l:{CC}` likes, `p:{CC}`
/// posts.
fn day_key(day: NaiveDate) -> String {
    format!("sg:geo:cact:{}", day.format("%Y%m%d"))
}

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
    async fn add(&self, country: CountryCode, day: NaiveDate, likes: i64, posts: i64) -> Result<(), GeoDiscoveryError> {
        let key = day_key(day);
        if likes != 0 {
            let _: i64 = self.client.inner.hincrby(&key, format!("l:{country}"), likes).await.map_err(fred_err)?;
        }
        if posts != 0 {
            let _: i64 = self.client.inner.hincrby(&key, format!("p:{country}"), posts).await.map_err(fred_err)?;
        }
        let _: bool = self.client.inner.expire(&key, DAY_TTL_SECS, None).await.map_err(fred_err)?;
        Ok(())
    }

    async fn totals(&self, days: &[NaiveDate]) -> Result<HashMap<CountryCode, CountryActivity>, GeoDiscoveryError> {
        let reads = days.iter().map(|day| {
            let client = self.client.clone();
            async move { client.inner.hgetall::<HashMap<String, i64>, _>(day_key(*day)).await }
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
