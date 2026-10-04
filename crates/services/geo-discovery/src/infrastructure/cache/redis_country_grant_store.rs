use async_trait::async_trait;
use fred::interfaces::KeysInterface;
use fred::types::Expiration;
use redis_storage::RedisClient;

use crate::application::port::CountryGrantStore;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// `sg:geo:cc:{principal}` — the country granted to a principal (token `sub`,
/// e.g. `guest:<uuid>`), a two-letter string with a TTL.
fn grant_key(principal: &str) -> String {
    format!("sg:geo:cc:{principal}")
}

fn fred_err(e: fred::error::Error) -> GeoDiscoveryError {
    GeoDiscoveryError::Redis(redis_storage::RedisStorageError::from(e))
}

pub struct RedisCountryGrantStore {
    client:   RedisClient,
    ttl_secs: u64,
}

impl RedisCountryGrantStore {
    pub fn new(client: RedisClient, ttl_secs: u64) -> Self {
        Self { client, ttl_secs: ttl_secs.max(1) }
    }
}

#[async_trait]
impl CountryGrantStore for RedisCountryGrantStore {
    async fn get(&self, principal: &str) -> Result<Option<CountryCode>, GeoDiscoveryError> {
        let value: Option<String> = self.client.inner.get(grant_key(principal)).await.map_err(fred_err)?;
        // An unreadable value grants nothing.
        Ok(value.and_then(|v| CountryCode::try_from(v.as_str()).ok()))
    }

    async fn set(&self, principal: &str, country: CountryCode) -> Result<(), GeoDiscoveryError> {
        self.client
            .inner
            .set::<(), _, _>(
                grant_key(principal),
                country.as_str(),
                Some(Expiration::EX(self.ttl_secs as i64)),
                None,
                false,
            )
            .await
            .map_err(fred_err)
    }

    async fn clear(&self, principal: &str) -> Result<(), GeoDiscoveryError> {
        let _: i64 = self.client.inner.del(grant_key(principal)).await.map_err(fred_err)?;
        Ok(())
    }
}
