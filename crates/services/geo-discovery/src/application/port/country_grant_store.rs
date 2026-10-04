use async_trait::async_trait;

use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// The country each principal (token `sub`) was last granted from its
/// location. One country at a time: a new grant replaces the previous one (the
/// country left behind locks again), and a grant expires on its own.
#[async_trait]
pub trait CountryGrantStore: Send + Sync + 'static {
    async fn get(&self, principal: &str) -> Result<Option<CountryCode>, GeoDiscoveryError>;
    async fn set(&self, principal: &str, country: CountryCode) -> Result<(), GeoDiscoveryError>;
    async fn clear(&self, principal: &str) -> Result<(), GeoDiscoveryError>;
}
