use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// A member's countries (#665): the free home country and those unlocked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountCountries {
    pub home:     Option<CountryCode>,
    /// Bought with gems (the home country is not listed here).
    pub unlocked: Vec<CountryCode>,
}

impl AccountCountries {
    /// Every country the member's map shows, home first.
    pub fn all(&self) -> Vec<CountryCode> {
        let mut all: Vec<CountryCode> = self.home.into_iter().collect();
        all.extend(self.unlocked.iter().filter(|c| Some(**c) != self.home));
        all
    }

    pub fn has(&self, country: CountryCode) -> bool {
        self.home == Some(country) || self.unlocked.contains(&country)
    }
}

/// Where a member's countries are kept, per account (one read on the map's
/// hot path).
#[async_trait]
pub trait CountryUnlockStore: Send + Sync + 'static {
    async fn get(&self, account: Uuid) -> Result<AccountCountries, GeoDiscoveryError>;

    /// Records the home country unless one is already: the first one stays
    /// for good. Returns the home country now.
    async fn set_home_once(&self, account: Uuid, country: CountryCode) -> Result<CountryCode, GeoDiscoveryError>;

    /// Records an unlock (idempotent).
    async fn add(&self, account: Uuid, country: CountryCode, price: i64, at: DateTime<Utc>) -> Result<(), GeoDiscoveryError>;
}
