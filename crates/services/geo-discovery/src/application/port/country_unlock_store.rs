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
    /// Being bought: the agreed price, while the gems are spent (not on the
    /// map yet).
    pub pending:  Vec<(CountryCode, i64)>,
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

    /// Records the price agreed for `country` before its gems are spent
    /// (kept 24 h): a retry after a failure pays that price, once.
    async fn mark_pending(&self, account: Uuid, country: CountryCode, price: i64) -> Result<(), GeoDiscoveryError>;

    /// Drops a pending purchase that was not paid.
    async fn clear_pending(&self, account: Uuid, country: CountryCode) -> Result<(), GeoDiscoveryError>;

    /// Records an unlock (idempotent; settles a pending one).
    async fn add(&self, account: Uuid, country: CountryCode, price: i64, at: DateTime<Utc>) -> Result<(), GeoDiscoveryError>;
}
