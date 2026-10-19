use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// A member's countries (#665): the free home country, those unlocked, and
/// its 24 h visas (#910).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountCountries {
    pub home:     Option<CountryCode>,
    /// Bought with gems (the home country is not listed here).
    pub unlocked: Vec<CountryCode>,
    /// Being bought: the agreed price, while the gems are spent (not on the
    /// map yet).
    pub pending:  Vec<(CountryCode, i64)>,
    /// Visas and when each ends (#910); an expired one may still be listed
    /// until the store forgets it ([`Self::visas_at`] filters).
    pub visas:    Vec<(CountryCode, DateTime<Utc>)>,
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

    /// The visas running at `now`, of countries not unlocked otherwise.
    pub fn visas_at(&self, now: DateTime<Utc>) -> Vec<(CountryCode, DateTime<Utc>)> {
        self.visas.iter().copied().filter(|(c, until)| *until > now && !self.has(*c)).collect()
    }

    /// When the visa to `country` ends, if one runs at `now`.
    pub fn visa_until(&self, country: CountryCode, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.visas_at(now).into_iter().find(|(c, _)| *c == country).map(|(_, until)| until)
    }

    /// Every country the member's map shows at `now`: [`Self::all`], then the
    /// running visas. The server stops a visa at its end, whatever the
    /// client caches.
    pub fn on_map(&self, now: DateTime<Utc>) -> Vec<CountryCode> {
        let mut countries = self.all();
        countries.extend(self.visas_at(now).into_iter().map(|(c, _)| c));
        countries
    }
}

/// A visa bought under one idempotency key (#910): what was agreed before
/// the gems were spent, so a retry pays that price once and grants the
/// same end, never a second extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisaPurchase {
    pub country: CountryCode,
    pub price:   i64,
    /// The visa's end once granted.
    pub until:   DateTime<Utc>,
    /// Granted: the key's purchase is done.
    pub settled: bool,
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

    /// Records an unlock (idempotent; settles a pending one, ends a visa to
    /// that country).
    async fn add(&self, account: Uuid, country: CountryCode, price: i64, at: DateTime<Utc>) -> Result<(), GeoDiscoveryError>;

    /// The visa purchase made under `key` (#910), if any (kept 7 days).
    async fn visa_purchase(&self, account: Uuid, key: &str) -> Result<Option<VisaPurchase>, GeoDiscoveryError>;

    /// Records a visa purchase as agreed (`settled: false`) or done.
    async fn record_visa_purchase(&self, account: Uuid, key: &str, purchase: VisaPurchase) -> Result<(), GeoDiscoveryError>;

    /// Drops a visa purchase that was not paid.
    async fn clear_visa_purchase(&self, account: Uuid, key: &str) -> Result<(), GeoDiscoveryError>;

    /// Runs the visa to `country` until `until` (forgotten after it), unless
    /// a later end is already recorded.
    async fn grant_visa(&self, account: Uuid, country: CountryCode, until: DateTime<Utc>) -> Result<(), GeoDiscoveryError>;
}
