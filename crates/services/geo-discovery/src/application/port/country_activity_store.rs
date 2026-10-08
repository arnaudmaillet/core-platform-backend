use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;

use crate::domain::country_standing::CountryActivity;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// Each country's likes and posts per UTC day (#665: the country ladder).
/// Counts are a ranking signal: a redelivered event may count twice.
#[async_trait]
pub trait CountryActivityStore: Send + Sync + 'static {
    /// Adds to `country`'s counts on `day`.
    async fn add(&self, country: CountryCode, day: NaiveDate, likes: i64, posts: i64) -> Result<(), GeoDiscoveryError>;

    /// The counts summed over `days`.
    async fn totals(&self, days: &[NaiveDate]) -> Result<HashMap<CountryCode, CountryActivity>, GeoDiscoveryError>;
}
