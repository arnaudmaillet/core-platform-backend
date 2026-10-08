use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDate;

use crate::domain::country_standing::CountryActivity;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// Each country's likes and posts per UTC day (#665: the country ladder,
/// which prices the unlocks).
#[async_trait]
pub trait CountryActivityStore: Send + Sync + 'static {
    /// Adds to `country`'s counts on `day`, **once per `event`** (a key naming
    /// what is counted: a reaction, a post): a redelivered or re-announced
    /// event counts nothing more. Atomic with its dedup marker.
    async fn add(
        &self,
        country: CountryCode,
        day: NaiveDate,
        likes: i64,
        posts: i64,
        event: &str,
    ) -> Result<(), GeoDiscoveryError>;

    /// The counts summed over `days`.
    async fn totals(&self, days: &[NaiveDate]) -> Result<HashMap<CountryCode, CountryActivity>, GeoDiscoveryError>;
}
