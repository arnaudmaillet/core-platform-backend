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
    /// event counts nothing more. Atomic with its dedup marker. Whether it
    /// counted (`false`: already counted).
    async fn add(
        &self,
        country: CountryCode,
        day: NaiveDate,
        likes: i64,
        posts: i64,
        event: &str,
    ) -> Result<bool, GeoDiscoveryError>;

    /// The counts summed over `days`.
    async fn totals(&self, days: &[NaiveDate]) -> Result<HashMap<CountryCode, CountryActivity>, GeoDiscoveryError>;

    /// Adds `likes` to `post`'s likes in `country` on `day` (#942: the Shop
    /// cover). Not deduplicated: called once [`Self::add`] counted the like.
    /// A day keeps its most liked posts only.
    async fn add_post_likes(
        &self,
        country: CountryCode,
        day: NaiveDate,
        post: uuid::Uuid,
        likes: i64,
    ) -> Result<(), GeoDiscoveryError>;

    /// `country`'s most liked posts over `days`, most first, at most `limit`.
    async fn top_posts(
        &self,
        country: CountryCode,
        days: &[NaiveDate],
        limit: usize,
    ) -> Result<Vec<(uuid::Uuid, i64)>, GeoDiscoveryError>;
}
