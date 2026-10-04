use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::value_object::LocationSharing;
use crate::error::GeoDiscoveryError;

/// The authors' location sharing, projected from `profile.v1.events`. Durable:
/// losing it would put ghosted authors back on the map.
#[async_trait]
pub trait LocationSettingsStore: Send + Sync + 'static {
    async fn set(&self, author_id: Uuid, sharing: LocationSharing) -> Result<(), GeoDiscoveryError>;

    /// The non-default sharing of each of `author_ids` (absent ⇒ default).
    async fn get_many(&self, author_ids: &[Uuid]) -> Result<HashMap<Uuid, LocationSharing>, GeoDiscoveryError>;
}

/// What the map does with an author's posts for a reader other than the
/// author: `None` drops them (ghost); `Some(city)` keeps them, coarsened when
/// `city`. The reader's own authors are always shown as they are.
pub async fn sharing_for_reader(
    store: &dyn LocationSettingsStore,
    owned: &[String],
    authors: impl IntoIterator<Item = Uuid>,
) -> Result<HashMap<Uuid, LocationSharing>, GeoDiscoveryError> {
    let mut others: Vec<Uuid> = authors
        .into_iter()
        .filter(|a| !owned.iter().any(|o| o == &a.to_string()))
        .collect();
    others.sort_unstable();
    others.dedup();
    if others.is_empty() {
        return Ok(HashMap::new());
    }
    store.get_many(&others).await
}
