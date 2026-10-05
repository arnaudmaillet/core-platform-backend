//! Persistence for the [`Asset`] aggregate (its renditions ride with it — there is
//! no separate rendition repository). The concrete adapter is Postgres (the
//! metadata SoR), injected as `Arc<dyn …>` at the composition root.

use async_trait::async_trait;

use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, ContentHash};
use crate::error::MediaError;

#[async_trait]
pub trait AssetRepository: Send + Sync + 'static {
    /// Upserts the asset (optimistic-lock semantics in the Phase-4 adapter; a
    /// concurrent writer surfaces `ConcurrentModification`).
    async fn save(&self, asset: &Asset) -> Result<(), MediaError>;

    async fn find_by_id(&self, id: &AssetId) -> Result<Option<Asset>, MediaError>;

    /// Every asset (not deleted) holding these exact bytes — they share their
    /// content-addressed objects.
    async fn find_by_content_hash(&self, hash: &ContentHash) -> Result<Vec<Asset>, MediaError>;

    /// Dedup lookup: an existing **READY** asset with these exact bytes, if any.
    /// Used only when dedup is enabled (fork B); returns `None` otherwise-unmatched.
    async fn find_ready_by_content_hash(
        &self,
        hash: &ContentHash,
    ) -> Result<Option<Asset>, MediaError>;
}
