//! Persistence for the [`Asset`] aggregate (its renditions ride with it — there is
//! no separate rendition repository). The concrete adapter is Postgres (the
//! metadata SoR), injected as `Arc<dyn …>` at the composition root.

use async_trait::async_trait;

use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, ContentHash, OwnerId, UploadKey};
use crate::error::MediaError;

#[async_trait]
pub trait AssetRepository: Send + Sync + 'static {
    /// Upserts the asset (optimistic-lock semantics in the Phase-4 adapter; a
    /// concurrent writer surfaces `ConcurrentModification`).
    async fn save(&self, asset: &Asset) -> Result<(), MediaError>;

    /// Saves a new asset under its owner's upload key (#876), unless a live
    /// (not deleted) asset of that owner already holds the key: then that
    /// asset's id, and nothing is saved. Atomic per key, so two tickets racing
    /// under one key reserve one asset.
    async fn insert_keyed(&self, asset: &Asset, key: &UploadKey) -> Result<Option<AssetId>, MediaError>;

    async fn find_by_id(&self, id: &AssetId) -> Result<Option<Asset>, MediaError>;

    /// Every asset (not deleted) holding these exact bytes — they share their
    /// content-addressed objects.
    async fn find_by_content_hash(&self, hash: &ContentHash) -> Result<Vec<Asset>, MediaError>;

    /// The account's assets (not deleted), by id, up to `limit` after `after`
    /// (#653: the GDPR export).
    async fn list_by_owner(
        &self,
        owner: &OwnerId,
        limit: i64,
        after: Option<&AssetId>,
    ) -> Result<Vec<Asset>, MediaError>;

    /// Private documents whose purge time has come (#777), oldest first, not
    /// deleted and not under a legal hold.
    async fn due_for_purge(&self, now: chrono::DateTime<chrono::Utc>, limit: i64) -> Result<Vec<Asset>, MediaError>;

    /// Dedup lookup: an existing **READY** asset with these exact bytes, if any.
    /// Used only when dedup is enabled (fork B); returns `None` otherwise-unmatched.
    async fn find_ready_by_content_hash(
        &self,
        hash: &ContentHash,
    ) -> Result<Option<Asset>, MediaError>;
}
