//! GDPR erasure (Art. 17, #777): when `account` finally deletes an account
//! (`account_deleted`), every asset it owns is deleted like an owner's delete
//! — bytes, renditions, quarantined copies, staging object, then the
//! tombstone, `AssetDeleted`, the cache and the CDN — except those under a
//! **legal hold** (CSAM evidence): they stay, untouched, and are logged.
//! Idempotent: a replay finds nothing left but the held ones.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use uuid::Uuid;

use crate::application::command::{DeleteAssetCommand, DeleteAssetHandler};
use crate::application::port::AssetRepository;
use crate::domain::value_object::{AssetId, OwnerId};
use crate::error::MediaError;

/// Assets read per page.
const PAGE: i64 = 100;

/// What an erasure did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ErasedMedia {
    pub deleted: u64,
    /// Kept under a legal hold.
    pub held:    u64,
}

pub struct OwnerErasure {
    assets: Arc<dyn AssetRepository>,
    delete: Arc<DeleteAssetHandler>,
}

impl OwnerErasure {
    pub fn new(assets: Arc<dyn AssetRepository>, delete: Arc<DeleteAssetHandler>) -> Self {
        Self { assets, delete }
    }

    /// Errors (storage, object store, CDN) are retryable: what was deleted
    /// stays deleted, the retry finishes the rest.
    pub async fn erase(&self, owner: &OwnerId, now: DateTime<Utc>) -> Result<ErasedMedia, MediaError> {
        let mut erased = ErasedMedia::default();
        let mut after: Option<AssetId> = None;
        loop {
            let page = self.assets.list_by_owner(owner, PAGE, after.as_ref()).await?;
            let full = page.len() as i64 == PAGE;
            after = page.last().map(|a| a.id());
            for asset in page {
                let command = DeleteAssetCommand { asset_id: asset.id(), owner_id: *owner };
                match self.delete.handle(Envelope::new(Uuid::now_v7(), command), now).await {
                    Ok(_) => erased.deleted += 1,
                    Err(MediaError::LegalHoldActive) => {
                        tracing::warn!(asset.id = %asset.id().as_str(), "asset under legal hold kept through account erasure");
                        erased.held += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
            if !full {
                break;
            }
        }
        tracing::info!(deleted = erased.deleted, held = erased.held, "account media erased");
        Ok(erased)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{owner, t0, Fixture};
    use crate::domain::value_object::{AssetState, MediaKind};

    #[tokio::test]
    async fn every_asset_of_the_account_goes_but_a_held_one() {
        let fx = Fixture::new();
        let (a, _) = fx.ready_asset(MediaKind::PostImage).await;
        let (b, _) = fx.ready_asset(MediaKind::Avatar).await;
        let (held, _) = fx.ready_asset(MediaKind::PostImage).await;
        {
            let mut asset = fx.assets.find_by_id(&held).await.unwrap().unwrap();
            asset.place_legal_hold(t0());
            fx.assets.save(&asset).await.unwrap();
        }
        let erasure = OwnerErasure::new(Arc::clone(&fx.assets) as _, Arc::new(fx.delete_handler()));

        // Someone else's erasure touches nothing of this account.
        let stranger = OwnerId::from_uuid(Uuid::from_u128(999));
        assert_eq!(erasure.erase(&stranger, t0()).await.unwrap(), ErasedMedia::default());

        assert_eq!(erasure.erase(&owner(), t0()).await.unwrap(), ErasedMedia { deleted: 2, held: 1 });
        for id in [a, b] {
            assert_eq!(fx.assets.find_by_id(&id).await.unwrap().unwrap().state(), AssetState::Deleted);
        }
        assert_eq!(fx.assets.find_by_id(&held).await.unwrap().unwrap().state(), AssetState::Ready);

        // A replay: only the held one is left.
        assert_eq!(erasure.erase(&owner(), t0()).await.unwrap(), ErasedMedia { deleted: 0, held: 1 });
    }
}
