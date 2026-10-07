//! Private-document retention (#777). A verification request's decision moves
//! its documents' purge to the decision + the retention (30 days by default,
//! `MEDIA_DOCUMENT_RETENTION_DAYS`); a sweeper deletes what is due, like an
//! owner's delete. A document never decided goes at its READY backstop
//! ([`PRIVATE_DOCUMENT_BACKSTOP`](crate::domain::aggregate::PRIVATE_DOCUMENT_BACKSTOP)).
//! A legal hold keeps it.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use cqrs::Envelope;
use uuid::Uuid;

use crate::application::command::{DeleteAssetCommand, DeleteAssetHandler};
use crate::application::port::AssetRepository;
use crate::domain::value_object::{AssetId, OwnerId};
use crate::error::MediaError;

/// Documents purged per sweep.
const SWEEP: i64 = 100;

pub struct DocumentRetention {
    assets:    Arc<dyn AssetRepository>,
    delete:    Arc<DeleteAssetHandler>,
    retention: Duration,
}

impl DocumentRetention {
    pub fn new(assets: Arc<dyn AssetRepository>, delete: Arc<DeleteAssetHandler>, retention: Duration) -> Self {
        Self { assets, delete, retention }
    }

    /// A request by `owner` was decided at `decided_at`: its documents (only
    /// the owner's private ones; anything else is ignored) are purged at the
    /// decision + the retention. Idempotent.
    pub async fn on_decision(
        &self,
        documents: &[AssetId],
        owner: &OwnerId,
        decided_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<u64, MediaError> {
        let mut scheduled = 0;
        for id in documents {
            let Some(mut asset) = self.assets.find_by_id(id).await? else { continue };
            if !asset.kind().is_private() || asset.owner_id() != *owner {
                tracing::warn!(asset.id = %id.as_str(), "a decided request names a document that is not the requester's");
                continue;
            }
            asset.schedule_purge(decided_at + self.retention, now)?;
            self.assets.save(&asset).await?;
            scheduled += 1;
        }
        Ok(scheduled)
    }

    /// Deletes the documents due at `now` (a page); returns how many went.
    pub async fn sweep(&self, now: DateTime<Utc>) -> Result<u64, MediaError> {
        let mut purged = 0;
        for asset in self.assets.due_for_purge(now, SWEEP).await? {
            let command = DeleteAssetCommand { asset_id: asset.id(), owner_id: asset.owner_id() };
            match self.delete.handle(Envelope::new(Uuid::now_v7(), command), now).await {
                Ok(_) => purged += 1,
                Err(MediaError::LegalHoldActive) => {}
                Err(error) => return Err(error),
            }
        }
        if purged > 0 {
            tracing::info!(purged, "private documents purged");
        }
        Ok(purged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{owner, t0, Fixture};
    use crate::domain::aggregate::PRIVATE_DOCUMENT_BACKSTOP;
    use crate::domain::value_object::{AssetState, MediaKind};

    fn retention(fx: &Fixture) -> DocumentRetention {
        DocumentRetention::new(Arc::clone(&fx.assets) as _, Arc::new(fx.delete_handler()), Duration::days(30))
    }

    #[tokio::test]
    async fn a_decided_document_goes_thirty_days_after_the_decision() {
        let fx = Fixture::new();
        let (doc, _) = fx.ready_asset(MediaKind::PrivateDocument).await;
        let (image, _) = fx.ready_asset(MediaKind::PostImage).await;
        let r = retention(&fx);
        let backstop = fx.assets.find_by_id(&doc).await.unwrap().unwrap().purge_after().unwrap();
        assert_eq!(backstop, t0() + PRIVATE_DOCUMENT_BACKSTOP);

        // Another account's decision, or a non-document, changes nothing.
        let stranger = OwnerId::from_uuid(Uuid::from_u128(999));
        assert_eq!(r.on_decision(&[doc], &stranger, t0(), t0()).await.unwrap(), 0);
        assert_eq!(r.on_decision(&[image], &owner(), t0(), t0()).await.unwrap(), 0);

        let decided = t0() + Duration::days(2);
        assert_eq!(r.on_decision(&[doc, image], &owner(), decided, decided).await.unwrap(), 1);
        assert_eq!(r.sweep(decided + Duration::days(29)).await.unwrap(), 0, "not yet");
        assert_eq!(r.sweep(decided + Duration::days(30)).await.unwrap(), 1);
        assert_eq!(fx.assets.find_by_id(&doc).await.unwrap().unwrap().state(), AssetState::Deleted);
        assert_eq!(fx.assets.find_by_id(&image).await.unwrap().unwrap().state(), AssetState::Ready);
    }

    #[tokio::test]
    async fn an_undecided_document_goes_at_its_backstop_unless_held() {
        let fx = Fixture::new();
        let (orphan, _) = fx.ready_asset(MediaKind::PrivateDocument).await;
        let (held, _) = fx.ready_asset(MediaKind::PrivateDocument).await;
        {
            let mut a = fx.assets.find_by_id(&held).await.unwrap().unwrap();
            a.place_legal_hold(t0());
            fx.assets.save(&a).await.unwrap();
        }
        let r = retention(&fx);
        assert_eq!(r.sweep(t0() + PRIVATE_DOCUMENT_BACKSTOP).await.unwrap(), 1);
        assert_eq!(fx.assets.find_by_id(&orphan).await.unwrap().unwrap().state(), AssetState::Deleted);
        assert_eq!(fx.assets.find_by_id(&held).await.unwrap().unwrap().state(), AssetState::Ready);
    }
}
