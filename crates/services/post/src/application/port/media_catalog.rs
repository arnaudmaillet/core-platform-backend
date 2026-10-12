use async_trait::async_trait;

use crate::domain::entity::MediaAttachment;
use crate::domain::value_object::MediaAssetId;
use crate::error::PostError;

/// What post needs to know of a media asset before an attachment names it
/// (#907).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaAssetFacts {
    /// The account that uploaded it.
    pub owner_account_id: String,
    /// A post image or a video (not an avatar, not a private document).
    pub attachable:       bool,
    /// Not failed, quarantined or deleted (a pending or processing asset is
    /// fine: the post shows its blurhash until it is ready).
    pub usable:           bool,
    /// Delivered SIGNED only (#919).
    pub signed:           bool,
    /// READY: its delivery visibility may change.
    pub ready:            bool,
    /// Its delivery may go SIGNED: an image, or a video once media plays
    /// SIGNED videos through signed cookies (#921; `POST_SIGNED_VIDEOS_ENABLED`).
    /// An unsignable one is left as it is.
    pub signable:         bool,
}

/// What a delivery-visibility change came to (#919 step 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetDelivery {
    /// It is delivered that way now (or already was).
    Done,
    /// Not READY yet: its visibility cannot change (media refuses).
    NotReady,
    /// media does not know it (deleted).
    Gone,
}

/// media's asset metadata (`GetAsset`).
#[async_trait]
pub trait MediaCatalog: Send + Sync + 'static {
    /// The asset, or `None` when media does not know it. Errors are
    /// `MediaUnavailable`; the write fails closed.
    async fn asset(&self, id: &MediaAssetId) -> Result<Option<MediaAssetFacts>, PostError>;

    /// Delivers the asset SIGNED (`signed`) or PUBLIC (#919 step 3), as its
    /// owning account (`owner`; looked up when `None`). Idempotent. Errors are
    /// `MediaUnavailable`.
    async fn set_delivery(&self, id: &MediaAssetId, owner: Option<&str>, signed: bool) -> Result<SetDelivery, PostError>;
}

/// Checks the media assets attachments name (#907): each must be a usable
/// post image or video and, for an edge caller (`account`), that account's
/// own — else anyone could show someone's SIGNED media through a post. One
/// answer for all refusals, so a probe learns nothing of others' assets. With
/// no `catalog` (media not configured), the assets are dropped: the
/// attachments still deliver by their URLs.
///
/// A private author's images are SIGNED only (#919 step 3): a READY public one
/// is made SIGNED here; one still processing must have been uploaded SIGNED
/// (`PST-3006`), as its visibility cannot change before it is ready. Videos
/// follow once signed videos are enabled (#921); until then they are left as
/// they are.
/// Whether `author`'s attachments must be SIGNED: asked only when one names
/// an asset that media will check.
pub async fn author_private_for(
    privacy: &dyn crate::application::port::AuthorPrivacy,
    catalog: Option<&dyn MediaCatalog>,
    author: &crate::domain::value_object::ProfileId,
    attachments: &[MediaAttachment],
) -> Result<bool, PostError> {
    if catalog.is_none() || attachments.iter().all(|a| a.asset_id.is_none()) {
        return Ok(false);
    }
    privacy.is_private(author).await
}

pub async fn check_media(
    catalog: Option<&dyn MediaCatalog>,
    account: Option<&str>,
    author_private: bool,
    attachments: &mut [MediaAttachment],
) -> Result<(), PostError> {
    let Some(catalog) = catalog else {
        for attachment in attachments.iter_mut().filter(|a| a.asset_id.is_some()) {
            tracing::warn!(asset_id = ?attachment.asset_id, "media is not configured: attachment asset dropped");
            attachment.asset_id = None;
        }
        return Ok(());
    };
    // One lookup per asset, all at once (a carousel names up to ten).
    let named: Vec<(usize, MediaAssetId)> =
        attachments.iter().enumerate().filter_map(|(index, a)| a.asset_id.map(|id| (index, id))).collect();
    let found = futures::future::try_join_all(named.iter().map(|(_, id)| catalog.asset(id))).await?;
    for ((index, id), facts) in named.into_iter().zip(found) {
        let Some(facts) = facts.filter(|facts| {
            facts.attachable && facts.usable && account.is_none_or(|account| facts.owner_account_id == account)
        }) else {
            return Err(PostError::MediaAssetNotUsable { index, asset_id: id.as_str() });
        };
        if author_private && !facts.signed && facts.signable {
            if !facts.ready {
                return Err(PostError::MediaNotSigned { index, asset_id: id.as_str() });
            }
            match catalog.set_delivery(&id, Some(&facts.owner_account_id), true).await? {
                SetDelivery::Done => {}
                SetDelivery::NotReady => return Err(PostError::MediaNotSigned { index, asset_id: id.as_str() }),
                SetDelivery::Gone => return Err(PostError::MediaAssetNotUsable { index, asset_id: id.as_str() }),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::domain::value_object::{CdnUrl, MimeType};

    struct FakeCatalog(Result<HashMap<String, MediaAssetFacts>, ()>, std::sync::Mutex<Vec<(String, bool)>>);

    #[async_trait]
    impl MediaCatalog for FakeCatalog {
        async fn asset(&self, id: &MediaAssetId) -> Result<Option<MediaAssetFacts>, PostError> {
            match &self.0 {
                Ok(assets) => Ok(assets.get(&id.as_str()).cloned()),
                Err(()) => Err(PostError::MediaUnavailable { reason: "down".into() }),
            }
        }

        async fn set_delivery(&self, id: &MediaAssetId, _owner: Option<&str>, signed: bool) -> Result<SetDelivery, PostError> {
            self.1.lock().unwrap().push((id.as_str(), signed));
            Ok(SetDelivery::Done)
        }
    }

    impl FakeCatalog {
        fn new(assets: Result<HashMap<String, MediaAssetFacts>, ()>) -> Self {
            Self(assets, Default::default())
        }

        fn changes(&self) -> Vec<(String, bool)> {
            self.1.lock().unwrap().clone()
        }
    }

    const MINE: &str = "0199b8c4-0000-7000-8000-000000000001";
    const ACCOUNT: &str = "acct-1";

    fn facts(owner: &str, attachable: bool, usable: bool) -> MediaAssetFacts {
        MediaAssetFacts { owner_account_id: owner.into(), attachable, usable, signed: false, ready: true, signable: true }
    }

    fn catalog(f: MediaAssetFacts) -> FakeCatalog {
        FakeCatalog::new(Ok(HashMap::from([(MINE.to_owned(), f)])))
    }

    fn attachments(asset: Option<&str>) -> Vec<MediaAttachment> {
        vec![MediaAttachment {
            cdn_url:          CdnUrl::new("https://cdn.example/a.jpg").unwrap(),
            mime_type:        MimeType::new("image/jpeg").unwrap(),
            width:            10,
            height:           10,
            thumbnail_url:    None,
            duration_seconds: None,
            asset_id:         asset.map(|a| MediaAssetId::try_from(a).unwrap()),
        }]
    }

    #[tokio::test]
    async fn the_callers_own_usable_post_media_is_accepted() {
        let mut a = attachments(Some(MINE));
        check_media(Some(&catalog(facts(ACCOUNT, true, true))), Some(ACCOUNT), false, &mut a).await.unwrap();
        assert!(a[0].asset_id.is_some());
    }

    /// #907: one refusal for every case, so a probe learns nothing of
    /// someone else's assets.
    #[tokio::test]
    async fn another_accounts_unknown_unusable_or_wrong_kind_asset_is_refused() {
        let other_unknown = "0199b8c4-0000-7000-8000-000000000002";
        let cases = [
            (catalog(facts("acct-2", true, true)), MINE),
            (catalog(facts(ACCOUNT, false, true)), MINE),
            (catalog(facts(ACCOUNT, true, false)), MINE),
            (catalog(facts(ACCOUNT, true, true)), other_unknown),
        ];
        for (catalog, asset) in cases {
            let err = check_media(Some(&catalog), Some(ACCOUNT), false, &mut attachments(Some(asset))).await.unwrap_err();
            assert!(matches!(err, PostError::MediaAssetNotUsable { index: 0, .. }), "{err:?}");
        }
    }

    #[tokio::test]
    async fn a_mesh_caller_is_not_bound_to_an_account() {
        let mut a = attachments(Some(MINE));
        check_media(Some(&catalog(facts("acct-2", true, true))), None, false, &mut a).await.unwrap();
    }

    #[tokio::test]
    async fn media_down_fails_the_write_retryably() {
        let err = check_media(Some(&FakeCatalog::new(Err(()))), Some(ACCOUNT), false, &mut attachments(Some(MINE))).await.unwrap_err();
        assert!(matches!(err, PostError::MediaUnavailable { .. }));
        assert!(error::AppError::is_retryable(&err));
    }

    /// Not configured: the attachments go through by their URLs, without assets.
    #[tokio::test]
    async fn without_media_the_assets_are_dropped() {
        let mut a = attachments(Some(MINE));
        check_media(None, Some(ACCOUNT), false, &mut a).await.unwrap();
        assert!(a[0].asset_id.is_none());
        check_media(None, Some(ACCOUNT), false, &mut attachments(None)).await.unwrap();
    }

    /// #919 step 3: a private author's READY public asset is made SIGNED; one
    /// still processing must have been uploaded SIGNED; a SIGNED one is fine.
    #[tokio::test]
    async fn a_private_authors_media_goes_signed() {
        let ready = catalog(facts(ACCOUNT, true, true));
        check_media(Some(&ready), Some(ACCOUNT), true, &mut attachments(Some(MINE))).await.unwrap();
        assert_eq!(ready.changes(), vec![(MINE.to_owned(), true)]);

        let processing = catalog(MediaAssetFacts { ready: false, ..facts(ACCOUNT, true, true) });
        let err = check_media(Some(&processing), Some(ACCOUNT), true, &mut attachments(Some(MINE))).await.unwrap_err();
        assert!(matches!(err, PostError::MediaNotSigned { index: 0, .. }), "{err:?}");

        let signed = catalog(MediaAssetFacts { ready: false, signed: true, ..facts(ACCOUNT, true, true) });
        check_media(Some(&signed), Some(ACCOUNT), true, &mut attachments(Some(MINE))).await.unwrap();
        assert!(signed.changes().is_empty());

        let video = catalog(MediaAssetFacts { signable: false, ..facts(ACCOUNT, true, true) });
        check_media(Some(&video), Some(ACCOUNT), true, &mut attachments(Some(MINE))).await.unwrap();
        assert!(video.changes().is_empty(), "a video stays public until signed videos are on (#921)");

        let public_author = catalog(facts(ACCOUNT, true, true));
        check_media(Some(&public_author), Some(ACCOUNT), false, &mut attachments(Some(MINE))).await.unwrap();
        assert!(public_author.changes().is_empty(), "a public author's media is left as it is");
    }

    /// Attachments stored before #907 read back without an asset.
    #[test]
    fn stored_attachments_without_an_asset_still_read() {
        let old = r#"[{"cdn_url":"https://cdn.example/a.jpg","mime_type":"image/jpeg","width":1,"height":1,"thumbnail_url":null,"duration_seconds":null}]"#;
        let parsed: Vec<MediaAttachment> = serde_json::from_str(old).unwrap();
        assert!(parsed[0].asset_id.is_none());
        let with = serde_json::to_string(&attachments(Some(MINE))).unwrap();
        assert!(with.contains(MINE));
        assert!(!serde_json::to_string(&attachments(None)).unwrap().contains("asset_id"));
    }
}
