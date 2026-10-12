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
}

/// media's asset metadata (`GetAsset`).
#[async_trait]
pub trait MediaCatalog: Send + Sync + 'static {
    /// The asset, or `None` when media does not know it. Errors are
    /// `MediaUnavailable`; the write fails closed.
    async fn asset(&self, id: &MediaAssetId) -> Result<Option<MediaAssetFacts>, PostError>;
}

/// Checks the media assets attachments name (#907): each must be a usable
/// post image or video and, for an edge caller (`account`), that account's
/// own — else anyone could show someone's SIGNED media through a post. One
/// answer for all refusals, so a probe learns nothing of others' assets. With
/// no `catalog` (media not configured), the assets are dropped: the
/// attachments still deliver by their URLs.
pub async fn check_media(
    catalog: Option<&dyn MediaCatalog>,
    account: Option<&str>,
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
        let ok = facts.is_some_and(|facts| {
            facts.attachable && facts.usable && account.is_none_or(|account| facts.owner_account_id == account)
        });
        if !ok {
            return Err(PostError::MediaAssetNotUsable { index, asset_id: id.as_str() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::domain::value_object::{CdnUrl, MimeType};

    struct FakeCatalog(Result<HashMap<String, MediaAssetFacts>, ()>);

    #[async_trait]
    impl MediaCatalog for FakeCatalog {
        async fn asset(&self, id: &MediaAssetId) -> Result<Option<MediaAssetFacts>, PostError> {
            match &self.0 {
                Ok(assets) => Ok(assets.get(&id.as_str()).cloned()),
                Err(()) => Err(PostError::MediaUnavailable { reason: "down".into() }),
            }
        }
    }

    const MINE: &str = "0199b8c4-0000-7000-8000-000000000001";
    const ACCOUNT: &str = "acct-1";

    fn facts(owner: &str, attachable: bool, usable: bool) -> MediaAssetFacts {
        MediaAssetFacts { owner_account_id: owner.into(), attachable, usable }
    }

    fn catalog(f: MediaAssetFacts) -> FakeCatalog {
        FakeCatalog(Ok(HashMap::from([(MINE.to_owned(), f)])))
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
        check_media(Some(&catalog(facts(ACCOUNT, true, true))), Some(ACCOUNT), &mut a).await.unwrap();
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
            let err = check_media(Some(&catalog), Some(ACCOUNT), &mut attachments(Some(asset))).await.unwrap_err();
            assert!(matches!(err, PostError::MediaAssetNotUsable { index: 0, .. }), "{err:?}");
        }
    }

    #[tokio::test]
    async fn a_mesh_caller_is_not_bound_to_an_account() {
        let mut a = attachments(Some(MINE));
        check_media(Some(&catalog(facts("acct-2", true, true))), None, &mut a).await.unwrap();
    }

    #[tokio::test]
    async fn media_down_fails_the_write_retryably() {
        let err = check_media(Some(&FakeCatalog(Err(()))), Some(ACCOUNT), &mut attachments(Some(MINE))).await.unwrap_err();
        assert!(matches!(err, PostError::MediaUnavailable { .. }));
        assert!(error::AppError::is_retryable(&err));
    }

    /// Not configured: the attachments go through by their URLs, without assets.
    #[tokio::test]
    async fn without_media_the_assets_are_dropped() {
        let mut a = attachments(Some(MINE));
        check_media(None, Some(ACCOUNT), &mut a).await.unwrap();
        assert!(a[0].asset_id.is_none());
        check_media(None, Some(ACCOUNT), &mut attachments(None)).await.unwrap();
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
