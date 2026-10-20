use async_trait::async_trait;

use crate::domain::value_object::{AccountId, MediaAssetId};
use crate::error::ProfileError;

/// media's images (#907): an avatar or banner may only show an image of the
/// profile's own account — else anyone could show someone's SIGNED media.
#[async_trait]
pub trait MediaImages: Send + Sync + 'static {
    /// `Ok` when `asset` is an image (`AVATAR` or `POST_IMAGE`) of `account`,
    /// not failed, quarantined or deleted (a processing one is fine);
    /// [`ProfileError::MediaAssetNotUsable`] otherwise, one answer for every
    /// case; [`ProfileError::MediaUnavailable`] when media cannot answer.
    async fn check_image(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError>;

    /// `Ok` when `asset` is a READY video (`VIDEO`, a post's) of `account`
    /// (#947: an animated banner); [`ProfileError::MediaAssetNotUsable`]
    /// otherwise; [`ProfileError::MediaUnavailable`] when media cannot answer.
    async fn check_video(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError>;

    /// `Ok` when `asset` is an animated avatar's clip (`AVATAR_CLIP`) of
    /// `account`, not failed, quarantined or deleted (#945; a processing one
    /// is fine: its sheets follow); [`ProfileError::MediaAssetNotUsable`]
    /// otherwise; [`ProfileError::MediaUnavailable`] when media cannot answer.
    async fn check_avatar_clip(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError>;
}

/// The asset an avatar / banner update names, checked (#907). Without
/// `images` (media not configured) it is dropped: the image still delivers by
/// its URL.
pub async fn checked_image(
    images: Option<&dyn MediaImages>,
    asset: Option<&str>,
    account: &AccountId,
) -> Result<Option<MediaAssetId>, ProfileError> {
    let Some(asset) = asset.filter(|a| !a.is_empty()).map(MediaAssetId::try_from).transpose()? else {
        return Ok(None);
    };
    let Some(images) = images else {
        tracing::warn!(%asset, "media is not configured: image asset dropped");
        return Ok(None);
    };
    images.check_image(&asset, account).await?;
    Ok(Some(asset))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accepts its one image for its one account; or media is down.
    struct FakeImages(Option<(MediaAssetId, AccountId)>);

    #[async_trait]
    impl MediaImages for FakeImages {
        async fn check_image(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError> {
            match self.0 {
                None => Err(ProfileError::MediaUnavailable { reason: "down".into() }),
                Some((a, o)) if a == *asset && o == *account => Ok(()),
                Some(_) => Err(ProfileError::MediaAssetNotUsable { id: asset.as_str() }),
            }
        }

        async fn check_video(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError> {
            self.check_image(asset, account).await
        }

        async fn check_avatar_clip(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError> {
            self.check_image(asset, account).await
        }
    }

    fn ids() -> (MediaAssetId, AccountId) {
        let asset = MediaAssetId::from_uuid(uuid::Uuid::now_v7());
        let account = AccountId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap();
        (asset, account)
    }

    #[tokio::test]
    async fn the_accounts_own_image_is_kept() {
        let (asset, account) = ids();
        let images = FakeImages(Some((asset, account)));
        let kept = checked_image(Some(&images), Some(&asset.as_str()), &account).await.unwrap();
        assert_eq!(kept, Some(asset));
    }

    #[tokio::test]
    async fn another_accounts_or_a_malformed_asset_is_refused() {
        let (asset, account) = ids();
        let (_, other) = ids();
        let images = FakeImages(Some((asset, account)));
        let err = checked_image(Some(&images), Some(&asset.as_str()), &other).await.unwrap_err();
        assert!(matches!(err, ProfileError::MediaAssetNotUsable { .. }));
        let err = checked_image(Some(&images), Some("not-a-uuid"), &account).await.unwrap_err();
        assert!(matches!(err, ProfileError::MediaAssetNotUsable { .. }));
    }

    #[tokio::test]
    async fn media_down_fails_the_update_retryably() {
        let (asset, account) = ids();
        let err = checked_image(Some(&FakeImages(None)), Some(&asset.as_str()), &account).await.unwrap_err();
        assert!(matches!(err, ProfileError::MediaUnavailable { .. }));
    }

    #[tokio::test]
    async fn no_asset_or_no_media_keeps_none() {
        let (asset, account) = ids();
        assert_eq!(checked_image(Some(&FakeImages(None)), None, &account).await.unwrap(), None);
        assert_eq!(checked_image(Some(&FakeImages(None)), Some(""), &account).await.unwrap(), None);
        assert_eq!(checked_image(None, Some(&asset.as_str()), &account).await.unwrap(), None, "not configured: dropped");
    }
}
