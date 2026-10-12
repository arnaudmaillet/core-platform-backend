use async_trait::async_trait;
use uuid::Uuid;

use crate::error::ChatError;

/// What chat needs to know of a media asset before a message names it
/// (#919 step 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaAssetFacts {
    /// The account that uploaded it.
    pub owner_account_id: String,
    /// An image or a video (not an avatar, not a private document).
    pub attachable:       bool,
    /// Not failed, quarantined or deleted.
    pub usable:           bool,
    /// Delivered SIGNED only.
    pub signed:           bool,
    /// READY: its delivery visibility may change.
    pub ready:            bool,
    /// A video: left as it is until signed HLS exists (#921).
    pub video:            bool,
}

/// media's asset metadata and delivery visibility.
#[async_trait]
pub trait MessageMedia: Send + Sync + 'static {
    /// The asset, or `None` when media does not know it. Errors are
    /// `MediaUnavailable`.
    async fn asset(&self, id: Uuid) -> Result<Option<MediaAssetFacts>, ChatError>;

    /// Delivers the asset SIGNED, as its owning account; `false` when media
    /// refuses because it is not READY yet. Idempotent.
    async fn make_signed(&self, id: Uuid, owner: &str) -> Result<bool, ChatError>;
}

/// Checks the asset a message names (#919 step 3): it must be an image or a
/// video of the sender's account (`account`, the edge caller's; `None` on the
/// mesh) that is usable — one answer for every refusal (`CHT-2011`). Chat
/// media is SIGNED only: a READY public image is made SIGNED here; one still
/// processing must have been uploaded SIGNED (`CHT-2012`). Videos are left as
/// they are until signed HLS exists (#921). Without `media` (not configured)
/// the asset is dropped: the message keeps its `media_ref`.
pub async fn checked_media(
    media: Option<&dyn MessageMedia>,
    account: Option<&str>,
    asset: Option<Uuid>,
) -> Result<Option<Uuid>, ChatError> {
    let Some(asset) = asset else { return Ok(None) };
    let Some(media) = media else {
        tracing::warn!(%asset, "media is not configured: message asset dropped");
        return Ok(None);
    };
    let not_usable = || ChatError::MediaAssetNotUsable { asset_id: asset.to_string() };
    let facts = media
        .asset(asset)
        .await?
        .filter(|f| f.attachable && f.usable && account.is_none_or(|a| f.owner_account_id == a))
        .ok_or_else(not_usable)?;
    if !facts.signed
        && !facts.video
        && (!facts.ready || !media.make_signed(asset, &facts.owner_account_id).await?)
    {
        return Err(ChatError::MediaNotSigned { asset_id: asset.to_string() });
    }
    Ok(Some(asset))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct FakeMedia(Result<Option<MediaAssetFacts>, ()>, Mutex<Vec<Uuid>>);

    #[async_trait]
    impl MessageMedia for FakeMedia {
        async fn asset(&self, _: Uuid) -> Result<Option<MediaAssetFacts>, ChatError> {
            self.0.clone().map_err(|()| ChatError::MediaUnavailable { reason: "down".into() })
        }
        async fn make_signed(&self, id: Uuid, owner: &str) -> Result<bool, ChatError> {
            assert_eq!(owner, "acct");
            self.1.lock().unwrap().push(id);
            Ok(true)
        }
    }

    fn facts() -> MediaAssetFacts {
        MediaAssetFacts {
            owner_account_id: "acct".into(),
            attachable:       true,
            usable:           true,
            signed:           false,
            ready:            true,
            video:            false,
        }
    }

    fn media(f: MediaAssetFacts) -> FakeMedia {
        FakeMedia(Ok(Some(f)), Mutex::default())
    }

    #[tokio::test]
    async fn the_senders_ready_image_is_made_signed() {
        let (asset, m) = (Uuid::now_v7(), media(facts()));
        assert_eq!(checked_media(Some(&m), Some("acct"), Some(asset)).await.unwrap(), Some(asset));
        assert_eq!(*m.1.lock().unwrap(), vec![asset]);
    }

    #[tokio::test]
    async fn a_signed_image_or_a_video_is_left_as_it_is() {
        for f in [MediaAssetFacts { signed: true, ready: false, ..facts() }, MediaAssetFacts { video: true, ..facts() }] {
            let m = media(f);
            checked_media(Some(&m), Some("acct"), Some(Uuid::now_v7())).await.unwrap();
            assert!(m.1.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn a_processing_public_image_must_have_been_uploaded_signed() {
        let m = media(MediaAssetFacts { ready: false, ..facts() });
        let err = checked_media(Some(&m), Some("acct"), Some(Uuid::now_v7())).await.unwrap_err();
        assert!(matches!(err, ChatError::MediaNotSigned { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn another_accounts_unknown_or_unusable_asset_is_refused() {
        let cases = [
            (media(facts()), "someone-else"),
            (FakeMedia(Ok(None), Mutex::default()), "acct"),
            (media(MediaAssetFacts { usable: false, ..facts() }), "acct"),
            (media(MediaAssetFacts { attachable: false, ..facts() }), "acct"),
        ];
        for (m, account) in cases {
            let err = checked_media(Some(&m), Some(account), Some(Uuid::now_v7())).await.unwrap_err();
            assert!(matches!(err, ChatError::MediaAssetNotUsable { .. }), "{err:?}");
        }
    }

    #[tokio::test]
    async fn media_down_is_retryable_and_none_configured_drops_the_asset() {
        let down = FakeMedia(Err(()), Mutex::default());
        let err = checked_media(Some(&down), Some("acct"), Some(Uuid::now_v7())).await.unwrap_err();
        assert!(matches!(err, ChatError::MediaUnavailable { .. }));
        assert_eq!(checked_media(None, Some("acct"), Some(Uuid::now_v7())).await.unwrap(), None);
        assert_eq!(checked_media(Some(&down), Some("acct"), None).await.unwrap(), None);
    }
}
