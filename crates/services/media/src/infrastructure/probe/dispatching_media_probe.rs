use std::sync::Arc;

use async_trait::async_trait;

use crate::application::port::{MediaProbe, MediaProbeReport};
use crate::domain::value_object::{MimeType, StorageKey};
use crate::error::MediaError;

/// Routes a probe to the image, video or document backend by the object's
/// declared type (a PDF or a HEIC — private documents only, #777 — goes to the
/// document probe).
/// `declared_mime` is safe to route on: the upload ticket already validated it
/// against the kind's allowlist, so a `video/*` declaration means a video asset
/// (and the chosen backend then does the real "never trust the client" check on
/// the bytes). Anything non-video falls through to the image probe.
pub struct DispatchingMediaProbe {
    image:    Arc<dyn MediaProbe>,
    video:    Arc<dyn MediaProbe>,
    document: Option<Arc<dyn MediaProbe>>,
}

impl DispatchingMediaProbe {
    pub fn new(image: Arc<dyn MediaProbe>, video: Arc<dyn MediaProbe>) -> Self {
        Self { image, video, document: None }
    }

    /// Routes PDF and HEIC declarations to `document`.
    pub fn with_documents(mut self, document: Arc<dyn MediaProbe>) -> Self {
        self.document = Some(document);
        self
    }
}

#[async_trait]
impl MediaProbe for DispatchingMediaProbe {
    async fn probe(
        &self,
        key: &StorageKey,
        declared_mime: &MimeType,
    ) -> Result<MediaProbeReport, MediaError> {
        if declared_mime.is_video() {
            self.video.probe(key, declared_mime).await
        } else if let Some(document) = self.document.as_ref().filter(|_| {
            matches!(declared_mime.as_str(), "application/pdf" | "image/heic")
        }) {
            document.probe(key, declared_mime).await
        } else {
            self.image.probe(key, declared_mime).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::domain::value_object::{AssetId, ContentHash, Dimensions};

    /// A probe that records whether it was called and returns a canned report.
    struct SpyProbe {
        tag: &'static str,
        called: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl MediaProbe for SpyProbe {
        async fn probe(
            &self,
            _key: &StorageKey,
            _declared_mime: &MimeType,
        ) -> Result<MediaProbeReport, MediaError> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(MediaProbeReport {
                mime_type: MimeType::new(format!("routed/{}", self.tag)).unwrap(),
                byte_size: 1,
                dimensions: Some(Dimensions::new(1, 1).unwrap()),
                content_hash: ContentHash::new("a".repeat(64)).unwrap(),
            })
        }
    }

    fn spy(tag: &'static str) -> (Arc<SpyProbe>, Arc<std::sync::atomic::AtomicBool>) {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        (Arc::new(SpyProbe { tag, called: called.clone() }), called)
    }

    #[tokio::test]
    async fn routes_video_declarations_to_the_video_probe() {
        let (image, image_hit) = spy("image");
        let (video, video_hit) = spy("video");
        let dispatch = DispatchingMediaProbe::new(image, video);

        let key = StorageKey::staging(AssetId::new());
        let report =
            dispatch.probe(&key, &MimeType::new("video/mp4").unwrap()).await.unwrap();

        assert_eq!(report.mime_type.as_str(), "routed/video");
        assert!(video_hit.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!image_hit.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn routes_image_declarations_to_the_image_probe() {
        let (image, image_hit) = spy("image");
        let (video, video_hit) = spy("video");
        let dispatch = DispatchingMediaProbe::new(image, video);

        let key = StorageKey::staging(AssetId::new());
        let report =
            dispatch.probe(&key, &MimeType::new("image/jpeg").unwrap()).await.unwrap();

        assert_eq!(report.mime_type.as_str(), "routed/image");
        assert!(image_hit.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!video_hit.load(std::sync::atomic::Ordering::SeqCst));
    }
}
