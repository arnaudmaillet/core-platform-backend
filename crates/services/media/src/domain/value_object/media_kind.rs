use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::MediaError;

/// What kind of media an asset holds. `kind` is the policy discriminant: it picks
/// the MIME allowlist, the size ceiling, the rendition ladder, and the leading
/// segment of the content-addressed storage path.
///
/// Images (`Avatar`, `PostImage`) are fully served. `Video` carries the ingest
/// policy (allowlist / ceiling / storage path) but its transformation pipeline is
/// still being built — the gRPC surface rejects video upload tickets as
/// UNIMPLEMENTED until the probe + transcode adapters land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    /// A profile/account avatar — square-cropped ladder.
    Avatar,
    /// An image attached to a post — feed-sized ladder.
    PostImage,
    /// A video attached to a post — adaptive-streaming ladder (manifest + poster).
    Video,
    /// Evidence for a verification request (#777): an identity document, a
    /// registry extract. **Private**: no renditions, never publicly resolvable;
    /// only its owner sees its metadata and only staff (over the mesh) get a
    /// short-lived signed link. Purged after the request's decision.
    PrivateDocument,
}

impl MediaKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avatar => "avatar",
            Self::PostImage => "post_image",
            Self::Video => "video",
            Self::PrivateDocument => "private_document",
        }
    }

    /// Never served publicly: no renditions, no CDN, no `ResolveDelivery`.
    pub fn is_private(&self) -> bool {
        matches!(self, Self::PrivateDocument)
    }

    /// Leading segment of the content-addressed storage key (`{segment}/{hash}/…`).
    pub fn path_segment(&self) -> &'static str {
        match self {
            Self::Avatar => "avatars",
            Self::PostImage => "post-images",
            Self::Video => "post-videos",
            // Never content-addressed: a private document lives at
            // `private/documents/{asset_id}` (see `StorageKey::private_document`).
            Self::PrivateDocument => "private-documents",
        }
    }

    /// The MIME types accepted for this kind (canonical, lowercase). Images share
    /// the modern still-image set; video accepts the interop container baseline
    /// (MP4 + QuickTime). The concrete rendition output format is chosen by the
    /// pipeline, independent of what was uploaded.
    pub fn allowed_mime_types(&self) -> &'static [&'static str] {
        match self {
            Self::Avatar | Self::PostImage => {
                &["image/jpeg", "image/png", "image/webp", "image/avif"]
            }
            Self::Video => &["video/mp4", "video/quicktime"],
            Self::PrivateDocument => &["image/jpeg", "image/png", "image/heic", "application/pdf"],
        }
    }

    /// Hard byte ceiling enforced at ticket time and re-checked on finalize. The
    /// video ceiling is a provisional short-form cap pending the product decision
    /// on max duration/size.
    pub fn max_bytes(&self) -> u64 {
        match self {
            Self::Avatar => 10 * 1024 * 1024,      // 10 MiB
            Self::PostImage => 25 * 1024 * 1024,   // 25 MiB
            Self::Video => 200 * 1024 * 1024,      // 200 MiB (short-form cap)
            Self::PrivateDocument => 10 * 1024 * 1024, // 10 MiB
        }
    }
}

impl fmt::Display for MediaKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for MediaKind {
    type Error = MediaError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "avatar" => Ok(Self::Avatar),
            "post_image" => Ok(Self::PostImage),
            "video" => Ok(Self::Video),
            "private_document" => Ok(Self::PrivateDocument),
            other => Err(MediaError::InvalidMediaKind {
                kind: other.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_round_trip() {
        for k in [MediaKind::Avatar, MediaKind::PostImage, MediaKind::Video, MediaKind::PrivateDocument] {
            assert_eq!(MediaKind::try_from(k.as_str()).unwrap(), k);
        }
    }

    #[test]
    fn unknown_kind_is_rejected() {
        assert!(matches!(
            MediaKind::try_from("hologram").unwrap_err(),
            MediaError::InvalidMediaKind { .. }
        ));
    }

    #[test]
    fn post_images_allow_a_larger_ceiling_than_avatars() {
        assert!(MediaKind::PostImage.max_bytes() > MediaKind::Avatar.max_bytes());
    }

    #[test]
    fn allowlist_is_the_modern_still_image_set() {
        assert!(MediaKind::Avatar.allowed_mime_types().contains(&"image/webp"));
        assert!(!MediaKind::Avatar.allowed_mime_types().contains(&"video/mp4"));
    }

    #[test]
    fn video_accepts_the_container_baseline_not_images() {
        let allowed = MediaKind::Video.allowed_mime_types();
        assert!(allowed.contains(&"video/mp4"));
        assert!(allowed.contains(&"video/quicktime"));
        assert!(!allowed.contains(&"image/jpeg"));
    }

    #[test]
    fn video_has_a_larger_ceiling_and_its_own_storage_segment() {
        assert!(MediaKind::Video.max_bytes() > MediaKind::PostImage.max_bytes());
        assert_eq!(MediaKind::Video.path_segment(), "post-videos");
    }
}
