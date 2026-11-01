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
    /// A short clip an animated avatar is baked from (#945): muted, its first
    /// 3 s, baked into sprite sheets (128 / 256 px cells) and a last-frame
    /// still — never served as a video.
    AvatarClip,
    /// A loader its owner uploaded (#975): the app's loading animation, a
    /// horizontal strip of [`LOADER_FRAME_PX`]-px square frames (1 to
    /// [`LOADER_MAX_FRAMES`]), PNG or HEIC, normalised by the client.
    /// **Private** to its owner: no renditions, never on the CDN; only its
    /// owner gets a short-lived signed link (`ResolveLoaders`). `Screen`ed at
    /// upload like every kind; no review while nobody else sees it.
    Loader,
}

/// A loader frame's side, in px (#975).
pub const LOADER_FRAME_PX: u32 = 128;
/// The most frames a loader strip holds: 2 s at 30 fps (#975).
pub const LOADER_MAX_FRAMES: u32 = 60;

impl MediaKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avatar => "avatar",
            Self::PostImage => "post_image",
            Self::Video => "video",
            Self::PrivateDocument => "private_document",
            Self::AvatarClip => "avatar_clip",
            Self::Loader => "loader",
        }
    }

    /// How an asset of this kind is delivered unless its owner asks for
    /// stricter (#919): every public kind is PUBLIC; a private kind is never
    /// delivered publicly at all, SIGNED as a backstop.
    pub fn default_visibility(&self) -> super::DeliveryVisibility {
        if self.is_private() { super::DeliveryVisibility::Signed } else { super::DeliveryVisibility::Public }
    }

    /// Never served publicly: no renditions, no CDN, no `ResolveDelivery`.
    pub fn is_private(&self) -> bool {
        matches!(self, Self::PrivateDocument | Self::Loader)
    }

    /// The upload's verified dimensions fit the kind: a loader is a strip of
    /// 1–[`LOADER_MAX_FRAMES`] square [`LOADER_FRAME_PX`]-px frames (#975);
    /// other kinds have no shape of their own here.
    pub fn check_dimensions(&self, dimensions: Option<super::Dimensions>) -> Result<(), MediaError> {
        if *self != Self::Loader {
            return Ok(());
        }
        let strip = dimensions.is_some_and(|d| {
            let frames = d.width() / LOADER_FRAME_PX;
            d.height() == LOADER_FRAME_PX && d.width() % LOADER_FRAME_PX == 0 && (1..=LOADER_MAX_FRAMES).contains(&frames)
        });
        if strip {
            Ok(())
        } else {
            Err(MediaError::CorruptMedia {
                reason: format!(
                    "a loader is one strip of 1–{LOADER_MAX_FRAMES} frames of {LOADER_FRAME_PX} × {LOADER_FRAME_PX} px, got {}",
                    dimensions.map_or("no dimensions".to_owned(), |d| format!("{} × {}", d.width(), d.height()))
                ),
            })
        }
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
            Self::AvatarClip => "avatar-clips",
            // Never content-addressed: `private/loaders/{asset_id}`.
            Self::Loader => "loaders",
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
            Self::Video | Self::AvatarClip => &["video/mp4", "video/quicktime"],
            Self::PrivateDocument => &["image/jpeg", "image/png", "image/heic", "application/pdf"],
            Self::Loader => &["image/png", "image/heic"],
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
            Self::AvatarClip => 20 * 1024 * 1024,  // 20 MiB (a few seconds)
            Self::Loader => 1024 * 1024,           // 1 MiB (normalised by the client)
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
            "avatar_clip" => Ok(Self::AvatarClip),
            "loader" => Ok(Self::Loader),
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
        for k in [MediaKind::Avatar, MediaKind::PostImage, MediaKind::Video, MediaKind::PrivateDocument, MediaKind::AvatarClip, MediaKind::Loader] {
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

    /// #975: a loader is a strip of square 128 px frames, 1 to 60.
    #[test]
    fn a_loader_is_a_strip_of_one_to_sixty_frames() {
        let dims = |w, h| Some(crate::domain::value_object::Dimensions::new(w, h).unwrap());
        for ok in [dims(128, 128), dims(128 * 60, 128), dims(128 * 12, 128)] {
            assert!(MediaKind::Loader.check_dimensions(ok).is_ok(), "{ok:?}");
        }
        for bad in [None, dims(128 * 61, 128), dims(128, 256), dims(200, 128), dims(256, 64)] {
            assert!(matches!(MediaKind::Loader.check_dimensions(bad), Err(MediaError::CorruptMedia { .. })), "{bad:?}");
        }
        assert!(MediaKind::Avatar.check_dimensions(None).is_ok());
        assert!(MediaKind::Loader.is_private() && MediaKind::Loader.max_bytes() == 1024 * 1024);
    }
}
