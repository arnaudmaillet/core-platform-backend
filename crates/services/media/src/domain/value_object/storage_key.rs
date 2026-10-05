use std::fmt;

use serde::{Deserialize, Serialize};

use crate::domain::value_object::{AssetId, ContentHash, MediaKind, RenditionKind};

/// An object-storage key — the path of an object in the canonical byte store. It
/// is never a URL and never bytes; the delivery plane turns a key into a CDN /
/// signed URL.
///
/// Two shapes, by lifecycle:
/// * **staging** (`uploads/{asset_id}`) — where the client PUTs the raw bytes
///   before the content hash is known. Keyed by the freshly-minted asset id.
/// * **rendition** (`{kind}/{content_hash}/{slug}.{ext}`) — the **content-
///   addressed** key for a derivative. Because the hash segment is derived from
///   the bytes, the same bytes always map to the same key (cacheable forever) and
///   an edit (different bytes → different hash) lands on a brand-new key, which is
///   why public delivery URLs are immutable and never need invalidation on edit.
///
/// The scheme is a **stateful contract**: once objects exist, its meaning must
/// never change (it is load-bearing for cache correctness and dedup) — version it,
/// don't mutate it.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorageKey(String);

impl StorageKey {
    /// The staging key the pre-signed upload PUTs to (pre-hash).
    pub fn staging(asset_id: AssetId) -> Self {
        Self(format!("uploads/{asset_id}"))
    }

    /// The content-addressed key for a finished rendition.
    pub fn rendition(
        kind: MediaKind,
        hash: &ContentHash,
        rendition: RenditionKind,
        extension: &str,
    ) -> Self {
        Self(format!(
            "{}/{}/{}.{}",
            kind.path_segment(),
            hash.as_str(),
            rendition.slug(),
            extension
        ))
    }

    /// A named object within a video asset's content-addressed HLS output tree
    /// (`{kind}/{content_hash}/{relative}`), e.g. `master.m3u8`, `poster.jpg`, or a
    /// rung segment `1080w/seg_001.m4s`. Same content-addressing guarantee as
    /// [`rendition`](Self::rendition): the whole tree is immutable per content hash.
    pub fn video_object(kind: MediaKind, hash: &ContentHash, relative: &str) -> Self {
        Self(format!("{}/{}/{}", kind.path_segment(), hash.as_str(), relative.trim_start_matches('/')))
    }

    /// Wraps a key read back from storage.
    pub fn from_raw(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Where a quarantined object lives: under `quarantine/`, a prefix the CDN's
    /// origin access is not granted — a known public URL stops resolving.
    pub fn quarantined(&self) -> Self {
        Self(format!("{QUARANTINE_PREFIX}{}", self.0))
    }

    /// The key a quarantined object is restored to (`self` if not quarantined).
    pub fn released(&self) -> Self {
        Self(self.0.strip_prefix(QUARANTINE_PREFIX).unwrap_or(&self.0).to_owned())
    }

    /// The content-addressed tree this object belongs to (`{kind}/{hash}/`):
    /// every rendition of a hash, and a video's whole HLS output.
    pub fn tree_prefix(&self) -> String {
        match self.0.split('/').collect::<Vec<_>>().as_slice() {
            [kind, hash, _, ..] => format!("{kind}/{hash}/"),
            _ => self.0.clone(),
        }
    }
}

/// The prefix quarantined objects are moved under.
pub const QUARANTINE_PREFIX: &str = "quarantine/";

impl fmt::Debug for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageKey({})", self.0)
    }
}

impl fmt::Display for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn hash() -> ContentHash {
        ContentHash::new("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").unwrap()
    }

    #[test]
    fn staging_key_is_keyed_by_asset_id() {
        let id = AssetId::from_uuid(Uuid::from_u128(1));
        assert_eq!(
            StorageKey::staging(id).as_str(),
            format!("uploads/{id}")
        );
    }

    #[test]
    fn rendition_key_is_content_addressed() {
        let k = StorageKey::rendition(MediaKind::PostImage, &hash(), RenditionKind::Thumbnail, "webp");
        assert_eq!(
            k.as_str(),
            "post-images/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/thumbnail.webp"
        );
    }

    #[test]
    fn identical_bytes_yield_identical_keys() {
        let a = StorageKey::rendition(MediaKind::Avatar, &hash(), RenditionKind::Original, "jpg");
        let b = StorageKey::rendition(MediaKind::Avatar, &hash(), RenditionKind::Original, "jpg");
        assert_eq!(a, b, "content-addressing must be deterministic");
    }

    #[test]
    fn video_object_keys_live_under_the_content_addressed_tree() {
        let h = hash();
        assert_eq!(
            StorageKey::video_object(MediaKind::Video, &h, "master.m3u8").as_str(),
            format!("post-videos/{}/master.m3u8", h.as_str())
        );
        // A leading slash in the relative path is normalized (no double slash).
        assert_eq!(
            StorageKey::video_object(MediaKind::Video, &h, "/1080w/seg_001.m4s").as_str(),
            format!("post-videos/{}/1080w/seg_001.m4s", h.as_str())
        );
    }
}
