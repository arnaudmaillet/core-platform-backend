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
/// * **restricted** (`restricted/{kind}/{asset_id}/{slug}.{ext}`, #919) — where a
///   SIGNED asset's renditions live once `MEDIA_RESTRICTED_PREFIX_ENABLED` is on:
///   per asset (never shared across owners, unguessable) and under a prefix the
///   CDN denies, so only a signed URL reaches the bytes.
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
        ObjectRoot::content_addressed(kind, hash).rendition(rendition, extension)
    }

    /// The per-asset restricted key for a finished rendition (#919).
    pub fn restricted(kind: MediaKind, asset_id: AssetId, rendition: RenditionKind, extension: &str) -> Self {
        ObjectRoot::restricted(kind, asset_id).rendition(rendition, extension)
    }

    /// A named object within a video asset's content-addressed HLS output tree
    /// (`{kind}/{content_hash}/{relative}`), e.g. `master.m3u8`, `poster.jpg`, or a
    /// rung segment `1080w/seg_001.m4s`. Same content-addressing guarantee as
    /// [`rendition`](Self::rendition): the whole tree is immutable per content hash.
    pub fn video_object(kind: MediaKind, hash: &ContentHash, relative: &str) -> Self {
        ObjectRoot::content_addressed(kind, hash).object(relative)
    }

    /// Where a private document lives once checked (#777): under `private/`, a
    /// prefix the CDN's origin access is not granted (like `quarantine/`), keyed
    /// by the asset — never content-addressed, so identical bytes uploaded by
    /// two accounts never share an object.
    pub fn private_document(asset_id: AssetId) -> Self {
        Self(format!("{PRIVATE_PREFIX}documents/{asset_id}"))
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

    /// The tree this object belongs to: `{kind}/{hash}/` (every rendition of a
    /// hash, and a video's whole HLS output) or, for a restricted object,
    /// `restricted/{kind}/{asset_id}/`.
    pub fn tree_prefix(&self) -> String {
        match self.0.split('/').collect::<Vec<_>>().as_slice() {
            [RESTRICTED_SEGMENT, kind, id, _, ..] => format!("{RESTRICTED_PREFIX}{kind}/{id}/"),
            [kind, hash, _, ..] => format!("{kind}/{hash}/"),
            _ => self.0.clone(),
        }
    }

    /// Whether this object lives under `restricted/` (#919).
    pub fn is_restricted(&self) -> bool {
        self.0.starts_with(RESTRICTED_PREFIX)
    }

    /// This key moved from one root to another (same relative path); `None`
    /// when it is not under `from`.
    pub fn rebased(&self, from: &ObjectRoot, to: &ObjectRoot) -> Option<Self> {
        self.0.strip_prefix(&from.prefix()).map(|relative| to.object(relative))
    }
}

/// Where an asset's renditions live (#919): the content-addressed tree
/// (`{kind}/{hash}`, shared by every asset holding the same bytes) or the
/// asset's own restricted tree (`restricted/{kind}/{asset_id}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRoot(String);

impl ObjectRoot {
    pub fn content_addressed(kind: MediaKind, hash: &ContentHash) -> Self {
        Self(format!("{}/{}", kind.path_segment(), hash.as_str()))
    }

    pub fn restricted(kind: MediaKind, asset_id: AssetId) -> Self {
        Self(format!("{RESTRICTED_PREFIX}{}/{asset_id}", kind.path_segment()))
    }

    /// A rendition's key under this root (`{root}/{slug}.{ext}`).
    pub fn rendition(&self, rendition: RenditionKind, extension: &str) -> StorageKey {
        StorageKey(format!("{}/{}.{}", self.0, rendition.slug(), extension))
    }

    /// A named object under this root, e.g. a video's `1080w/seg_001.m4s`.
    pub fn object(&self, relative: &str) -> StorageKey {
        StorageKey(format!("{}/{}", self.0, relative.trim_start_matches('/')))
    }

    /// The listing prefix of every object under this root (trailing `/`).
    pub fn prefix(&self) -> String {
        format!("{}/", self.0)
    }

    pub fn is_restricted(&self) -> bool {
        self.0.starts_with(RESTRICTED_PREFIX)
    }
}

/// The prefix quarantined objects are moved under.
pub const QUARANTINE_PREFIX: &str = "quarantine/";

/// The prefix of objects never served publicly (#777): the CDN's origin access
/// is not granted it either.
pub const PRIVATE_PREFIX: &str = "private/";

/// The prefix of SIGNED assets' renditions (#919): the CDN denies it, so only a
/// signed URL reaches them.
pub const RESTRICTED_PREFIX: &str = "restricted/";
const RESTRICTED_SEGMENT: &str = "restricted";

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
    fn restricted_keys_are_per_asset_and_keep_their_tree() {
        let id = AssetId::from_uuid(Uuid::from_u128(7));
        let k = StorageKey::restricted(MediaKind::PostImage, id, RenditionKind::Thumbnail, "jpg");
        assert_eq!(k.as_str(), format!("restricted/post-images/{id}/thumbnail.jpg"));
        assert!(k.is_restricted());
        // The tree is the asset's own, never every restricted asset of the kind.
        assert_eq!(k.tree_prefix(), format!("restricted/post-images/{id}/"));
        assert_eq!(k.quarantined().released(), k);
        let other = StorageKey::restricted(MediaKind::PostImage, AssetId::from_uuid(Uuid::from_u128(8)), RenditionKind::Thumbnail, "jpg");
        assert_ne!(k, other, "identical bytes of two assets never share a restricted key");
    }

    #[test]
    fn a_key_rebases_between_roots() {
        let id = AssetId::from_uuid(Uuid::from_u128(7));
        let public = ObjectRoot::content_addressed(MediaKind::Video, &hash());
        let restricted = ObjectRoot::restricted(MediaKind::Video, id);
        let segment = StorageKey::video_object(MediaKind::Video, &hash(), "1080w/seg_001.m4s");
        let moved = segment.rebased(&public, &restricted).unwrap();
        assert_eq!(moved.as_str(), format!("restricted/post-videos/{id}/1080w/seg_001.m4s"));
        assert_eq!(moved.rebased(&restricted, &public), Some(segment.clone()));
        assert_eq!(segment.rebased(&restricted, &public), None, "not under that root");
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
