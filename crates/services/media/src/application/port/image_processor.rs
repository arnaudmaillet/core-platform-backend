//! The transformation engine (Plane B). For images-first v1 this derives the
//! resize ladder + a BlurHash from the validated master; the concrete adapter
//! (in-process image library, Phase 4) reads the source object and writes each
//! content-addressed derivative to the store, returning their metadata. Video gets
//! a sibling `Transcoder` port in the fast-follow phase — this trait does not grow
//! a video arm.

use async_trait::async_trait;

use crate::domain::aggregate::Rendition;
use crate::domain::value_object::{Blurhash, MediaKind, ObjectRoot, StorageKey};
use crate::error::MediaError;

/// The derivatives produced for an asset: the rendition catalog plus the BlurHash
/// placeholder.
#[derive(Debug, Clone)]
pub struct DerivedRenditions {
    pub renditions: Vec<Rendition>,
    pub blurhash: Blurhash,
}

#[async_trait]
pub trait ImageProcessor: Send + Sync + 'static {
    /// Derives the rendition ladder for `kind` from the validated master at
    /// `source`, writing each derivative to the store under `root` (the
    /// content-addressed tree of its bytes, or a SIGNED asset's restricted
    /// tree, #919).
    async fn derive(
        &self,
        source: &StorageKey,
        kind: MediaKind,
        root: &ObjectRoot,
    ) -> Result<DerivedRenditions, MediaError>;
}
