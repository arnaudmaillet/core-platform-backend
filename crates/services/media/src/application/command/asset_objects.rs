//! An asset's stored objects, for takedowns. Renditions are content-addressed
//! trees (`{kind}/{hash}/…`): an image's renditions, or a video's manifest,
//! poster and every HLS segment. A quarantine moves the whole tree under
//! `quarantine/` — a prefix the CDN's origin access is not granted — so a public
//! URL handed out before stops resolving at the origin, not just at the edge.

use std::collections::BTreeSet;

use crate::application::port::{AssetRepository, ObjectStore};
use crate::domain::aggregate::Asset;
use crate::domain::value_object::storage_key::QUARANTINE_PREFIX;
use crate::domain::value_object::StorageKey;
use crate::error::MediaError;

/// Whether the asset's objects are its own to move or delete. Identical bytes
/// share content-addressed keys, so when another asset with the same bytes is
/// still delivered, the shared objects stay (moving them would break that
/// asset); a legal hold (CSAM evidence) always takes them.
pub(crate) async fn owns_objects(assets: &dyn AssetRepository, asset: &Asset) -> Result<bool, MediaError> {
    if asset.legal_hold() {
        return Ok(true);
    }
    let Some(hash) = asset.content_hash() else { return Ok(true) };
    match assets.find_ready_by_content_hash(hash).await? {
        Some(other) if other.id() != asset.id() => {
            tracing::warn!(
                asset.id = %asset.id(),
                other.id = %other.id(),
                "objects shared with another delivered asset (same bytes) left in place"
            );
            Ok(false)
        }
        _ => Ok(true),
    }
}

/// The asset's content-addressed trees.
fn trees(asset: &Asset) -> BTreeSet<String> {
    asset.renditions().iter().map(|r| r.storage_key().tree_prefix()).collect()
}

/// Moves every object of the asset's trees under `quarantine/`; returns their
/// public keys (what the CDN must purge). Idempotent: on a redelivery the
/// objects already moved are simply not listed any more.
pub(crate) async fn quarantine(store: &dyn ObjectStore, asset: &Asset) -> Result<Vec<StorageKey>, MediaError> {
    let mut public: BTreeSet<String> =
        asset.renditions().iter().map(|r| r.storage_key().as_str().to_owned()).collect();
    // The original upload too (never handed out, but kept at the origin).
    let staging = StorageKey::staging(asset.id());
    store.relocate(&staging, &staging.quarantined()).await?;
    for tree in trees(asset) {
        for key in store.list(&tree).await? {
            store.relocate(&key, &key.quarantined()).await?;
            public.insert(key.as_str().to_owned());
        }
        // Already moved by an earlier delivery: still purge their public paths.
        for key in store.list(&format!("{QUARANTINE_PREFIX}{tree}")).await? {
            public.insert(key.released().as_str().to_owned());
        }
    }
    Ok(public.into_iter().map(StorageKey::from_raw).collect())
}

/// Moves the asset's quarantined objects back to their public keys.
pub(crate) async fn release(store: &dyn ObjectStore, asset: &Asset) -> Result<(), MediaError> {
    let staging = StorageKey::staging(asset.id());
    store.relocate(&staging.quarantined(), &staging).await?;
    for tree in trees(asset) {
        for key in store.list(&format!("{QUARANTINE_PREFIX}{tree}")).await? {
            store.relocate(&key, &key.released()).await?;
        }
    }
    Ok(())
}

/// Every object the asset has, public or quarantined (for a delete), and the
/// public keys among them (for the CDN purge).
pub(crate) async fn all(
    store: &dyn ObjectStore,
    asset: &Asset,
) -> Result<(Vec<StorageKey>, Vec<StorageKey>), MediaError> {
    let mut objects: BTreeSet<String> =
        asset.renditions().iter().map(|r| r.storage_key().as_str().to_owned()).collect();
    objects.insert(StorageKey::staging(asset.id()).quarantined().as_str().to_owned());
    for tree in trees(asset) {
        objects.extend(store.list(&tree).await?.into_iter().map(|k| k.as_str().to_owned()));
        objects.extend(
            store.list(&format!("{QUARANTINE_PREFIX}{tree}")).await?.into_iter().map(|k| k.as_str().to_owned()),
        );
    }
    let staging = StorageKey::staging(asset.id());
    let public: BTreeSet<String> = objects
        .iter()
        .map(|k| StorageKey::from_raw(k.clone()).released())
        .filter(|k| *k != staging)
        .map(|k| k.as_str().to_owned())
        .collect();
    Ok((
        objects.into_iter().map(StorageKey::from_raw).collect(),
        public.into_iter().map(StorageKey::from_raw).collect(),
    ))
}
