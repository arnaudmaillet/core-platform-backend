use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::GeoDiscoveryError;

/// The videos' preview loops (#963), from media: what a Shop cover plays.
#[async_trait]
pub trait MediaPreviews: Send + Sync + 'static {
    /// Each asset's preview loop URL, for the ones media has one for and
    /// delivers publicly (a SIGNED video's needs cookies a cover cannot
    /// carry); the others are absent.
    async fn previews(&self, asset_ids: &[Uuid]) -> Result<HashMap<Uuid, String>, GeoDiscoveryError>;
}
