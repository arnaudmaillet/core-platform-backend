use async_trait::async_trait;

use crate::domain::value_object::{LocationSharing, ProfileId};
use crate::error::PostError;

/// The authors' location sharing, projected from `profile.v1.events`
/// (`ProfileLocationSettingsChanged`) and applied to every read of a post's
/// location by anyone but its author. Durable: losing it would show a ghosted
/// author's exact locations.
#[async_trait]
pub trait AuthorLocationStore: Send + Sync + 'static {
    /// The author's sharing; the default (precise) for one the projection has
    /// not seen a change for.
    async fn get(&self, profile_id: &ProfileId) -> Result<LocationSharing, PostError>;

    /// Upsert (idempotent, last-writer-wins).
    async fn set(&self, profile_id: &ProfileId, sharing: LocationSharing) -> Result<(), PostError>;
}
