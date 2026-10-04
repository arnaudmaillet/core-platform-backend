use async_trait::async_trait;

use crate::domain::value_object::PostId;
use crate::error::TimelineError;

/// geo-discovery's map index: the posts geotagged around a point. A mesh read,
/// so unfiltered by audience (the caller applies moderation state and the
/// reader's audience) — except a guest's country limit, which geo applies for
/// the `guest` principal it is given.
#[async_trait]
pub trait NearbyPosts: Send + Sync + 'static {
    /// Errors are `NearbyUnavailable`.
    async fn around(&self, lat: f64, lng: f64, guest: Option<&str>) -> Result<Vec<PostId>, TimelineError>;
}
