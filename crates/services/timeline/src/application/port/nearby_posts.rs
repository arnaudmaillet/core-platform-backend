use async_trait::async_trait;

use crate::domain::value_object::PostId;
use crate::error::TimelineError;

/// geo-discovery's map index: the posts geotagged around a point. Unfiltered
/// (a mesh read): the caller applies moderation state and the reader's audience.
#[async_trait]
pub trait NearbyPosts: Send + Sync + 'static {
    /// Errors are `NearbyUnavailable`.
    async fn around(&self, lat: f64, lng: f64) -> Result<Vec<PostId>, TimelineError>;
}
