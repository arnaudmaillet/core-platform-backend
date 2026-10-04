use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lightweight map marker for the Radar (pan/zoom) read path.
///
/// The bare minimum required to render a pin on the map: identity, exact
/// location, and a cover thumbnail. Deliberately carries NO author metadata,
/// caption, virality, or tier — those live on [`MapPostCard`] and are fetched
/// only on focus (pin tap) via the GetGeoTimeline path.
///
/// Stored in Redis (msgpack) under `sg:geo:pin:{post_id}`, written alongside the
/// spatial-index entry at index time. The Radar query reads pins exclusively
/// from Redis — there is no ScyllaDB fallback on the pan path.
///
/// [`MapPostCard`]: crate::domain::entity::MapPostCard
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RadarPin {
    pub post_id:       Uuid,
    pub lat:           f64,
    pub lng:           f64,
    pub thumbnail_url: String,
    /// The post's author, for the per-reader audience filter. Internal: not on
    /// the wire. `None` on pins written before it existed; a client never sees
    /// those (unverifiable author).
    #[serde(default)]
    pub author_id:     Option<Uuid>,
}
