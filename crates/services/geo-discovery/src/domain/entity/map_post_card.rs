use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::{AuthorId, H3Index, PostId, ViralityScore};

/// Fully hydrated map card projection.
///
/// Stored in Redis (msgpack) and ScyllaDB (durable). The BFF renders a map pin
/// and card preview exclusively from these fields — no fan-out to services/post
/// or services/profile.
///
/// Serde derives are required for both the Redis msgpack encoding and the
/// ScyllaDB cold-start recovery path (where rows are mapped into this struct).
///
/// `author_tier` carries a `#[serde(default)]` annotation so that cards cached
/// before this field was introduced (u8 absent from the msgpack payload) decode
/// as `0` (Standard) rather than failing — enabling zero-downtime rolling deploys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapPostCard {
    pub post_id:           Uuid,
    pub author_id:         Uuid,
    pub author_handle:     String,
    pub author_avatar_url: String,
    pub thumbnail_url:     String,
    /// Post caption, denormalized from `post.published`. Served on the Focus-mode
    /// read path. `#[serde(default)]` so cards cached before this field existed
    /// decode as an empty caption rather than failing (zero-downtime rolling deploy).
    #[serde(default)]
    pub caption:           String,
    /// Canonical tile at resolution 7 for deep-link map centering.
    pub h3_index_r7:       i64,
    pub virality_score:    f32,
    /// Unix epoch milliseconds.
    pub published_at_ms:   i64,
    /// Static author tier badge. 0=Standard, 1=Premium, 2=VIP.
    /// Resolved client-side against the session's social graph for friend/following badges.
    #[serde(default)]
    pub author_tier:       u8,
    /// The post location, kept so a lifted suppression can rebuild the pin.
    /// `None` on cards indexed before it was stored.
    #[serde(default)]
    pub lat:               Option<f64>,
    #[serde(default)]
    pub lng:               Option<f64>,
}

impl MapPostCard {
    pub fn post_id_vo(&self) -> PostId {
        PostId::from(self.post_id)
    }

    pub fn author_id_vo(&self) -> AuthorId {
        AuthorId::from(self.author_id)
    }

    pub fn virality_score_vo(&self) -> ViralityScore {
        ViralityScore::from(self.virality_score)
    }

    pub fn h3_index_r7_vo(&self) -> Option<H3Index> {
        H3Index::from_i64(self.h3_index_r7).ok()
    }
}
