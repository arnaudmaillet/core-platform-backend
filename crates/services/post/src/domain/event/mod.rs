use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostPublishedEvent {
    pub post_id:         String,
    pub profile_id:      String,
    pub kind:            String,
    pub published_at_ms: i64,
    /// The author's tier at publish time (0=Standard, 1=Premium, 2=Vip),
    /// denormalized from `profile.v1.events` and stamped by the publish handler.
    /// `timeline` routes VIP authors to its read path on this field.
    #[serde(default)]
    pub author_tier:     u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_id:        Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_kind:      Option<u8>,

    // ── Denormalized for geo-discovery (Radar pin + Focus card) ──────────────
    /// Post caption. Empty string when the post has none. Feeds the Focus-mode
    /// card in geo-discovery. Additive + `#[serde(default)]`, so other consumers
    /// (`notification`, `timeline`, `search`) ignore it transparently.
    #[serde(default)]
    pub caption:         String,
    /// Cover thumbnail — the first attachment's `thumbnail_url`. Drives the map
    /// pin image. Absent for text-only posts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thumbnail_url:   Option<String>,
    /// The first attachment's media asset (#963): geo-discovery resolves a
    /// video cover's preview loop from it. Absent for text-only posts and for
    /// attachments stored before they named their asset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cover_asset_id:  Option<String>,
    /// Client-supplied post location (WGS-84). Absent → the post is NOT
    /// geo-indexed (geo-discovery skips it). `lat`/`lng` are always emitted
    /// together or not at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lat:             Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lng:             Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostUpdatedEvent {
    pub post_id:    String,
    pub profile_id: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostDeletedEvent {
    pub post_id:    String,
    pub profile_id: String,
    pub deleted_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DomainEvent {
    PostPublished(PostPublishedEvent),
    PostUpdated(PostUpdatedEvent),
    PostDeleted(PostDeletedEvent),
}
