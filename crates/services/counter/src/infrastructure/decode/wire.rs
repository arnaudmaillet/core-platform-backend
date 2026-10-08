//! Counter-owned deserialization DTOs for the events it consumes.
//!
//! Counter must not depend on the `engagement` / `social-graph` crates (a sideways
//! services→services edge the tiering forbids), so it owns its read schema:
//! minimal, lenient structs that match the published JSON. Extra fields are
//! ignored, so an additive change upstream never breaks a consumer.
//!
//! Integration reality (mirrors `search`'s honesty about thin events):
//! * `view` / `impression` / `click` are **counter-owned firehose schemas** — no
//!   upstream producer exists yet; the edge/BFF will produce telemetry matching
//!   these shapes. They are notifications, and counts need nothing more than the
//!   `(entity, actor?, time)` they carry — no hydration.
//! * `wallet.v1.events` **matches the live upstream schema** (the wallet
//!   publishes it, internally tagged on `type`, snake_case): likes are points
//!   (#665), each `stake_committed` adds its points to the target's likes.
//! * `social-graph` follow events are a **counter-owned schema** pending an
//!   upstream follow stream (an upstream prerequisite, like `profile.v1.events`
//!   is for search).

use chrono::{DateTime, Utc};
use serde::Deserialize;

// ── view / impression / click — counter-owned firehose schema ─────────────────

/// One engagement hit on an entity. `actor_id`, when present, is folded into the
/// unique-cardinality estimator (unique viewers / reach); it is never stored.
#[derive(Debug, Clone, Deserialize)]
pub struct HitWire {
    pub entity_type: String,
    pub entity_id: String,
    #[serde(default)]
    pub actor_id: Option<String>,
    pub occurred_at_ms: i64,
}

// ── wallet.v1.events — MATCHES the upstream wallet schema (#665) ─────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalletWire {
    StakeCommitted(StakeCommittedWire),
    /// Any other wallet event: nothing to count.
    #[serde(other)]
    Other,
}

/// A batch of likes: `points` more on the post or comment.
#[derive(Debug, Clone, Deserialize)]
pub struct StakeCommittedWire {
    /// `post` or `comment`.
    pub target_kind: String,
    pub target_id: String,
    pub points: i64,
    pub staked_at: DateTime<Utc>,
}

// ── social-graph follow — counter-owned schema (upstream stream is a prereq) ──

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FollowWire {
    Followed(FollowChangeWire),
    Unfollowed(FollowChangeWire),
}

#[derive(Debug, Clone, Deserialize)]
pub struct FollowChangeWire {
    pub follower_id: String,
    pub followee_id: String,
    pub occurred_at_ms: i64,
}
