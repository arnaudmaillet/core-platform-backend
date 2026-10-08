//! What the wallet announces on `wallet.v1.events` (#665): likes staked.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The wallet's events, tagged on `type`, snake_case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalletEvent {
    StakeCommitted(StakeCommitted),
}

/// A batch of likes landed on a post or a comment. Announced at least once
/// (a retried batch announces again): consumers dedup on `stake_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StakeCommitted {
    pub account_id:        String,
    /// The profile that liked.
    pub profile_id:        String,
    /// `post` or `comment`.
    pub target_kind:       String,
    pub target_id:         String,
    /// The target's author.
    pub author_profile_id: String,
    /// Points (likes) this batch added.
    pub points:            i64,
    /// This account's points on the target now.
    pub total:             i64,
    /// The account's first batch on the target ("X liked your post").
    pub first:             bool,
    /// The batch's key, unique per account: the consumers' dedup key.
    pub stake_key:         String,
    pub staked_at:         DateTime<Utc>,
}

impl WalletEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::StakeCommitted(_) => "stake_committed",
        }
    }

    /// The partition key: the target, so its likes stay in order.
    pub fn key(&self) -> String {
        match self {
            Self::StakeCommitted(e) => format!("{}:{}", e.target_kind, e.target_id),
        }
    }
}
