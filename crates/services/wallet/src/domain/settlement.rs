//! Stake settlement (#665, economy charter §33). A position — an account's
//! stakes on one post or comment — settles once, a while after its first
//! stake: what the post or comment came to is observed, the position scored,
//! and only then may gems be minted.
//!
//! **Shadow mode** (charter phase 2): positions are scored and recorded for
//! calibration; no gems are minted. The v0 score is earliness (how much of
//! everyone else's points came after the account's first like) scaled by
//! how much the account committed; the day's outcome (top of the day or not)
//! and the envelope's share come with the daily envelope.

use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::{AccountId, StakeTarget};

/// The scoring model a settlement was computed with.
pub const SHADOW_MODEL: &str = "shadow-v0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementPolicy {
    /// How long after its first stake a position settles.
    pub delay:         TimeDelta,
    /// A position's full commitment (the per-target cap): a position holding
    /// it weighs 1.
    pub full_position: i64,
}

impl Default for SettlementPolicy {
    fn default() -> Self {
        Self { delay: TimeDelta::hours(24), full_position: 250 }
    }
}

/// A position due for settlement, as the wallet holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuePosition {
    pub account:  AccountId,
    pub target:   StakeTarget,
    /// The account's points on the target.
    pub points:   i64,
    pub first_at: DateTime<Utc>,
}

/// What the target came to, as engagement saw it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Observed {
    /// The account's points on it (engagement's copy).
    pub total:            i64,
    /// The target's like count just before the account's first like;
    /// unknown for likes recorded before arrivals were kept.
    pub count_on_arrival: Option<i64>,
    /// The target's like count now.
    pub count_now:        i64,
}

/// A settled position, as recorded.
#[derive(Debug, Clone, PartialEq)]
pub struct Settlement {
    pub account:             AccountId,
    pub target:              StakeTarget,
    pub points:              i64,
    pub first_at:            DateTime<Utc>,
    pub settled_at:          DateTime<Utc>,
    pub count_on_arrival:    Option<i64>,
    pub count_at_settlement: i64,
    pub earliness:           Option<f64>,
    pub pre_score:           Option<f64>,
    pub model:               &'static str,
}

impl SettlementPolicy {
    /// When a position whose first stake was at `first_at` settles.
    pub fn settles_at(&self, first_at: DateTime<Utc>) -> DateTime<Utc> {
        first_at + self.delay
    }

    /// Settles `position` with what engagement observed.
    pub fn settle(&self, position: DuePosition, observed: Observed, now: DateTime<Utc>) -> Settlement {
        let earliness = earliness(observed);
        let pre_score = earliness.map(|e| e * self.weight(position.points));
        Settlement {
            account: position.account,
            target: position.target,
            points: position.points,
            first_at: position.first_at,
            settled_at: now,
            count_on_arrival: observed.count_on_arrival,
            count_at_settlement: observed.count_now,
            earliness,
            pre_score,
            model: SHADOW_MODEL,
        }
    }

    /// How much the account committed, √(points / full): diminishing, so a
    /// large position does not turn its weight into certainty (charter §6).
    fn weight(&self, points: i64) -> f64 {
        let full = self.full_position.max(1) as f64;
        (points.clamp(0, self.full_position.max(1)) as f64 / full).sqrt()
    }
}

/// The share of everyone else's points that came after the account's first
/// like: 1 when it came first, 0 when it came last (or nobody else liked).
/// `None` when its arrival is unknown.
pub fn earliness(observed: Observed) -> Option<f64> {
    let arrival = observed.count_on_arrival?;
    let others = observed.count_now - observed.total;
    if others <= 0 {
        return Some(0.0);
    }
    let others_after = (observed.count_now - arrival - observed.total).clamp(0, others);
    Some(others_after as f64 / others as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed(total: i64, arrival: Option<i64>, now: i64) -> Observed {
        Observed { total, count_on_arrival: arrival, count_now: now }
    }

    #[test]
    fn earliness_is_the_share_of_the_others_that_came_after() {
        assert_eq!(earliness(observed(10, Some(0), 110)), Some(1.0), "first: everyone else came after");
        assert_eq!(earliness(observed(10, Some(100), 110)), Some(0.0), "last");
        assert_eq!(earliness(observed(10, Some(25), 110)), Some(0.75));
        assert_eq!(earliness(observed(10, Some(0), 10)), Some(0.0), "nobody else liked it");
        assert_eq!(earliness(observed(10, None, 110)), None, "an arrival never kept");
        // Engagement lagging behind (a count below what it saw at arrival):
        // never outside [0, 1].
        assert_eq!(earliness(observed(10, Some(200), 110)), Some(0.0));
    }

    #[test]
    fn a_position_settles_a_day_after_its_first_stake_and_scores_with_its_weight() {
        let policy = SettlementPolicy::default();
        let first_at = DateTime::from_timestamp(1_000_000, 0).unwrap();
        assert_eq!(policy.settles_at(first_at), first_at + TimeDelta::hours(24));

        let position = |points| DuePosition {
            account: AccountId::parse("0199c3a0-0000-7000-8000-000000000001").unwrap(),
            target: StakeTarget::Post("p".into()),
            points,
            first_at,
        };
        let now = first_at + TimeDelta::hours(25);
        let full = policy.settle(position(250), observed(250, Some(0), 1_250), now);
        assert_eq!((full.earliness, full.pre_score, full.model), (Some(1.0), Some(1.0), SHADOW_MODEL));
        let quarter = policy.settle(position(62), observed(62, Some(0), 1_062), now);
        assert!((quarter.pre_score.unwrap() - (62.0_f64 / 250.0).sqrt()).abs() < 1e-12);
        let unknown = policy.settle(position(5), observed(5, None, 50), now);
        assert_eq!((unknown.earliness, unknown.pre_score), (None, None));
        assert_eq!((unknown.count_at_settlement, unknown.settled_at), (50, now));
    }
}
